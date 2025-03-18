"""
增强版自动删除管理器，负责管理并执行消息的定时删除
"""
import logging
import asyncio
import time
import traceback
from typing import Dict, Any, Optional, List, Set, Tuple, Union, Callable, Coroutine
from datetime import datetime, timedelta
from enum import Enum

from telegram import Bot, Message, Update, InlineKeyboardMarkup
from telegram.error import BadRequest, Forbidden, TelegramError, TimedOut, RetryAfter
from telegram.ext import CallbackContext

from db.database import Database
from db.models import GroupPermission
from utils.message_utils import validate_delete_timeout, is_auto_delete_exempt

logger = logging.getLogger(__name__)

class MessageType(Enum):
    """消息类型枚举"""
    DEFAULT = 'default'
    COMMAND = 'command'
    KEYWORD = 'keyword'
    BROADCAST = 'broadcast'
    RANKING = 'ranking'
    ERROR = 'error'
    WARNING = 'warning'
    HELP = 'help'
    FEEDBACK = 'feedback'
    INTERACTION = 'interaction'

# 默认超时时间（秒）
DEFAULT_TIMEOUTS = {
    MessageType.DEFAULT.value: 300,       # 默认5分钟
    MessageType.COMMAND.value: 300,       # 命令响应5分钟
    MessageType.KEYWORD.value: 300,       # 关键词回复5分钟
    MessageType.BROADCAST.value: 300,     # 轮播消息5分钟
    MessageType.RANKING.value: 300,       # 排行榜5分钟
    MessageType.ERROR.value: 30,          # 错误消息30秒
    MessageType.WARNING.value: 30,        # 警告提示30秒
    MessageType.HELP.value: 300,          # 帮助信息5分钟
    MessageType.FEEDBACK.value: 30,       # 用户操作反馈30秒
    MessageType.INTERACTION.value: 180    # 交互消息3分钟
}

class AutoDeleteManager:
    """自动删除管理器，负责管理并执行消息的定时删除"""
    
    def __init__(self, db: Database, apply_defaults: bool = False):
        """
        初始化自动删除管理器
        
        参数:
            db: 数据库实例
            apply_defaults: 是否应用默认设置
        """
        self.db = db
        self.delete_tasks = {}  # 存储直接删除任务 {message_id: task}（兼容原有代码）
        self.message_queue = asyncio.Queue()  # 消息删除队列
        self.failed_messages = {}  # 存储删除失败的消息
        self.last_cleanup_time = datetime.now()
        self.running = True
        self.worker_task = None
        self.cleanup_task = None
        self.recovery_task = None
        self.shutting_down = False
        self.bot = None
        
        # 各类消息的默认超时设置（秒）
        self.default_timeouts = DEFAULT_TIMEOUTS.copy()
        
        # 只在首次初始化时应用默认设置
        if apply_defaults:
            asyncio.create_task(self._apply_default_settings())
        
        # 初始化任务
        self._init_tasks()
        
        logger.info("自动删除管理器初始化完成")
    
    async def _apply_default_settings(self):
        """应用默认自动删除设置"""
        try:
            from config import AUTO_DELETE_SETTINGS
            logger.info("应用默认自动删除设置...")
            
            # 获取所有群组
            groups = await self.db.find_all_groups()
            
            for group in groups:
                group_id = group.get('group_id')
                settings = await self.db.get_group_settings(group_id)
                
                # 只在设置不存在时应用默认值
                if 'auto_delete' not in settings:
                    settings['auto_delete'] = AUTO_DELETE_SETTINGS.get('default_enabled', False)
                
                if 'auto_delete_timeout' not in settings:
                    settings['auto_delete_timeout'] = AUTO_DELETE_SETTINGS.get('default_timeout', 300)
                
                # 确保 auto_delete_timeouts 存在
                if 'auto_delete_timeouts' not in settings:
                    settings['auto_delete_timeouts'] = {
                        'default': settings.get('auto_delete_timeout', 300),
                        'keyword': settings.get('auto_delete_timeout', 300),
                        'broadcast': settings.get('auto_delete_timeout', 300),
                        'ranking': settings.get('auto_delete_timeout', 300),
                        'command': settings.get('auto_delete_timeout', 300),
                        # 增加新消息类型的默认超时设置
                        'error': 30,
                        'warning': 30,
                        'help': 300,
                        'feedback': 30,
                        'interaction': 180
                    }
                
                await self.db.update_group_settings(group_id, settings)
                logger.info(f"已更新群组 {group_id} 的自动删除设置")
        except Exception as e:
            logger.error(f"应用默认自动删除设置失败: {e}", exc_info=True)

    def _init_tasks(self):
        """初始化后台任务"""
        # 消息删除工作线程
        self.worker_task = asyncio.create_task(self._message_worker())
        # 失败消息清理线程
        self.cleanup_task = asyncio.create_task(self._cleanup_failed_messages())
        # 恢复线程
        self.recovery_task = asyncio.create_task(self._recovery_check())
        
        logger.info("自动删除管理器任务已初始化")
    
    async def schedule_delete(self, message: Message, message_type: str = 'default', 
                              chat_id: Optional[int] = None, timeout: Optional[int] = None):
        """
        安排消息删除
        
        参数:
            message: 消息对象
            message_type: 消息类型（default, command, keyword, broadcast, ranking, error, warning, help, feedback, interaction）
            chat_id: 聊天ID，如果不提供则从消息获取
            timeout: 超时时间（秒），如果不提供则根据消息类型和群组设置获取
        """
        if not message:
            logger.warning("尝试安排删除空消息")
            return False
            
        # 获取聊天ID
        if not chat_id and message.chat:
            chat_id = message.chat.id
        
        if not chat_id:
            logger.warning("无法确定聊天ID，取消删除任务")
            return False
            
        # 检查自动删除是否启用
        if not await self._is_auto_delete_enabled(chat_id):
            logger.debug(f"群组 {chat_id} 的自动删除未启用，跳过消息: {message.message_id}")
            return False
            
        # 检查用户是否豁免自动删除（如管理员）
        if message.from_user:
            user = await self.db.get_user(message.from_user.id)
            if user and is_auto_delete_exempt(user.get('role', ''), message.text):
                logger.debug(f"用户 {message.from_user.id} 免除自动删除")
                return False
            
        # 如果未指定超时时间，从数据库获取
        if timeout is None:
            timeout = await self._get_timeout_for_type(chat_id, message_type)
        
        # 将删除任务添加到队列
        delete_time = datetime.now() + timedelta(seconds=timeout)
        await self.message_queue.put((message, delete_time, chat_id))
        
        # 兼容旧版本：为直接API保存任务引用
        message_id = f"{chat_id}_{message.message_id}"
        self.delete_tasks[message_id] = asyncio.create_task(self._delete_after(message, timeout))
        
        logger.debug(f"已安排消息 {message.message_id} 在 {delete_time} 删除 (类型: {message_type})")
        return True
        
    async def _delete_after(self, message: Message, timeout: int):
        """
        延迟删除消息 (兼容老版本的直接API)
        
        参数:
            message: 要删除的消息
            timeout: 延迟时间（秒）
        """
        if not message or not message.chat:
            return
            
        message_id = f"{message.chat.id}_{message.message_id}"
        try:
            await asyncio.sleep(timeout)
            # 检查任务是否仍然在列表中（可能在等待期间被取消）
            if message_id in self.delete_tasks:
                await message.delete()
                logger.info(f"已删除消息 {message.message_id}")
        except asyncio.CancelledError:
            logger.info(f"删除任务已取消: {message.message_id}")
        except Exception as e:
            logger.error(f"删除消息 {message.message_id} 失败: {e}")
        finally:
            # 清理任务引用
            if message_id in self.delete_tasks:
                del self.delete_tasks[message_id]
    
    async def _message_worker(self):
        """消息删除工作线程"""
        while self.running:
            try:
                # 获取下一个要删除的消息
                message, delete_time, chat_id = await self.message_queue.get()
                
                # 计算等待时间
                now = datetime.now()
                wait_time = (delete_time - now).total_seconds()
                
                if wait_time > 0:
                    # 如果还没到删除时间，等待
                    await asyncio.sleep(wait_time)
                
                # 删除消息
                try:
                    await message.delete()
                    logger.debug(f"已删除消息: chat_id={chat_id}, message_id={message.message_id}")
                except BadRequest as e:
                    # 处理消息已删除的情况
                    if "message to delete not found" in str(e):
                        logger.debug(f"消息已被删除: chat_id={chat_id}, message_id={message.message_id}")
                    else:
                        logger.warning(f"删除消息时出现BadRequest: {e}, chat_id={chat_id}, message_id={message.message_id}")
                        # 记录失败的消息
                        self._add_failed_message(chat_id, message.message_id, str(e))
                except Forbidden as e:
                    logger.warning(f"没有权限删除消息: {e}, chat_id={chat_id}, message_id={message.message_id}")
                    # 记录权限问题
                    self._add_failed_message(chat_id, message.message_id, f"权限错误: {e}")
                except Exception as e:
                    logger.error(f"删除消息时出错: {e}, chat_id={chat_id}, message_id={message.message_id}")
                    # 记录其他错误
                    self._add_failed_message(chat_id, message.message_id, f"未知错误: {e}")
                finally:
                    # 标记任务完成
                    self.message_queue.task_done()
            except asyncio.CancelledError:
                logger.info("消息工作线程被取消")
                break
            except Exception as e:
                logger.error(f"消息工作线程出错: {e}", exc_info=True)
                # 避免死循环
                await asyncio.sleep(5)
    
    def _add_failed_message(self, chat_id: int, message_id: int, error: str):
        """添加删除失败的消息到记录"""
        key = f"{chat_id}:{message_id}"
        self.failed_messages[key] = {
            'chat_id': chat_id,
            'message_id': message_id,
            'error': error,
            'time': datetime.now(),
            'retry_count': self.failed_messages.get(key, {}).get('retry_count', 0) + 1
        }

    async def _cleanup_failed_messages(self):
        """定期清理失败的消息记录"""
        while self.running:
            try:
                now = datetime.now()
                # 每小时执行一次清理
                await asyncio.sleep(3600)
                
                # 清理超过24小时的失败记录
                keys_to_remove = []
                for key, data in self.failed_messages.items():
                    if (now - data['time']).total_seconds() > 86400:  # 24小时
                        keys_to_remove.append(key)
                
                # 删除旧记录
                for key in keys_to_remove:
                    del self.failed_messages[key]
                
                logger.info(f"已清理 {len(keys_to_remove)} 条过期的失败消息记录")
                
                # 尝试重试一些失败的删除
                await self._retry_failed_deletions()
            except asyncio.CancelledError:
                logger.info("清理任务被取消")
                break
            except Exception as e:
                logger.error(f"清理任务出错: {e}", exc_info=True)
                await asyncio.sleep(300)  # 出错后5分钟再试
    
    async def _retry_failed_deletions(self):
        """尝试重新删除失败的消息"""
        if not self.bot:
            logger.warning("没有可用的Bot实例，无法重试失败的删除")
            return
            
        retry_candidates = []
        
        # 找出可以重试的消息（重试次数少于3次且最后一次尝试超过10分钟）
        now = datetime.now()
        for key, data in self.failed_messages.items():
            if (data['retry_count'] < 3 and 
                (now - data['time']).total_seconds() > 600):  # 10分钟
                retry_candidates.append(data)
        
        if not retry_candidates:
            return
            
        logger.info(f"尝试重新删除 {len(retry_candidates)} 条失败的消息")
        
        for data in retry_candidates:
            try:
                chat_id = data['chat_id']
                message_id = data['message_id']
                
                # 检查群组是否仍启用自动删除
                if not await self._is_auto_delete_enabled(chat_id):
                    logger.debug(f"群组 {chat_id} 已禁用自动删除，移除失败记录")
                    del self.failed_messages[f"{chat_id}:{message_id}"]
                    continue
                
                # 尝试删除消息
                await self.bot.delete_message(chat_id=chat_id, message_id=message_id)
                logger.info(f"成功重新删除消息: chat_id={chat_id}, message_id={message_id}")
                
                # 从失败记录中移除
                del self.failed_messages[f"{chat_id}:{message_id}"]
            except BadRequest as e:
                # 如果消息已不存在，从失败记录中移除
                if "message to delete not found" in str(e):
                    del self.failed_messages[f"{chat_id}:{message_id}"]
                else:
                    # 更新失败记录
                    data['error'] = str(e)
                    data['time'] = now
                    self.failed_messages[f"{chat_id}:{message_id}"] = data
            except Exception as e:
                logger.error(f"重试删除消息时出错: {e}, chat_id={chat_id}, message_id={message_id}")
                # 更新失败记录
                data['error'] = str(e)
                data['time'] = now
                self.failed_messages[f"{chat_id}:{message_id}"] = data
    
    async def _recovery_check(self):
        """恢复检查，处理系统休眠后应该删除的消息"""
        while self.running:
            try:
                await asyncio.sleep(300)  # 每5分钟检查一次
                
                # 检测是否存在系统休眠
                last_active = getattr(self, 'last_active_time', datetime.now())
                now = datetime.now()
                time_diff = (now - last_active).total_seconds()
                
                # 如果时间差超过10分钟，认为系统可能休眠过
                if time_diff > 600:  # 10分钟
                    logger.warning(f"检测到可能的系统休眠，时间差: {time_diff:.2f}秒")
                    await self._handle_post_sleep_recovery()
                
                # 更新最后活动时间
                self.last_active_time = now
            except asyncio.CancelledError:
                logger.info("恢复检查任务被取消")
                break
            except Exception as e:
                logger.error(f"恢复检查任务出错: {e}", exc_info=True)
                await asyncio.sleep(300)  # 出错后5分钟再试
    
    async def _handle_post_sleep_recovery(self):
        """处理系统休眠后的恢复"""
        logger.info("开始系统休眠后的恢复处理")
        
        # 检查是否有消息删除积压
        queue_size = self.message_queue.qsize()
        if queue_size > 0:
            logger.info(f"发现积压的删除任务: {queue_size}个")
            
            # 紧急处理队列中的任务
            emergency_worker = asyncio.create_task(self._emergency_message_worker())
            
            # 等待紧急工作线程完成
            try:
                await asyncio.wait_for(emergency_worker, timeout=300)  # 最多等待5分钟
            except asyncio.TimeoutError:
                logger.warning("紧急工作线程超时，继续正常操作")
            except Exception as e:
                logger.error(f"紧急工作线程出错: {e}", exc_info=True)
            
            logger.info("紧急处理完成")
    
    async def _emergency_message_worker(self):
        """紧急情况下处理积压的删除任务"""
        processed = 0
        start_time = time.time()
        
        while not self.message_queue.empty():
            try:
                # 获取下一个要删除的消息
                message, delete_time, chat_id = self.message_queue.get_nowait()
                
                # 检查是否应该删除
                now = datetime.now()
                if now >= delete_time:
                    # 立即删除过期消息
                    try:
                        await message.delete()
                        logger.debug(f"紧急模式: 已删除过期消息: chat_id={chat_id}, message_id={message.message_id}")
                        processed += 1
                    except Exception as e:
                        logger.warning(f"紧急模式: 删除消息出错: {e}, chat_id={chat_id}, message_id={message.message_id}")
                        # 记录失败的消息
                        self._add_failed_message(chat_id, message.message_id, str(e))
                else:
                    # 对于未过期的消息，重新放回队列
                    await self.message_queue.put((message, delete_time, chat_id))
                
                # 标记任务完成
                self.message_queue.task_done()
                
                # 限制处理速度，避免API限制
                if processed % 20 == 0:
                    await asyncio.sleep(1)
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                logger.error(f"紧急工作线程处理出错: {e}", exc_info=True)
                await asyncio.sleep(1)
        
        total_time = time.time() - start_time
        logger.info(f"紧急模式: 处理了 {processed} 条消息，耗时 {total_time:.2f} 秒")
    
    async def _is_auto_delete_enabled(self, chat_id: int) -> bool:
        """检查群组是否启用了自动删除"""
        try:
            settings = await self.db.get_group_settings(chat_id)
            return settings.get('auto_delete', False)
        except Exception as e:
            logger.error(f"检查自动删除状态出错: {e}", exc_info=True)
            return False
    
    async def _get_timeout_for_type(self, chat_id: int, message_type: str) -> int:
        """获取特定消息类型的超时时间"""
        try:
            settings = await self.db.get_group_settings(chat_id)
            # 获取特定类型的超时设置
            timeouts = settings.get('auto_delete_timeouts', {})
            
            # 如果指定类型的超时设置存在，则使用它
            if message_type in timeouts:
                return timeouts[message_type]
            
            # 否则使用默认超时设置
            return settings.get('auto_delete_timeout', self.default_timeouts.get(message_type, 300))
        except Exception as e:
            logger.error(f"获取超时设置出错: {e}", exc_info=True)
            # 出错时使用默认值
            return self.default_timeouts.get(message_type, 300)
    
    # 以下为兼容原有API的方法
    
    async def cancel_delete(self, message: Message):
        """
        取消删除任务
        
        参数:
            message: 消息对象
        """
        if not message or not message.chat:
            return
            
        message_id = f"{message.chat.id}_{message.message_id}"
        if message_id in self.delete_tasks:
            self.delete_tasks[message_id].cancel()
            del self.delete_tasks[message_id]
            logger.info(f"已取消消息 {message.message_id} 的删除任务")
    
    # 以下为特定消息类型的处理方法，保留原有接口
    
    async def handle_command_response(self, message: Message, group_id: int):
        """
        处理命令响应消息的自动删除
        
        参数:
            message: 命令响应消息
            group_id: 群组ID
        """
        await self.schedule_delete(message, MessageType.COMMAND.value, group_id)
        
    async def handle_keyword_response(self, message: Message, group_id: int):
        """
        处理关键词响应消息的自动删除
        
        参数:
            message: 关键词响应消息
            group_id: 群组ID
        """
        await self.schedule_delete(message, MessageType.KEYWORD.value, group_id)
        
    async def handle_broadcast_message(self, message: Message, group_id: int):
        """
        处理轮播消息的自动删除
        
        参数:
            message: 轮播消息
            group_id: 群组ID
        """
        await self.schedule_delete(message, MessageType.BROADCAST.value, group_id)
        
    async def handle_ranking_message(self, message: Message, group_id: int):
        """
        处理排行榜消息的自动删除
        
        参数:
            message: 排行榜消息
            group_id: 群组ID
        """
        await self.schedule_delete(message, MessageType.RANKING.value, group_id)
        
    async def handle_user_command(self, message: Message):
        """
        处理用户命令的自动删除
        
        参数:
            message: 用户命令消息
        """
        if not message or not message.chat:
            return
            
        group_id = message.chat.id
        if message.chat.type != 'private':  # 只在群组中自动删除
            await self.schedule_delete(message, MessageType.DEFAULT.value, group_id)
    
    # 新增: 设置机器人实例
    def set_bot(self, bot: Bot):
        """设置机器人实例，用于重试删除"""
        self.bot = bot
        logger.info("已设置机器人实例")
    
    # 增强版shutdown方法
    async def shutdown(self):
        """关闭自动删除管理器"""
        logger.info("开始关闭自动删除管理器...")
        self.shutting_down = True
        self.running = False
        
        # 取消所有删除任务
        for message_id, task in list(self.delete_tasks.items()):
            try:
                logger.info(f"取消删除任务: {message_id}")
                task.cancel()
            except Exception as e:
                logger.error(f"取消删除任务 {message_id} 时出错: {e}")
        
        # 清理任务字典
        self.delete_tasks.clear()
        
        # 取消所有后台任务
        for task in [self.worker_task, self.cleanup_task, self.recovery_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        logger.info("自动删除管理器已关闭")


# 辅助函数 - 消息发送工具

async def send_auto_delete_message(
    bot: Bot, 
    chat_id: int, 
    text: str, 
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    message_type: str = MessageType.DEFAULT.value,
    reply_to_message_id: Optional[int] = None,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = False,
    timeout: Optional[int] = None
) -> Optional[Message]:
    """
    发送将被自动删除的消息
    
    参数:
        bot: 机器人实例
        chat_id: 聊天ID
        text: 消息文本
        reply_markup: 内联键盘标记
        message_type: 消息类型，用于确定默认超时时间
        reply_to_message_id: 要回复的消息ID
        parse_mode: 解析模式
        disable_web_page_preview: 是否禁用网页预览
        timeout: 可选的超时时间覆盖（秒）
        
    返回:
        发送的消息对象
    """
    # 验证message_type是否有效
    if message_type not in DEFAULT_TIMEOUTS:
        logger.warning(f"无效的消息类型: {message_type}，使用默认类型")
        message_type = MessageType.DEFAULT.value
    
    try:
        # 发送消息
        message = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview
        )
        
        # 获取机器人实例并调度自动删除
        from telegram.ext import ApplicationBuilder
        try:
            if hasattr(bot, 'application') and bot.application:
                app = bot.application
                if 'bot_instance' in app.bot_data:
                    bot_instance = app.bot_data['bot_instance']
                    if hasattr(bot_instance, 'auto_delete_manager'):
                        # 使用自动删除管理器
                        if timeout is None:
                            await bot_instance.auto_delete_manager.schedule_delete(message, message_type, chat_id)
                        else:
                            await bot_instance.auto_delete_manager.schedule_delete(message, message_type, chat_id, timeout)
                        return message
        except Exception as e:
            logger.error(f"调度自动删除失败: {e}", exc_info=True)
        
        # 如果找不到自动删除管理器或出错，使用基本方法
        if timeout is None:
            timeout = DEFAULT_TIMEOUTS.get(message_type, 300)
        
        # 使用基本的延迟删除方法
        asyncio.create_task(delayed_delete(message, timeout))
        
        return message
    except Exception as e:
        logger.error(f"发送自动删除消息失败: {e}", exc_info=True)
        return None

async def send_error_message(bot: Bot, chat_id: int, text: str) -> Optional[Message]:
    """发送错误消息（30秒后自动删除）"""
    return await send_auto_delete_message(bot, chat_id, f"❌ {text}", message_type=MessageType.ERROR.value)

async def send_warning_message(bot: Bot, chat_id: int, text: str) -> Optional[Message]:
    """发送警告消息（30秒后自动删除）"""
    return await send_auto_delete_message(bot, chat_id, f"⚠️ {text}", message_type=MessageType.WARNING.value)

async def send_success_message(bot: Bot, chat_id: int, text: str) -> Optional[Message]:
    """发送成功消息（30秒后自动删除）"""
    return await send_auto_delete_message(bot, chat_id, f"✅ {text}", message_type=MessageType.FEEDBACK.value)

async def send_help_message(bot: Bot, chat_id: int, text: str, 
                            reply_markup: Optional[InlineKeyboardMarkup] = None) -> Optional[Message]:
    """发送帮助消息（5分钟后自动删除）"""
    return await send_auto_delete_message(bot, chat_id, text, reply_markup, 
                                         message_type=MessageType.HELP.value)

async def send_interaction_message(bot: Bot, chat_id: int, text: str, 
                                  reply_markup: Optional[InlineKeyboardMarkup] = None) -> Optional[Message]:
    """发送交互消息（3分钟后自动删除）"""
    return await send_auto_delete_message(bot, chat_id, text, reply_markup, 
                                         message_type=MessageType.INTERACTION.value)

async def delayed_delete(message: Message, timeout: int):
    """
    延迟删除消息的基本实现
    
    参数:
        message: 要删除的消息
        timeout: 超时时间（秒）
    """
    try:
        await asyncio.sleep(timeout)
        await message.delete()
    except BadRequest as e:
        # 忽略"消息找不到"的错误
        if "message to delete not found" not in str(e):
            logger.warning(f"删除消息失败: {e}")
    except Exception as e:
        logger.error(f"删除消息出错: {e}", exc_info=True)

async def update_interaction_message(message: Message, text: str, 
                                   reply_markup: Optional[InlineKeyboardMarkup] = None,
                                   parse_mode: Optional[str] = None) -> bool:
    """
    更新交互消息并重置删除计时器
    
    参数:
        message: 要更新的消息
        text: 新消息文本
        reply_markup: 新的内联键盘标记
        parse_mode: 解析模式
        
    返回:
        是否成功更新
    """
    try:
        # 更新消息
        await message.edit_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        
        # 重置删除计时器
        try:
            bot = message.get_bot()
            from telegram.ext import ApplicationBuilder
            if hasattr(bot, 'application') and bot.application:
                app = bot.application
                if 'bot_instance' in app.bot_data:
                    bot_instance = app.bot_data['bot_instance']
                    if hasattr(bot_instance, 'auto_delete_manager'):
                        # 取消现有的删除计划并重新调度
                        chat_id = message.chat_id
                        await bot_instance.auto_delete_manager.schedule_delete(
                            message, 
                            MessageType.INTERACTION.value, 
                            chat_id
                        )
                        return True
        except Exception as e:
            logger.error(f"重置删除计时器失败: {e}", exc_info=True)
        
        return True
    except Exception as e:
        logger.error(f"更新交互消息失败: {e}", exc_info=True)
        return False

async def cancel_interaction(message: Message, text: str = "❌ 操作已取消") -> bool:
    """
    取消交互并快速删除消息
    
    参数:
        message: 要取消的消息
        text: 取消提示文本
        
    返回:
        是否成功取消
    """
    try:
        # 更新消息为取消状态
        await message.edit_text(text)
        
        # 5秒后删除
        try:
            bot = message.get_bot()
            from telegram.ext import ApplicationBuilder
            if hasattr(bot, 'application') and bot.application:
                app = bot.application
                if 'bot_instance' in app.bot_data:
                    bot_instance = app.bot_data['bot_instance']
                    if hasattr(bot_instance, 'auto_delete_manager'):
                        # 取消现有的删除计划并重新调度为5秒
                        chat_id = message.chat_id
                        await bot_instance.auto_delete_manager.schedule_delete(
                            message, 
                            MessageType.FEEDBACK.value, 
                            chat_id, 
                            5
                        )
                        return True
        except Exception as e:
            logger.error(f"重置删除计时器失败: {e}", exc_info=True)
            
        # 如果找不到自动删除管理器，使用基本方法
        asyncio.create_task(delayed_delete(message, 5))
        
        return True
    except Exception as e:
        logger.error(f"取消交互失败: {e}", exc_info=True)
        return False

# 错误跟踪器，用于记录和分析删除错误
class ErrorTracker:
    """错误跟踪器，用于记录和分析错误"""
    
    def __init__(self):
        """初始化错误跟踪器"""
        self.errors = {}  # 错误计数器
        self.error_history = []  # 错误历史
        self.last_cleanup = datetime.now()
        self.max_history = 100  # 最大历史记录数
    
    def record_error(self, error_type: str, error: Exception, context: Optional[Dict[str, Any]] = None):
        """
        记录错误
        
        参数:
            error_type: 错误类型
            error: 异常对象
            context: 上下文信息
        """
        # 更新错误计数
        if error_type not in self.errors:
            self.errors[error_type] = 0
        self.errors[error_type] += 1
        
        # 记录错误详情
        error_info = {
            'type': error_type,
            'error': str(error),
            'time': datetime.now(),
            'context': context or {},
            'traceback': traceback.format_exc()
        }
        
        # 添加到历史记录
        self.error_history.append(error_info)
        
        # 如果历史记录过长，裁剪
        if len(self.error_history) > self.max_history:
            self.error_history = self.error_history[-self.max_history:]
        
        # 定期清理旧记录
        self._cleanup_old_records()
        
        logger.error(f"记录错误: {error_type} - {error}", exc_info=True)
    
    def _cleanup_old_records(self):
        """清理旧的错误记录"""
        now = datetime.now()
        # 每小时清理一次
        if (now - self.last_cleanup).total_seconds() > 3600:
            # 保留最近24小时的记录
            cutoff = now - timedelta(hours=24)
            self.error_history = [e for e in self.error_history if e['time'] > cutoff]
            self.last_cleanup = now
    
    def get_error_summary(self) -> Dict[str, int]:
        """获取错误摘要"""
        return self.errors.copy()
    
    def get_recent_errors(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        获取最近的错误
        
        参数:
            count: 返回的错误数量
            
        返回:
            最近的错误列表
        """
        return self.error_history[-count:] if self.error_history else []

class ErrorTracker:
    """
    错误跟踪器
    用于跟踪和记录系统中的错误
    """
    
    def __init__(self):
        """初始化错误跟踪器"""
        self.errors = []
        self.max_errors = 100  # 最多保存100条错误记录
        
    def record_error(self, error_type: str, exception: Exception, context: Optional[Dict[str, Any]] = None):
        """
        记录错误
        
        参数:
            error_type: 错误类型
            exception: 异常对象
            context: 错误上下文信息
        """
        error_info = {
            'type': error_type,
            'message': str(exception),
            'timestamp': datetime.now(),
            'context': context or {}
        }
        
        self.errors.append(error_info)
        
        # 如果错误记录超过最大数量，删除最旧的记录
        if len(self.errors) > self.max_errors:
            self.errors = self.errors[-self.max_errors:]
            
        logger.error(f"已记录错误: {error_type} - {exception}")
        
    def get_recent_errors(self, count: int = 10) -> List[Dict[str, Any]]:
        """
        获取最近的错误记录
        
        参数:
            count: 返回的错误记录数量
            
        返回:
            最近的错误记录列表
        """
        return self.errors[-count:]
        
    def get_errors_by_type(self, error_type: str) -> List[Dict[str, Any]]:
        """
        获取特定类型的错误记录
        
        参数:
            error_type: 错误类型
            
        返回:
            指定类型的错误记录列表
        """
        return [e for e in self.errors if e['type'] == error_type]
