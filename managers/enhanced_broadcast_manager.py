Enhanced Auto Delete Manager

"""
增强版自动删除管理器，负责管理并执行消息的定时删除
"""
import logging
import asyncio
import time
import traceback
import enum
from datetime import datetime
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

class MessageType(enum.Enum):
    """消息类型枚举"""
    DEFAULT = 'default'
    KEYWORD = 'keyword'
    BROADCAST = 'broadcast'
    RANKING = 'ranking'
    COMMAND = 'command'
    ERROR = 'error'
    WARNING = 'warning'
    SUCCESS = 'success'
    HELP = 'help'
    INTERACTION = 'interaction'
    FEEDBACK = 'feedback'

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
        self.message_cache = {}  # 缓存消息状态，减少数据库查询
        self.settings_cache = {}  # 缓存群组设置
        self.cache_expiry = {}  # 缓存过期时间
        self.CACHE_TTL = 300  # 缓存有效期（秒）
        
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
                
                # 更新缓存
                self.settings_cache[group_id] = settings
                self.cache_expiry[f"settings_{group_id}"] = datetime.now() + timedelta(seconds=self.CACHE_TTL)
                
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
        # 缓存清理线程
        self.cache_cleanup_task = asyncio.create_task(self._cleanup_cache())
        
        logger.info("自动删除管理器任务已初始化")
    
    async def schedule_delete(self, message: Message, message_type: str = 'default', 
                              chat_id: Optional[int] = None, timeout: Optional[int] = None,
                              retry_on_failure: bool = True, priority: bool = False):
        """
        安排消息删除
        
        参数:
            message: 消息对象
            message_type: 消息类型（default, command, keyword, broadcast, ranking, error, warning, help, feedback, interaction）
            chat_id: 聊天ID，如果不提供则从消息获取
            timeout: 超时时间（秒），如果不提供则根据消息类型和群组设置获取
            retry_on_failure: 是否在失败时重试
            priority: 是否为优先级删除
            
        返回:
            bool: 是否成功安排删除
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
        
        # 优化：使用缓存减少数据库查询负担    
        # 检查自动删除是否启用
        if not await self._is_auto_delete_enabled(chat_id):
            logger.debug(f"群组 {chat_id} 的自动删除未启用，跳过消息: {message.message_id}")
            return False
            
        # 检查用户是否豁免自动删除（如管理员）
        if message.from_user:
            user_id = message.from_user.id
            cache_key = f"user_{user_id}"
            
            # 尝试从缓存获取用户信息
            user = None
            if cache_key in self.message_cache and datetime.now() < self.cache_expiry.get(cache_key, datetime.now()):
                user = self.message_cache[cache_key]
            else:
                user = await self.db.get_user(user_id)
                if user:
                    # 更新缓存
                    self.message_cache[cache_key] = user
                    self.cache_expiry[cache_key] = datetime.now() + timedelta(seconds=self.CACHE_TTL)
            
            if user and is_auto_delete_exempt(user.get('role', ''), message.text):
                logger.debug(f"用户 {user_id} 免除自动删除")
                return False
            
        # 如果未指定超时时间，从数据库获取
        if timeout is None:
            timeout = await self._get_timeout_for_type(chat_id, message_type)
        
        # 强化对轮播消息的处理 
        if message_type == MessageType.BROADCAST.value:
            # 验证timeout是否合理
            if timeout <= 0:
                logger.warning(f"轮播消息删除超时时间无效: {timeout}，使用默认值300秒")
                timeout = 300
            logger.debug(f"配置轮播消息自动删除: message_id={message.message_id}, chat_id={chat_id}, timeout={timeout}秒")
        
        # 将删除任务添加到队列
        delete_time = datetime.now() + timedelta(seconds=timeout)
        
        # 特殊处理优先级任务
        if priority:
            # 创建新队列来临时存储排在前面的任务
            temp_queue = asyncio.Queue()
            # 先处理优先任务
            await self.message_queue.put((message, delete_time, chat_id, retry_on_failure, priority))
            # 然后移回原队列中的所有任务
            while not temp_queue.empty():
                await self.message_queue.put(await temp_queue.get())
        else:
            await self.message_queue.put((message, delete_time, chat_id, retry_on_failure, priority))
        
        # 兼容旧版本：为直接API保存任务引用
        message_id = f"{chat_id}_{message.message_id}"
        self.delete_tasks[message_id] = asyncio.create_task(self._delete_after(message, timeout))
        
        logger.debug(f"已安排消息 {message.message_id} 在 {delete_time} 删除 (类型: {message_type}, 超时: {timeout}秒)")
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
                try:
                    await message.delete()
                    logger.info(f"已删除消息 {message.message_id}")
                except BadRequest as e:
                    if "message to delete not found" in str(e):
                        logger.debug(f"消息已被删除: {message.message_id}")
                    else:
                        logger.warning(f"删除消息时出现BadRequest: {e}, message_id={message.message_id}")
                except Exception as e:
                    logger.error(f"删除消息 {message.message_id} 失败: {e}")
        except asyncio.CancelledError:
            logger.info(f"删除任务已取消: {message.message_id}")
        except Exception as e:
            logger.error(f"删除任务执行出错: {e}, message_id={message.message_id}")
        finally:
            # 清理任务引用
            if message_id in self.delete_tasks:
                del self.delete_tasks[message_id]
    
    async def _message_worker(self):
        """消息删除工作线程"""
        while self.running:
            try:
                # 获取下一个要删除的消息
                message, delete_time, chat_id, retry_on_failure, priority = await self.message_queue.get()
                
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
                        if retry_on_failure:
                            self._add_failed_message(chat_id, message.message_id, str(e))
                except Forbidden as e:
                    logger.warning(f"没有权限删除消息: {e}, chat_id={chat_id}, message_id={message.message_id}")
                    # 记录权限问题
                    if retry_on_failure:
                        self._add_failed_message(chat_id, message.message_id, f"权限错误: {e}")
                except RetryAfter as e:
                    # 处理API限制，稍后重试
                    retry_after = int(str(e).split()[2])
                    logger.warning(f"API限制，将在 {retry_after} 秒后重试: chat_id={chat_id}, message_id={message.message_id}")
                    if retry_on_failure:
                        # 重新排队，延迟处理
                        new_delete_time = datetime.now() + timedelta(seconds=retry_after + 1)
                        await self.message_queue.put((message, new_delete_time, chat_id, retry_on_failure, False))
                except TimedOut as e:
                    logger.warning(f"删除消息超时: {e}, chat_id={chat_id}, message_id={message.message_id}")
                    if retry_on_failure:
                        # 30秒后重试
                        new_delete_time = datetime.now() + timedelta(seconds=30)
                        await self.message_queue.put((message, new_delete_time, chat_id, retry_on_failure, False))
                except Exception as e:
                    logger.error(f"删除消息时出错: {e}, chat_id={chat_id}, message_id={message.message_id}")
                    # 记录其他错误
                    if retry_on_failure:
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
        for key, data in list(self.failed_messages.items()):
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
                    key = f"{chat_id}:{message_id}"
                    if key in self.failed_messages:
                        del self.failed_messages[key]
                    continue
                
                # 尝试删除消息
                try:
                    # 使用指数退避策略，等待时间随重试次数增加
                    await self.bot.delete_message(chat_id=chat_id, message_id=message_id)
                    logger.info(f"成功重新删除消息: chat_id={chat_id}, message_id={message_id}")
                    
                    # 从失败记录中移除
                    key = f"{chat_id}:{message_id}"
                    if key in self.failed_messages:
                        del self.failed_messages[key]
                except RetryAfter as e:
                    # 处理API限制
                    retry_after = int(str(e).split()[2])
                    logger.warning(f"API限制，将在下次重试周期重试: chat_id={chat_id}, message_id={message_id}")
                    # 更新失败记录但不增加重试计数
                    data['time'] = now
                    self.failed_messages[f"{chat_id}:{message_id}"] = data
                except Exception as e:
                    # 其他错误
                    logger.error(f"重试删除消息失败: {e}, chat_id={chat_id}, message_id={message_id}")
                    # 更新失败记录并增加重试计数
                    data['error'] = str(e)
                    data['time'] = now
                    data['retry_count'] += 1
                    self.failed_messages[f"{chat_id}:{message_id}"] = data
            except BadRequest as e:
                # 如果消息已不存在，从失败记录中移除
                if "message to delete not found" in str(e):
                    key = f"{chat_id}:{message_id}"
                    if key in self.failed_messages:
                        del self.failed_messages[key]
                else:
                    # 更新失败记录
                    data['error'] = str(e)
                    data['time'] = now
                    data['retry_count'] += 1
                    self.failed_messages[f"{chat_id}:{message_id}"] = data
            except Exception as e:
                logger.error(f"重试删除消息时出错: {e}, chat_id={chat_id}, message_id={message_id}")
                # 更新失败记录
                data['error'] = str(e)
                data['time'] = now
                data['retry_count'] += 1
                self.failed_messages[f"{chat_id}:{message_id}"] = data
    
    async def _recovery_check(self):
        """恢复检查，处理系统休眠后应该删除的消息"""
        last_check_time = datetime.now()
        
        while self.running:
            try:
                await asyncio.sleep(300)  # 每5分钟检查一次
                
                # 检测是否存在系统休眠
                now = datetime.now()
                time_diff = (now - last_check_time).total_seconds()
                
                # 如果时间差超过10分钟，认为系统可能休眠过
                if time_diff > 600:  # 10分钟
                    logger.warning(f"检测到可能的系统休眠，时间差: {time_diff:.2f}秒")
                    await self._handle_post_sleep_recovery()
                
                # 更新最后检查时间
                last_check_time = now
            except asyncio.CancelledError:
                logger.info("恢复检查任务被取消")
                break
            except Exception as e:
                logger.error(f"恢复检查任务出错: {e}", exc_info=True)
                await asyncio.sleep(300)  # 出错后5分钟再试
    
    async def _handle_post_sleep_recovery(self):
        """处理系统休眠后的恢复"""
        logger.info("开始系统休眠后的恢复处理")
        
        try:
            # 检查是否有消息删除积压
            queue_size = self.message_queue.qsize()
            if queue_size > 0:
                logger.info(f"发现积压的删除任务: {queue_size}个")
                
                # 创建紧急处理任务
                emergency_worker = asyncio.create_task(self._emergency_message_worker())
                
                # 等待紧急工作线程完成
                try:
                    await asyncio.wait_for(emergency_worker, timeout=300)  # 最多等待5分钟
                except asyncio.TimeoutError:
                    logger.warning("紧急工作线程超时，继续正常操作")
                except Exception as e:
                    logger.error(f"紧急工作线程出错: {e}", exc_info=True)
                
                logger.info("紧急处理完成")
            else:
                logger.info("无积压的删除任务，无需恢复处理")
                
            # 清理缓存
            self.settings_cache.clear()
            self.message_cache.clear()
            self.cache_expiry.clear()
            logger.info("已清理缓存")
                
        except Exception as e:
            logger.error(f"恢复处理出错: {e}", exc_info=True)
    
    async def _emergency_message_worker(self):
        """紧急情况下处理积压的删除任务"""
        processed = 0
        skipped = 0
        failed = 0
        start_time = time.time()
        
        try:
            # 创建临时队列存储未过期的消息
            temp_queue = asyncio.Queue()
            
            while not self.message_queue.empty():
                try:
                    # 获取下一个要删除的消息
                    item = await self.message_queue.get_nowait()
                    if len(item) == 5:  # 兼容新格式
                        message, delete_time, chat_id, retry_on_failure, priority = item
                    else:  # 兼容旧格式
                        message, delete_time, chat_id = item
                        retry_on_failure = True
                        priority = False
                    
                    # 检查是否应该删除
                    now = datetime.now()
                    if now >= delete_time:
                        try:
                            await message.delete()
                            logger.debug(f"紧急模式: 已删除过期消息: chat_id={chat_id}, message_id={message.message_id}")
                            processed += 1
                        except Exception as e:
                            logger.warning(f"紧急模式: 删除消息出错: {e}, chat_id={chat_id}, message_id={message.message_id}")
                            # 记录失败的消息
                            if retry_on_failure:
                                self._add_failed_message(chat_id, message.message_id, str(e))
                            failed += 1
                    else:
                        # 对于未过期的消息，放入临时队列
                        await temp_queue.put(item)
                        skipped += 1
                    
                    # 标记任务完成
                    self.message_queue.task_done()
                    
                    # 限制处理速度，避免API限制
                    if (processed + failed) % 20 == 0:
                        await asyncio.sleep(1)
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    logger.error(f"紧急工作线程处理出错: {e}", exc_info=True)
                    await asyncio.sleep(1)
            
            # 将未过期的消息重新放回主队列
            while not temp_queue.empty():
                await self.message_queue.put(await temp_queue.get_nowait())
                
        except Exception as e:
            logger.error(f"紧急工作线程整体执行出错: {e}", exc_info=True)
        finally:
            total_time = time.time() - start_time
            logger.info(f"紧急模式: 处理了 {processed} 条消息，跳过 {skipped} 条，失败 {failed} 条，耗时 {total_time:.2f} 秒")
    
    async def _is_auto_delete_enabled(self, chat_id: int) -> bool:
        """检查群组是否启用了自动删除"""
        try:
            # 尝试从缓存获取设置
            cache_key = f"settings_{chat_id}"
            if cache_key in self.cache_expiry and datetime.now() < self.cache_expiry.get(cache_key, datetime.now()):
                if chat_id in self.settings_cache:
                    settings = self.settings_cache[chat_id]
                    return settings.get('auto_delete', False)
            
            # 从数据库获取设置
            settings = await self.db.get_group_settings(chat_id)
            
            # 更新缓存
            self.settings_cache[chat_id] = settings
            self.cache_expiry[cache_key] = datetime.now() + timedelta(seconds=self.CACHE_TTL)
            
            return settings.get('auto_delete', False)
        except Exception as e:
            logger.error(f"检查自动删除状态出错: {e}", exc_info=True)
            return False
            
    async def _cleanup_cache(self):
        """定期清理过期缓存"""
        while self.running:
            try:
                # 每10分钟执行一次清理
                await asyncio.sleep(600)
                
                now = datetime.now()
                # 清理过期的缓存
                for key in list(self.cache_expiry.keys()):
                    if now > self.cache_expiry[key]:
                        # 根据缓存类型删除对应数据
                        if key.startswith("settings_"):
                            group_id = int(key.split("_")[1])
                            if group_id in self.settings_cache:
                                del self.settings_cache[group_id]
                        elif key.startswith("user_"):
                            user_id = key.split("_")[1]
                            if user_id in self.message_cache:
                                del self.message_cache[user_id]
                        
                        # 删除过期记录
                        del self.cache_expiry[key]
                
                logger.debug(f"清理缓存完成，剩余: {len(self.cache_expiry)} 项")
            except asyncio.CancelledError:
                logger.info("缓存清理任务被取消")
                break
            except Exception as e:
                logger.error(f"缓存清理任务出错: {e}", exc_info=True)
                await asyncio.sleep(300)  # 出错后5分钟再试
    
    async def _get_timeout_for_type(self, chat_id: int, message_type: str) -> int:
        """获取特定消息类型的超时时间"""
        try:
            # 尝试从缓存获取设置
            cache_key = f"settings_{chat_id}"
            settings = None
            
            if cache_key in self.cache_expiry and datetime.now() < self.cache_expiry.get(cache_key, datetime.now()):
                if chat_id in self.settings_cache:
                    settings = self.settings_cache[chat_id]
            else:
                # 从数据库获取设置
                settings = await self.db.get_group_settings(chat_id)
                
                # 更新缓存
                self.settings_cache[chat_id] = settings
                self.cache_expiry[cache_key] = datetime.now() + timedelta(seconds=self.CACHE_TTL)
            
            # 获取特定类型的超时设置
            timeouts = settings.get('auto_delete_timeouts', {})
            
            # 如果指定类型的超时设置存在，则使用它
            if message_type in timeouts:
                return timeouts[message_type]
            
            # 否则使用默认超时设置
            return settings.get('auto_delete_timeout', self.default_timeouts.get(message_type, 300))
