"""
增强版轮播消息管理器，支持时间校准和智能调度
"""
import logging
import asyncio
from datetime import datetime, timedelta
import math
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, Forbidden

logger = logging.getLogger(__name__)

class EnhancedBroadcastManager:
    """增强版轮播消息管理器"""
    
    def __init__(self, db, bot_instance):
        """
        初始化轮播消息管理器
        
        参数:
            db: 数据库实例
            bot_instance: 机器人实例
        """
        self.db = db
        self.bot_instance = bot_instance
        self.calibration_manager = None
        self._running = False
        self._task = None
        self._force_check_event = asyncio.Event()
        self._processing_lock = asyncio.Lock()
        self._check_interval = 30  # 默认每30秒检查一次
        self._last_check_time = datetime.now()
        self._startup_time = datetime.now()  # 记录启动时间，用于休眠检测
        
    async def start(self):
        """启动轮播消息管理器"""
        if self._running:
            return
            
        logger.info("启动轮播消息管理器")
        self._running = True
        
        # 强制重新计算所有轮播消息的下一次执行时间
        # 如果有时间校准管理器，则使用它
        if self.calibration_manager:
            await self.calibration_manager.force_recalculate_all()
        
        # 启动轮播任务
        self._task = asyncio.create_task(self._broadcast_loop())
        logger.info("轮播消息管理器已启动")
        
    async def stop(self):
        """停止轮播消息管理器"""
        if not self._running:
            return
            
        logger.info("停止轮播消息管理器")
        self._running = False
        
        # 取消轮播任务
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        logger.info("轮播消息管理器已停止")
        
    async def force_check(self):
        """强制执行一次轮播检查，用于系统唤醒后"""
        logger.info("强制执行轮播检查")
        self._force_check_event.set()
        
    async def _broadcast_loop(self):
        """轮播消息检查循环"""
        try:
            while self._running:
                # 记录循环开始时间
                loop_start_time = time.time()
                
                # 处理所有需要发送的轮播消息
                await self._process_broadcasts()
                
                # 计算下一次检查的间隔
                elapsed = time.time() - loop_start_time
                sleep_time = max(1, self._check_interval - elapsed)
                
                # 等待间隔时间或强制检查事件
                try:
                    await asyncio.wait_for(self._force_check_event.wait(), timeout=sleep_time)
                    self._force_check_event.clear()  # 清除事件
                except asyncio.TimeoutError:
                    pass  # 正常等待超时
                
                # 检查系统是否刚从休眠中唤醒
                await self._check_system_wake()
        
        except asyncio.CancelledError:
            logger.info("轮播循环已取消")
            raise
        except Exception as e:
            logger.error(f"轮播循环异常: {e}", exc_info=True)
            
    async def _check_system_wake(self):
        """检查系统是否刚从休眠中唤醒"""
        current_time = datetime.now()
        expected_time = self._last_check_time + timedelta(seconds=self._check_interval + 5)  # 添加5秒容差
        
        # 计算时间偏移（秒）
        time_drift = (current_time - expected_time).total_seconds()
        
        # 更新最后检查时间
        self._last_check_time = current_time
        
        # 如果时间偏移超过阈值（比如30秒），可能发生了休眠
        if time_drift > 30:
            logger.warning(f"检测到轮播管理器可能因系统休眠而暂停，时间偏移: {time_drift:.2f}秒")
            # 将此信息通知时间校准管理器
            if self.calibration_manager:
                # 时间校准管理器会处理系统唤醒后的调整
                pass
                
    async def _process_broadcasts(self):
        """处理所有需要发送的轮播消息"""
        # 使用锁防止并发处理
        async with self._processing_lock:
            try:
                # 获取当前时间
                now = datetime.now()
                logger.info(f"开始处理轮播消息，当前时间: {now}")
                
                # 获取应该发送的轮播消息
                due_broadcasts = await self.db.get_due_broadcasts()
                
                # 添加详细日志
                if due_broadcasts:
                    logger.info(f"找到 {len(due_broadcasts)} 个需要发送的轮播消息")
                    for b in due_broadcasts:
                        logger.info(f"准备发送轮播 - ID: {b['_id']}, 群组: {b.get('group_id')}, "
                                  f"开始时间: {b.get('start_time')} ({type(b.get('start_time')).__name__}), "
                                  f"结束时间: {b.get('end_time')} ({type(b.get('end_time')).__name__}), "
                                  f"上次发送: {b.get('last_broadcast')}, "
                                  f"间隔: {b.get('interval')}分钟")
                else:
                    logger.info("没有找到需要发送的轮播消息")
                
                for broadcast in due_broadcasts:
                    # 检查是否在合理的时间范围内
                    broadcast_id = str(broadcast["_id"])
                    
                    # 如果有时间校准管理器，获取下一次执行时间
                    next_time = None
                    if self.calibration_manager:
                        next_time = await self.calibration_manager.get_next_execution_time(broadcast_id)
                        if next_time:
                            logger.info(f"轮播 {broadcast_id} 的校准执行时间: {next_time}")
                    
                    # 如果有预期的下一次执行时间，检查是否到期
                    if next_time and next_time > now:
                        logger.debug(f"轮播消息 {broadcast_id} 的下一次执行时间是 {next_time}，尚未到期")
                        continue
                    
                    # 检查群组的轮播功能开关
                    group_id = broadcast.get("group_id")
                    group = await self.db.get_group(group_id)
                    if group:
                        feature_switches = group.get("feature_switches", {})
                        if not feature_switches.get("broadcast", True):
                            logger.info(f"群组 {group_id} 的轮播功能已关闭，跳过发送")
                            continue
                    
                    # 发送轮播消息
                    await self.send_broadcast(broadcast)
                    
            except Exception as e:
                logger.error(f"处理轮播消息时出错: {e}", exc_info=True)
    
    async def send_broadcast(self, broadcast):
        """
        发送轮播消息
        
        参数:
            broadcast: 轮播消息对象
        """
        try:
            broadcast_id = str(broadcast["_id"])
            group_id = broadcast.get("group_id")
            logger.info(f"发送轮播消息: {broadcast_id} 到群组 {group_id}")
            logger.info(f"轮播时间信息: 开始={broadcast.get('start_time')} ({type(broadcast.get('start_time')).__name__}), "
                       f"结束={broadcast.get('end_time')} ({type(broadcast.get('end_time')).__name__}), "
                       f"上次发送={broadcast.get('last_broadcast')}, 间隔={broadcast.get('interval')}分钟")
            
            # 获取消息内容
            text = broadcast.get("text", "")
            media = broadcast.get("media")
            buttons = broadcast.get("buttons", [])
            
            # 创建按钮键盘
            reply_markup = None
            if buttons:
                keyboard = []
                for button in buttons:
                    keyboard.append([InlineKeyboardButton(button["text"], url=button["url"])])
                reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 发送消息
            sent_message = None
            if media and media.get("type"):
                if media["type"] == "photo":
                    sent_message = await self.bot_instance.application.bot.send_photo(
                        chat_id=group_id,
                        photo=media["file_id"],
                        caption=text if text else None,
                        reply_markup=reply_markup
                    )
                elif media["type"] == "video":
                    sent_message = await self.bot_instance.application.bot.send_video(
                        chat_id=group_id,
                        video=media["file_id"],
                        caption=text if text else None,
                        reply_markup=reply_markup
                    )
                elif media["type"] == "document":
                    sent_message = await self.bot_instance.application.bot.send_document(
                        chat_id=group_id,
                        document=media["file_id"],
                        caption=text if text else None,
                        reply_markup=reply_markup
                    )
                else:
                    logger.warning(f"未知的媒体类型: {media['type']}")
            else:
                sent_message = await self.bot_instance.application.bot.send_message(
                    chat_id=group_id,
                    text=text or "轮播消息",
                    reply_markup=reply_markup
                )
            
            # 更新最后发送时间
            current_time = datetime.now()
            await self.db.update_broadcast_time(broadcast_id, current_time)
            
            # 通知时间校准管理器
            if self.calibration_manager:
                await self.calibration_manager.handle_broadcast_sent(broadcast_id)
            
            logger.info(f"成功发送轮播消息: {broadcast_id}, 消息ID: {sent_message.message_id if sent_message else 'unknown'}")
            
        except BadRequest as e:
            logger.error(f"发送轮播消息时出现Bad Request错误: {str(e)}")
            # 可能的无效文件ID或媒体过期
            if "wrong file_id" in str(e).lower() or "file is too big" in str(e).lower():
                logger.warning(f"轮播消息 {broadcast_id} 的媒体文件无效，将禁用媒体")
                # 禁用媒体，仅保留文本
                await self.db.update_broadcast(broadcast_id, {"media": None})
        except Forbidden as e:
            logger.error(f"发送轮播消息时被拒绝: {str(e)}")
            # 机器人可能被踢出群组
            if "bot was kicked" in str(e).lower() or "chat not found" in str(e).lower():
                logger.warning(f"机器人可能已被踢出群组 {group_id}，将禁用该群组的轮播")
                # 可以选择禁用该群组的轮播功能
                group = await self.db.get_group(group_id)
                if group:
                    feature_switches = group.get("feature_switches", {})
                    feature_switches["broadcast"] = False
                    await self.db.update_group_settings(group_id, {"feature_switches": feature_switches})
        except Exception as e:
            logger.error(f"发送轮播消息时出错: {str(e)}", exc_info=True)
            
    async def process_broadcasts(self):
        """
        处理轮播消息，兼容旧接口
        """
        # 强制执行一次检查
        await self.force_check()
