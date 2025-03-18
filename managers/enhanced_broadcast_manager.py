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
        logger.info("强制执行轮播检查开始")
        self._force_check_event.set()
        
        # 添加显式调用
        try:
            logger.info("直接调用_process_broadcasts进行轮播检查")
            await self._process_broadcasts()
        except Exception as e:
            logger.error(f"强制轮播检查出错: {e}", exc_info=True)
        
        logger.info("强制执行轮播检查结束")
        
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
                logger.info(f"当前时间: {now}, 准备查询应发送的轮播消息")
                
                # 获取应该发送的轮播消息
                logger.info(f"开始调用 get_due_broadcasts() 获取待发送的轮播消息")
                due_broadcasts = await self.db.get_due_broadcasts()
                
                if due_broadcasts:
                    logger.info(f"找到 {len(due_broadcasts)} 个需要发送的轮播消息")
                    for b in due_broadcasts:
                        b_id = str(b['_id'])
                        logger.info(f"待发送轮播: ID={b_id}, 群组={b.get('group_id')}, "
                                   f"开始时间={b.get('start_time')}, 结束时间={b.get('end_time')}, "
                                   f"上次发送时间={b.get('last_broadcast')}, 间隔={b.get('interval')}分钟")
                else:
                    logger.info("没有找到需要发送的轮播消息")
                    possible_broadcasts = await self.db.db.broadcasts.find({}).to_list(None)
                    if possible_broadcasts:
                        logger.info(f"数据库中有 {len(possible_broadcasts)} 条轮播消息，但没有符合发送条件的")
                        # 随机抽取一个轮播消息进行详细检查
                        if len(possible_broadcasts) > 0:
                            sample = possible_broadcasts[0]
                            sample_id = str(sample['_id'])
                            logger.info(f"随机抽查一条轮播消息 ID={sample_id}")
                            await self.db.inspect_broadcast(sample_id)
                    else:
                        logger.info("数据库中没有轮播消息")
                
                # 添加详细日志
                if due_broadcasts:
                    logger.info(f"找到 {len(due_broadcasts)} 个需要发送的轮播消息")
                    for b in due_broadcasts:
                        b_id = str(b['_id'])
                        logger.info(f"【轮播ID: {b_id}】")
                        logger.info(f"  - 群组ID: {b.get('group_id')}")
                        logger.info(f"  - 开始时间: {b.get('start_time')} ({type(b.get('start_time')).__name__})")
                        logger.info(f"  - 结束时间: {b.get('end_time')} ({type(b.get('end_time')).__name__})")
                        logger.info(f"  - 上次发送: {b.get('last_broadcast')} ({type(b.get('last_broadcast') or 'None').__name__})")
                        logger.info(f"  - 间隔分钟: {b.get('interval')}")
                        logger.info(f"  - 重复类型: {b.get('repeat_type')}")
                        logger.info(f"  - 内容类型: {'有媒体' if b.get('media') else '纯文本'}")
                else:
                    logger.info("没有找到需要发送的轮播消息")
                    logger.info("检查可能的原因:")
                    logger.info("1. 时间条件未满足 - 当前时间不在轮播设定的时间范围内")
                    logger.info("2. 间隔条件未满足 - 距离上次发送未到设定的间隔时间")
                    logger.info("3. 数据格式问题 - 时间字段格式不一致，无法正确比较")
                
                for broadcast in due_broadcasts:
                    broadcast_id = str(broadcast["_id"])
                    group_id = broadcast.get("group_id")
                    
                    # 检查时间校准系统的下一次执行时间
                    next_time = None
                    if self.calibration_manager:
                        next_time = await self.calibration_manager.get_next_execution_time(broadcast_id)
                        if next_time:
                            time_diff = (next_time - now).total_seconds()
                            logger.info(f"轮播 {broadcast_id} 的校准执行时间: {next_time}, 与当前时间相差: {time_diff:.2f}秒")
                            if next_time > now:
                                logger.info(f"根据时间校准系统，轮播 {broadcast_id} 的执行时间 {next_time} 尚未到达，跳过")
                                continue
                            else:
                                logger.info(f"轮播 {broadcast_id} 的执行时间已到达，准备发送")
                        else:
                            logger.info(f"轮播 {broadcast_id} 在时间校准系统中没有下一次执行时间记录")
                    
                    # 检查群组的轮播功能开关
                    logger.info(f"正在检查群组 {group_id} 的轮播功能开关")
                    group = await self.db.get_group(group_id)
                    if group:
                        feature_switches = group.get("feature_switches", {})
                        broadcast_enabled = feature_switches.get("broadcast", True)
                        logger.info(f"群组 {group_id} 的轮播功能状态: {'开启' if broadcast_enabled else '关闭'}")
                        if not broadcast_enabled:
                            logger.info(f"群组 {group_id} 的轮播功能已关闭，跳过发送")
                            continue
                    else:
                        logger.warning(f"找不到群组 {group_id} 的信息，可能已被删除")
                        continue
                    
                    # 准备发送轮播消息
                    logger.info(f"准备发送轮播消息 {broadcast_id} 到群组 {group_id}")
                    
                    # 发送轮播消息
                    try:
                        await self.send_broadcast(broadcast)
                        logger.info(f"成功发送轮播消息 {broadcast_id} 到群组 {group_id}")
                    except Exception as e:
                        logger.error(f"发送轮播消息 {broadcast_id} 时出错: {e}", exc_info=True)
                    
            except Exception as e:
                logger.error(f"处理轮播消息时出错: {e}", exc_info=True)

    async def recalibrate_broadcast_time(self, broadcast_id):
        """重新校准轮播消息回到原始设定的固定时间点"""
        try:
            broadcast = await self.db.get_broadcast_by_id(broadcast_id)
            if not broadcast:
                logger.error(f"找不到轮播消息: {broadcast_id}")
                return False
                
            # 检查是否有调度时间
            schedule_time = broadcast.get('schedule_time')
            if not schedule_time:
                # 如果没有固定调度时间，则从开始时间提取
                start_time = broadcast.get('start_time')
                if not start_time:
                    logger.error(f"轮播 {broadcast_id} 没有开始时间，无法校准")
                    return False
                    
                schedule_time = f"{start_time.hour}:{start_time.minute:02d}"
                # 保存到数据库
                await self.db.update_broadcast(broadcast_id, {'schedule_time': schedule_time})
                logger.info(f"从开始时间提取并设置轮播 {broadcast_id} 的调度时间为 {schedule_time}")
            
            # 将更新标志设置为已启用固定时间
            await self.db.update_broadcast(broadcast_id, {'use_fixed_time': True})
            
            logger.info(f"已重置轮播 {broadcast_id} 的时间调度，下次将按固定时间 {schedule_time} 发送")
            return True
        except Exception as e:
            logger.error(f"重置轮播时间调度失败: {e}", exc_info=True)
            return False
    
    async def send_broadcast(self, broadcast):
        """
        发送轮播消息
        
        参数:
            broadcast: 轮播消息对象
        """
        try:
            broadcast_id = str(broadcast["_id"])
            group_id = broadcast.get("group_id")
            logger.info(f"准备发送轮播消息: {broadcast_id} 到群组 {group_id}")
            logger.info(f"轮播消息详情: {broadcast}")
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
