"""
增强版轮播消息管理器，处理定时消息发送
添加锚点时间状态记录，避免重复发送
"""
import logging
import asyncio
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple, Union, Set
import random

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Bot, Message
from telegram.error import BadRequest, Forbidden, TelegramError, TimedOut, RetryAfter

logger = logging.getLogger(__name__)

class EnhancedBroadcastManager:
    """
    增强版轮播消息管理器，处理定时消息的发送
    支持固定时间发送和间隔发送两种模式
    """
    def __init__(self, db, bot_instance, apply_defaults=True):
        """初始化轮播消息管理器"""
        self.db = db
        self.bot = bot_instance
        self.active_broadcasts = set()  # 用于跟踪正在处理的轮播消息
        self.broadcast_cache = {}  # 缓存轮播消息数据
        self.last_cache_cleanup = datetime.now()
        self.sending_lock = asyncio.Lock()  # 避免并发发送同一条轮播消息
        self.CACHE_TTL = 300  # 缓存有效期（秒）
        self.error_tracker = {}  # 记录发送错误
        self.retry_tracker = {}  # 跟踪重试状态
        self.MAX_ERROR_COUNT = 6  # 最大错误次数
        self.RETRY_ATTEMPTS = 3   # 最大重试次数
        self.RETRY_INTERVALS = [60, 180]  # 重试间隔（秒）
        
        # 新增：锚点处理状态记录
        self.anchor_processed = {}  # 格式: {broadcast_id: {anchor_time: timestamp}}
        self.ANCHOR_RECORD_TTL = 3600  # 锚点记录保留1小时
        
        # 启动后台任务
        self.running = True
        self.cache_cleanup_task = asyncio.create_task(self._cleanup_cache())
        
        # 只在首次初始化时应用默认设置
        if apply_defaults:
            asyncio.create_task(self._apply_default_settings())
            
    async def _apply_default_settings(self):
        """应用默认轮播设置"""
        try:
            from config import BROADCAST_SETTINGS
            logger.info("应用默认轮播设置...")
            
            # 应用默认设置的逻辑
            default_intervals = BROADCAST_SETTINGS.get('default_intervals', [30, 60, 240])
            
            # 将默认间隔选项保存到数据库配置中
            system_settings = await self.db.get_system_settings()
            if 'broadcast_intervals' not in system_settings:
                system_settings['broadcast_intervals'] = default_intervals
                await self.db.update_system_settings(system_settings)
                logger.info(f"已设置默认轮播间隔选项: {default_intervals}分钟")
            
            # 其他默认设置...
        except Exception as e:
            logger.error(f"应用默认轮播设置失败: {e}", exc_info=True)
    
    async def _cleanup_cache(self):
        """定期清理过期缓存"""
        while self.running:
            try:
                # 每10分钟执行一次清理
                await asyncio.sleep(600)
                
                now = datetime.now()
                if (now - self.last_cache_cleanup).total_seconds() > 600:
                    # 清理过期的缓存
                    to_remove = []
                    for broadcast_id, data in self.broadcast_cache.items():
                        if 'timestamp' in data and (now - data['timestamp']).total_seconds() > self.CACHE_TTL:
                            to_remove.append(broadcast_id)
                    
                    # 删除过期的缓存
                    for broadcast_id in to_remove:
                        if broadcast_id in self.broadcast_cache:
                            del self.broadcast_cache[broadcast_id]
                    
                    self.last_cache_cleanup = now
                    logger.debug(f"清理轮播缓存完成，移除 {len(to_remove)} 项")
            except asyncio.CancelledError:
                logger.info("轮播缓存清理任务被取消")
                break
            except Exception as e:
                logger.error(f"轮播缓存清理任务出错: {e}", exc_info=True)
                await asyncio.sleep(300)  # 出错后5分钟再试
    
    
    async def add_broadcast(self, broadcast_data: Dict[str, Any]) -> Optional[str]:
        """
        添加轮播消息
        
        参数:
            broadcast_data: 轮播消息数据
            
        返回:
            轮播消息ID或None
        """
        try:
            # 验证轮播消息数据
            self._validate_broadcast_data(broadcast_data)
            
            # 设置调度时间
            if 'start_time' not in broadcast_data:
                broadcast_data['start_time'] = datetime.now()
            
            start_time = broadcast_data['start_time']
            # 保存时间格式为 "HH:MM" 用于固定时间发送
            schedule_time = f"{start_time.hour:02d}:{start_time.minute:02d}"
            broadcast_data['schedule_time'] = schedule_time
            logger.info(f"设置固定调度时间: {schedule_time}")
            
            # 重置last_broadcast，确保下次固定时间发送正常进行
            broadcast_data['last_broadcast'] = None
            
            # 添加到数据库
            broadcast_id = await self.db.add_broadcast(broadcast_data)
            logger.info(f"已添加轮播消息: {broadcast_id}")
            
            # 注册到时间校准系统
            if hasattr(self.bot, 'calibration_manager') and self.bot.calibration_manager:
                broadcast = await self.db.get_broadcast_by_id(str(broadcast_id))
                if broadcast:
                    await self.bot.calibration_manager.register_broadcast(broadcast)
                    logger.info(f"已注册轮播消息 {broadcast_id} 到时间校准系统")
            
            return str(broadcast_id)
        except Exception as e:
            logger.error(f"添加轮播消息失败: {e}", exc_info=True)
            raise
            
    def _validate_broadcast_data(self, data: Dict[str, Any]):
        """
        验证轮播消息数据
        
        参数:
            data: 轮播消息数据
            
        抛出:
            ValueError: 数据无效
        """
        # 检查必要字段
        required_fields = ['group_id']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"缺少必要字段: {field}")
                
        # 确保有内容
        if not data.get('text') and not data.get('media') and not data.get('buttons', []):
            raise ValueError("轮播消息必须包含文本、媒体或按钮中的至少一项")
        
        # 如果没有start_time，设置为当前时间
        if 'start_time' not in data:
            data['start_time'] = datetime.now()
            
        # 如果没有repeat_type，设置为'once'
        if 'repeat_type' not in data:
            data['repeat_type'] = 'once'
            
        # 如果没有interval，根据repeat_type设置默认值
        if 'interval' not in data:
            if data['repeat_type'] == 'once':
                data['interval'] = 0
            elif data['repeat_type'] == 'hourly':
                data['interval'] = 60
            elif data['repeat_type'] == 'daily':
                data['interval'] = 1440
            else:
                data['interval'] = 30  # 默认30分钟
                
        # 如果是单次发送，end_time与start_time相同
        if data['repeat_type'] == 'once':
            data['end_time'] = data['start_time']
        # 如果没有end_time且不是单次发送，设置为30天后
        elif 'end_time' not in data:
            data['end_time'] = data['start_time'] + timedelta(days=30)
            
        # 验证间隔
        import config
        min_interval = config.BROADCAST_SETTINGS.get('min_interval', 5)  # 默认最小5分钟
        if data['interval'] < min_interval and data['repeat_type'] != 'once':
            raise ValueError(f"间隔不能小于 {min_interval} 分钟")
    
    async def send_broadcast(self, broadcast: Dict[str, Any]) -> bool:
        """
        发送轮播消息到指定群组
        
        参数:
            broadcast: 轮播消息数据
            
        返回:
            是否成功发送
        """
        try:
            broadcast_id = str(broadcast.get('_id', ''))
            group_id = broadcast['group_id']
            
            # 获取消息内容
            text = broadcast.get('text', '')
            media = broadcast.get('media')
            buttons = broadcast.get('buttons', [])
            
            # 构建按钮标记
            reply_markup = None
            if buttons:
                keyboard = []
                # 检查buttons是否是二维数组
                if buttons and isinstance(buttons[0], list):
                    # 二维数组格式处理
                    for row in buttons:
                        keyboard_row = []
                        for button in row:
                            if isinstance(button, dict):
                                # 支持URL和回调按钮
                                if 'url' in button:
                                    keyboard_row.append(InlineKeyboardButton(
                                        text=button['text'],
                                        url=button['url']
                                    ))
                                elif 'callback_data' in button:
                                    keyboard_row.append(InlineKeyboardButton(
                                        text=button['text'],
                                        callback_data=button['callback_data']
                                    ))
                        if keyboard_row:
                            keyboard.append(keyboard_row)
                else:
                    # 一维数组格式处理
                    keyboard_row = []
                    for button in buttons:
                        if isinstance(button, dict):
                            # 支持URL和回调按钮
                            if 'url' in button:
                                keyboard_row.append(InlineKeyboardButton(
                                    text=button['text'],
                                    url=button['url']
                                ))
                            elif 'callback_data' in button:
                                keyboard_row.append(InlineKeyboardButton(
                                    text=button['text'],
                                    callback_data=button['callback_data']
                                ))
                    if keyboard_row:
                        keyboard.append(keyboard_row)
                        
                if keyboard:
                    reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 发送消息
            msg = None
            if media and media.get('type'):
                # 使用底层的Telegram Bot API发送媒体消息
                if media['type'] == 'photo':
                    msg = await self.bot.application.bot.send_photo(
                        chat_id=group_id,
                        photo=media['file_id'],
                        caption=text,
                        reply_markup=reply_markup
                    )
                elif media['type'] == 'video':
                    msg = await self.bot.application.bot.send_video(
                        chat_id=group_id,
                        video=media['file_id'],
                        caption=text,
                        reply_markup=reply_markup
                    )
                elif media['type'] == 'document':
                    msg = await self.bot.application.bot.send_document(
                        chat_id=group_id,
                        document=media['file_id'],
                        caption=text,
                        reply_markup=reply_markup
                    )
                elif media['type'] == 'animation':
                    msg = await self.bot.application.bot.send_animation(
                        chat_id=group_id,
                        animation=media['file_id'],
                        caption=text,
                        reply_markup=reply_markup
                    )
                else:
                    # 默认作为文档发送
                    msg = await self.bot.application.bot.send_document(
                        chat_id=group_id,
                        document=media['file_id'],
                        caption=text,
                        reply_markup=reply_markup
                    )
            else:
                # 纯文本消息或只有按钮的消息
                msg = await self.bot.application.bot.send_message(
                    chat_id=group_id,
                    text=text or "轮播消息",
                    reply_markup=reply_markup
                )
                
            # 处理自动删除
            if msg:
                # 记录已发送的消息ID
                message_data = {
                    'message_id': msg.message_id,
                    'chat_id': msg.chat.id,
                    'date': msg.date
                }
                await self.db.update_broadcast(broadcast_id, {
                    'last_message': message_data
                })
                
                # 如果配置了自动删除，安排删除任务
                if hasattr(self.bot, 'auto_delete_manager') and self.bot.auto_delete_manager:
                    settings = await self.db.get_group_settings(group_id)
                    if settings.get('auto_delete', False):
                        await self.bot.auto_delete_manager.schedule_delete(
                            message=msg,
                            message_type='broadcast',
                            chat_id=group_id
                        )
                    
            logger.info(f"已发送轮播消息: group_id={group_id}, broadcast_id={broadcast_id}")
            return True
            
        except Exception as e:
            logger.error(f"发送轮播消息错误: {e}, broadcast_id={broadcast_id}", exc_info=True)
            return False

    async def send_broadcast_now(self, broadcast_id: str, group_id: int) -> bool:
        """
        立即发送轮播消息，用于强制发送
        
        参数:
            broadcast_id: 轮播消息ID
            group_id: 群组ID
            
        返回:
            是否发送成功
        """
        try:
            # 获取轮播消息数据
            broadcast = await self.db.get_broadcast_by_id(broadcast_id)
            if not broadcast:
                logger.error(f"找不到轮播消息: {broadcast_id}")
                return False
                
            # 发送消息
            success = await self.send_broadcast(broadcast)
            
            if success:
                # 更新最后发送时间
                now = datetime.now()
                await self.db.update_broadcast_time(broadcast_id, now)
                logger.info(f"强制发送轮播消息成功: {broadcast_id}, 更新最后发送时间为 {now}")
                return True
            else:
                logger.error(f"强制发送轮播消息失败: {broadcast_id}")
                return False
        except Exception as e:
            logger.error(f"强制发送轮播消息出错: {broadcast_id}, {e}", exc_info=True)
            return False
    
    async def update_broadcast(self, broadcast_id: str, broadcast_data: Dict[str, Any]) -> bool:
        """
        更新轮播消息
        
        参数:
            broadcast_id: 轮播消息ID
            broadcast_data: 更新的数据
            
        返回:
            是否成功
        """
        try:
            # 检查是否有start_time
            if 'start_time' in broadcast_data:
                start_time = broadcast_data['start_time']
                # 保存时间格式为 "HH:MM" 用于固定时间发送
                schedule_time = f"{start_time.hour:02d}:{start_time.minute:02d}"
                broadcast_data['schedule_time'] = schedule_time
                logger.info(f"更新轮播消息 {broadcast_id} 的固定调度时间: {schedule_time}")
            else:
                # 获取当前轮播消息
                current_broadcast = await self.db.get_broadcast_by_id(broadcast_id)
                if current_broadcast and 'start_time' in current_broadcast:
                    start_time = current_broadcast['start_time']
                    schedule_time = f"{start_time.hour:02d}:{start_time.minute:02d}"
                    broadcast_data['schedule_time'] = schedule_time
            
            # 更新数据库
            success = await self.db.update_broadcast(broadcast_id, broadcast_data)
            
            if success:
                logger.info(f"已更新轮播消息: {broadcast_id}")
                
                # 清除缓存
                if broadcast_id in self.broadcast_cache:
                    del self.broadcast_cache[broadcast_id]
                
                # 更新时间校准系统
                if hasattr(self.bot, 'calibration_manager') and self.bot.calibration_manager:
                    broadcast = await self.db.get_broadcast_by_id(broadcast_id)
                    if broadcast:
                        await self.bot.calibration_manager.register_broadcast(broadcast)
                        logger.info(f"已更新轮播消息 {broadcast_id} 在时间校准系统中的注册")
            
            return success
        except Exception as e:
            logger.error(f"更新轮播消息失败: {e}", exc_info=True)
            return False
            
    async def recalibrate_broadcast_time(self, broadcast_id: str) -> bool:
        """
        重置轮播消息时间调度，确保下次按固定时间发送
        
        参数:
            broadcast_id: 轮播消息ID
            
        返回:
            是否成功
        """
        try:
            # 获取轮播消息
            broadcast = await self.db.get_broadcast_by_id(broadcast_id)
            if not broadcast:
                logger.warning(f"找不到轮播消息: {broadcast_id}")
                return False
            
            # 只处理需要重复发送的消息
            if broadcast.get('repeat_type') == 'once':
                logger.warning(f"轮播消息 {broadcast_id} 是单次发送，无需重置")
                return False
            
            # 确保有schedule_time
            if 'schedule_time' not in broadcast:
                # 根据start_time设置schedule_time
                start_time = broadcast.get('start_time')
                if not start_time:
                    start_time = datetime.now()
                
                schedule_time = f"{start_time.hour:02d}:{start_time.minute:02d}"
                await self.update_broadcast(broadcast_id, {'schedule_time': schedule_time})
                logger.info(f"已设置轮播消息 {broadcast_id} 的固定调度时间: {schedule_time}")
            
            # 重置last_broadcast，确保下次固定时间发送正常进行
            await self.update_broadcast(broadcast_id, {'last_broadcast': None})
            logger.info(f"已重置轮播消息 {broadcast_id} 的发送时间")
            
            return True
        except Exception as e:
            logger.error(f"重置轮播消息时间调度失败: {e}", exc_info=True)
            return False
            
    async def remove_broadcast(self, broadcast_id: str) -> bool:
        """
        删除轮播消息
        
        参数:
            broadcast_id: 轮播消息ID
            
        返回:
            是否成功
        """
        try:
            # 删除数据库记录
            success = await self.db.delete_broadcast(broadcast_id)
            
            if success:
                logger.info(f"已删除轮播消息: {broadcast_id}")
                
                # 清除缓存
                if broadcast_id in self.broadcast_cache:
                    del self.broadcast_cache[broadcast_id]
                
                # 清除错误记录
                if broadcast_id in self.error_tracker:
                    del self.error_tracker[broadcast_id]
                
                # 从时间校准系统中移除
                if hasattr(self.bot, 'calibration_manager') and self.bot.calibration_manager:
                    await self.bot.calibration_manager.unregister_broadcast(broadcast_id)
                    logger.info(f"已从时间校准系统中移除轮播消息 {broadcast_id}")
            
            return success
        except Exception as e:
            logger.error(f"删除轮播消息失败: {e}", exc_info=True)
            return False
            
    async def get_broadcasts(self, group_id: int) -> List[Dict[str, Any]]:
        """
        获取群组的轮播消息
        
        参数:
            group_id: 群组ID
            
        返回:
            轮播消息列表
        """
        try:
            # 从数据库获取
            broadcasts = await self.db.get_broadcasts(group_id)
            
            # 优化轮播消息显示状态
            for broadcast in broadcasts:
                # 添加当前状态字段
                broadcast['status'] = self._get_broadcast_status(broadcast)
                
                # 添加下次发送时间估计
                broadcast['next_send_time'] = self._calculate_next_send_time(broadcast)
                
                # 添加错误计数
                broadcast_id = str(broadcast.get('_id', ''))
                if broadcast_id in self.error_tracker:
                    broadcast['error_count'] = self.error_tracker[broadcast_id]['count']
                    broadcast['last_error'] = self.error_tracker[broadcast_id]['last_error']
            
            return broadcasts
        except Exception as e:
            logger.error(f"获取轮播消息失败: {e}", exc_info=True)
            return []
    
    def _get_broadcast_status(self, broadcast: Dict[str, Any]) -> str:
        """
        获取轮播消息当前状态
        
        参数:
            broadcast: 轮播消息数据
            
        返回:
            状态描述
        """
        now = datetime.now()
        start_time = broadcast.get('start_time')
        end_time = broadcast.get('end_time')
        
        # 检查错误状态
        broadcast_id = str(broadcast.get('_id', ''))
        if broadcast_id in self.error_tracker and self.error_tracker[broadcast_id]['count'] >= self.MAX_ERROR_COUNT:
            return "已暂停(错误过多)"
        
        # 检查时间状态
        if not start_time or now < start_time:
            return "未开始"
        elif not end_time or now > end_time:
            return "已结束"
        else:
            repeat_type = broadcast.get('repeat_type')
            if repeat_type == 'once':
                if broadcast.get('last_broadcast'):
                    return "已发送"
                else:
                    return "待发送"
            else:
                # 检查是否正在发送
                if broadcast_id in self.active_broadcasts:
                    return "正在发送"
                else:
                    # 返回发送模式
                    if repeat_type == 'hourly':
                        return "每小时固定时间发送"
                    elif repeat_type == 'daily':
                        return "每天固定时间发送"
                    else:
                        interval = broadcast.get('interval', 0)
                        return f"每{interval}分钟固定发送"
    
    def _calculate_next_send_time(self, broadcast: Dict[str, Any]) -> Optional[datetime]:
        """
        计算下次发送时间
        
        参数:
            broadcast: 轮播消息数据
            
        返回:
            预计下次发送时间
        """
        now = datetime.now()
        start_time = broadcast.get('start_time')
        end_time = broadcast.get('end_time')
        repeat_type = broadcast.get('repeat_type')
        interval = broadcast.get('interval', 0)
        
        # 检查消息是否在有效期内
        if not start_time or now < start_time:
            return start_time
        elif not end_time or now > end_time:
            return None
        
        # 单次发送
        if repeat_type == 'once':
            if broadcast.get('last_broadcast'):
                return None  # 已发送，不再发送
            else:
                return start_time  # 未发送，按开始时间发送
        
        # 固定时间发送
        schedule_time = broadcast.get('schedule_time')
        if schedule_time:
            hour, minute = map(int, schedule_time.split(':'))
            
            if repeat_type == 'hourly':
                # 找到下一个整点中的指定分钟
                next_time = now.replace(minute=minute, second=0, microsecond=0)
                if next_time <= now:
                    next_time += timedelta(hours=1)
                return next_time
            elif repeat_type == 'daily':
                # 找到今天或明天的指定时间
                next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if next_time <= now:
                    next_time += timedelta(days=1)
                return next_time
            else:  # custom - 固定分钟发送
                # 找到下一个符合间隔的时间点
                base_minute = minute
                current_minute = now.hour * 60 + now.minute
                next_minute = ((current_minute // interval) + 1) * interval
                next_hour = next_minute // 60
                next_minute = next_minute % 60
                
                next_time = now.replace(hour=next_hour, minute=next_minute, second=0, microsecond=0)
                if next_time <= now:
                    next_time += timedelta(hours=1)
                return next_time
        
        # 如果没有找到有效的计划时间，使用基本逻辑
        if broadcast.get('last_broadcast'):
            # 基于上次发送时间和锚点模式计算
            if repeat_type == 'hourly':
                base_time = broadcast.get('last_broadcast')
                return base_time + timedelta(hours=1)
            elif repeat_type == 'daily':
                base_time = broadcast.get('last_broadcast')
                return base_time + timedelta(days=1)
            else:
                base_time = broadcast.get('last_broadcast')
                return base_time + timedelta(minutes=interval)
        else:
            # 首次发送，按开始时间发送
            return start_time
            
    async def process_broadcasts(self):
        """处理所有待发送的轮播消息"""
        try:
            logger.info("======= 开始处理轮播消息 =======")
            from db.models import GroupPermission
            
            # 获取所有应发送的轮播消息
            due_broadcasts = await self.db.get_due_broadcasts()
            logger.info(f"找到 {len(due_broadcasts)} 条待发送的轮播消息")
            
            # 增加详细日志
            for broadcast in due_broadcasts:
                broadcast_id = str(broadcast.get('_id', ''))
                logger.info(f"轮播消息详情 ID={broadcast_id}:")
                logger.info(f"  group_id={broadcast['group_id']}")
                logger.info(f"  start_time={broadcast.get('start_time')}")
                logger.info(f"  end_time={broadcast.get('end_time')}")
                logger.info(f"  repeat_type={broadcast.get('repeat_type')}")
                logger.info(f"  interval={broadcast.get('interval')}")
                logger.info(f"  use_fixed_time={broadcast.get('use_fixed_time')}")
                logger.info(f"  schedule_time={broadcast.get('schedule_time')}")
                logger.info(f"  last_broadcast={broadcast.get('last_broadcast')}")
            
            # 检查需要重试的消息
            now = datetime.now()
            retry_broadcasts = []
            for broadcast_id, retry_info in list(self.retry_tracker.items()):
                if now >= retry_info['next_retry']:
                    logger.info(f"轮播消息 {broadcast_id} 需要重试，第 {retry_info['attempt']} 次尝试")
                    retry_broadcasts.append(retry_info['broadcast'])
                else:
                    remaining = (retry_info['next_retry'] - now).total_seconds()
                    logger.info(f"轮播消息 {broadcast_id} 将在 {remaining:.1f} 秒后重试")
            
            # 合并待发送的消息（首次发送和需要重试的）
            all_broadcasts = due_broadcasts + retry_broadcasts
            logger.info(f"总共有 {len(all_broadcasts)} 条轮播消息需要处理（包括 {len(retry_broadcasts)} 条重试）")
            
            # 优化处理逻辑，使用asyncio.gather进行并行处理
            tasks = []
            broadcast_ids = []  # 用于收集广播ID
            
            for broadcast in all_broadcasts:
                # 跳过正在处理的轮播消息
                broadcast_id = str(broadcast.get('_id', ''))
                if broadcast_id in self.active_broadcasts:
                    logger.info(f"轮播消息 {broadcast_id} 正在处理中，跳过")
                    continue
                
                group_id = broadcast['group_id']
                
                # 检查群组权限
                has_permission = await self.bot.has_permission(group_id, GroupPermission.BROADCAST)
                logger.info(f"群组 {group_id} 轮播权限: {has_permission}")
                
                if not has_permission:
                    logger.warning(f"群组 {group_id} 没有轮播消息权限，跳过")
                    continue
                
                # 检查错误计数 - 仅针对非重试消息进行
                if broadcast_id in self.error_tracker and broadcast_id not in self.retry_tracker:
                    last_error_time = self.error_tracker[broadcast_id]['timestamp']
                    # 检查是否可以重试（经过RETRY_INTERVAL后）
                    retry_delta = (datetime.now() - last_error_time).total_seconds()
                    logger.info(f"轮播 {broadcast_id} 错误计数: {self.error_tracker[broadcast_id]['count']}, 距上次错误: {retry_delta}秒")
                    
                    if retry_delta < self.RETRY_INTERVAL:
                        logger.warning(f"轮播消息 {broadcast_id} 错误次数过多，暂停发送")
                        continue
                    else:
                        # 重置错误计数，给予重试机会
                        logger.info(f"重置轮播 {broadcast_id} 的错误计数")
                        self.error_tracker[broadcast_id]['count'] = 0
                
                # 添加到处理中列表
                logger.info(f"将轮播 {broadcast_id} 添加到活动处理列表")
                self.active_broadcasts.add(broadcast_id)
                
                # 创建发送任务
                logger.info(f"创建轮播 {broadcast_id} 的处理任务")
                task = asyncio.create_task(self._process_broadcast(broadcast))
                tasks.append(task)
                broadcast_ids.append(broadcast_id)  # 记录对应的广播ID
            
            # 等待所有任务完成，添加更好的日志记录
            if tasks:
                task_map = {id(task): broadcast_id for task, broadcast_id in zip(tasks, broadcast_ids)}
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        task_id = id(tasks[i])
                        broadcast_id = task_map.get(task_id, "未知")
                        logger.error(f"轮播任务 {broadcast_id} 出错: {result}")
                    else:
                        logger.info(f"轮播任务 {i} 成功完成")
            else:
                logger.info("没有需要处理的轮播任务")
            
            logger.info("======= 轮播消息处理完成 =======")
        except Exception as e:
            logger.error(f"处理轮播消息出错: {e}", exc_info=True)
            
    async def _process_broadcast(self, broadcast: Dict[str, Any]):
        """处理单个轮播消息"""
        broadcast_id = str(broadcast.get('_id', ''))
        
        try:
            # 检查是否是重试
            is_retry = broadcast_id in self.retry_tracker
            retry_attempt = self.retry_tracker.get(broadcast_id, {}).get('attempt', 0)
            
            logger.info(f"开始处理轮播消息 {broadcast_id}" + (f" (重试第{retry_attempt}次)" if is_retry else ""))
            
            # 使用锁避免并发发送同一条轮播消息
            async with self.sending_lock:
                # 如果是重试，跳过锚点检查直接发送
                should_send = True
                reason = f"重试第{retry_attempt}次" if is_retry else ""
                
            # 如果不是重试，检查是否应该发送
            if not is_retry:
                # 调用修改后的函数，可能会返回3个值
                result = await self._should_send_broadcast(broadcast)
                if len(result) == 3:
                    should_send, reason, current_anchor_id = result
                    # 保存锚点ID以便后续使用
                    broadcast['current_anchor_id'] = current_anchor_id
                else:
                    should_send, reason = result
                logger.info(f"轮播 {broadcast_id} 发送决策: {should_send}, 原因: {reason}")
            
            # 检查是否是强制发送标记
            is_forced_send = broadcast.get('force_sent', False)
            if is_forced_send:
                logger.info(f"检测到轮播消息 {broadcast_id} 有强制发送标记，不影响锚点判断")
            elif not should_send:  # 添加这个条件：如果不是强制发送且不应该发送，则直接返回
                logger.info(f"轮播消息 {broadcast_id} 不应发送: {reason}")
                self.active_broadcasts.discard(broadcast_id)
                return
            
            # 发送轮播消息
            logger.info(f"准备{'重试' if is_retry else ''}发送轮播消息: {broadcast_id}")
            success = await self.send_broadcast(broadcast)
            
            if success:
                # 如果是强制发送，只更新last_forced_send，不更新last_broadcast
                now = datetime.now()
                if is_forced_send:
                    await self.db.update_broadcast(broadcast_id, {
                        'last_forced_send': now,
                        'force_sent': False  # 重置强制发送标记
                    })
                    logger.info(f"已强制发送轮播消息 {broadcast_id}, 更新last_forced_send时间为 {now}，并重置force_sent标记")
                else:
                    # 正常发送，更新最后发送时间和锚点ID
                    update_data = {'last_broadcast': now}
                    
                    # 如果有锚点ID，也更新它
                    if 'current_anchor_id' in broadcast:
                        update_data['last_anchor_id'] = broadcast['current_anchor_id']
                        logger.info(f"更新锚点ID: {broadcast['current_anchor_id']}")
                        
                    await self.db.update_broadcast(broadcast_id, update_data)
                    logger.info(f"已发送轮播消息 {broadcast_id}, 更新最后发送时间为 {now}")
                
                # 清除错误记录和重试状态
                if broadcast_id in self.error_tracker:
                    del self.error_tracker[broadcast_id]
                if broadcast_id in self.retry_tracker:
                    del self.retry_tracker[broadcast_id]
            else:
                # 发送失败，处理重试逻辑
                if not is_retry:
                    # 首次失败，设置重试状态
                    self.retry_tracker[broadcast_id] = {
                        'attempt': 1,
                        'next_retry': datetime.now() + timedelta(seconds=self.RETRY_INTERVALS[0]),
                        'broadcast': broadcast
                    }
                    logger.info(f"轮播消息 {broadcast_id} 首次发送失败，将在 {self.RETRY_INTERVALS[0]} 秒后重试")
                else:
                    # 更新重试次数
                    retry_attempt = self.retry_tracker[broadcast_id]['attempt']
                    
                    if retry_attempt < len(self.RETRY_INTERVALS):
                        # 还有重试机会
                        next_interval = self.RETRY_INTERVALS[retry_attempt]
                        self.retry_tracker[broadcast_id].update({
                            'attempt': retry_attempt + 1,
                            'next_retry': datetime.now() + timedelta(seconds=next_interval)
                        })
                        logger.info(f"轮播消息 {broadcast_id} 第 {retry_attempt} 次重试失败，将在 {next_interval} 秒后再次重试")
                    else:
                        # 所有重试都失败了
                        logger.warning(f"轮播消息 {broadcast_id} 在 {retry_attempt} 次尝试后仍然失败")
                        
                        # 记录到错误跟踪器
                        if broadcast_id not in self.error_tracker:
                            self.error_tracker[broadcast_id] = {
                                'count': 0,
                                'last_error': "多次重试失败",
                                'timestamp': datetime.now()
                            }
                        
                        self.error_tracker[broadcast_id]['count'] += 1
                        self.error_tracker[broadcast_id]['timestamp'] = datetime.now()
                        
                        # 清除重试状态
                        del self.retry_tracker[broadcast_id]
                        
                        logger.warning(f"轮播消息 {broadcast_id} 发送失败，当前错误计数: {self.error_tracker[broadcast_id]['count']}")

        except Exception as e:
            logger.error(f"处理轮播消息 {broadcast_id} 出错: {e}", exc_info=True)
            # 更新数据库中的错误状态
            try:
                await self.db.update_broadcast(broadcast_id, {
                    'error': str(e),
                    'error_time': datetime.now()
                })
            except Exception as db_error:
                logger.error(f"更新轮播消息错误状态失败: {db_error}")
            
            # 记录错误
            if broadcast_id not in self.error_tracker:
                self.error_tracker[broadcast_id] = {
                    'count': 0,
                    'last_error': str(e),
                    'timestamp': datetime.now()
                }
            
            self.error_tracker[broadcast_id]['count'] += 1
            self.error_tracker[broadcast_id]['last_error'] = str(e)
            self.error_tracker[broadcast_id]['timestamp'] = datetime.now()
            
            # 清除重试状态（如果有异常，不再重试）
            if broadcast_id in self.retry_tracker:
                del self.retry_tracker[broadcast_id]
        finally:
            # 从处理中列表移除
            self.active_broadcasts.discard(broadcast_id)
            logger.info(f"轮播消息 {broadcast_id} 处理完成")
            
    async def _should_send_broadcast(self, broadcast: Dict[str, Any]) -> Tuple[bool, str]:
        """
        检查轮播消息是否应该发送
        
        参数:
            broadcast: 轮播消息数据
            
        返回:
            (是否应发送, 原因)
        """
        now = datetime.now()
        broadcast_id = str(broadcast.get('_id', ''))
        
        logger.info(f"===== 开始检查轮播消息 {broadcast_id} 是否应该发送 =====")
        logger.info(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 检查开始时间和结束时间
        start_time = broadcast.get('start_time')
        end_time = broadcast.get('end_time')
        
        logger.info(f"开始时间: {start_time}")
        logger.info(f"结束时间: {end_time}")
        
        if not start_time or now < start_time:
            logger.info(f"未到开始时间，不发送: 当前={now}, 开始={start_time}")
            return False, "未到开始时间"
        
        if end_time and now > end_time:
            logger.info(f"已过结束时间，不发送: 当前={now}, 结束={end_time}")
            return False, "已过结束时间"

        if broadcast.get('force_sent', False):
            logger.info(f"检测到强制发送标记，忽略锚点时间检查")
            return True, "强制发送"
        
        # 检查重复类型
        repeat_type = broadcast.get('repeat_type')
        last_broadcast = broadcast.get('last_broadcast')
        
        logger.info(f"重复类型: {repeat_type}")
        logger.info(f"上次发送时间: {last_broadcast}")
        
        # 单次发送
        if repeat_type == 'once':
            if last_broadcast:
                logger.info(f"单次发送已完成，不再发送: 上次发送时间={last_broadcast}")
                return False, "单次发送已完成"
            else:
                logger.info("单次发送，未发送过，可以发送")
                return True, "单次发送"
        
        # 获取间隔时间（分钟）
        if repeat_type == 'hourly':
            interval_minutes = 60
        elif repeat_type == 'daily':
            interval_minutes = 1440  # 24小时
        else:  # custom
            interval_minutes = broadcast.get('interval', 30)
        
        logger.info(f"发送间隔: {interval_minutes} 分钟")
        
        # 锚点模式发送逻辑
        schedule_time = broadcast.get('schedule_time')
        logger.info(f"调度时间设置: {schedule_time}")
        
        if not schedule_time:
            logger.info(f"缺少调度时间设置，无法发送")
            return False, "缺少调度时间设置"
        
        try:
            schedule_hour, schedule_minute = map(int, schedule_time.split(':'))
            logger.info(f"解析后的调度时间: {schedule_hour}:{schedule_minute}")
            
            if repeat_type == 'hourly':
                # 整点的指定分钟发送 - 只检查当前是否是指定分钟
                logger.info(f"当前分钟: {now.minute}, 调度分钟: {schedule_minute}")
                
                # 构建当前锚点标识
                current_anchor = f"{now.year}-{now.month}-{now.day}-{now.hour}:{schedule_minute}"
                
                if now.minute == schedule_minute:
                    # 检查此锚点是否已处理过
                    if broadcast_id in self.anchor_processed and current_anchor in self.anchor_processed[broadcast_id]:
                        last_process_time = self.anchor_processed[broadcast_id][current_anchor]
                        time_diff = (now - last_process_time).total_seconds()
                        logger.info(f"此锚点 {current_anchor} 已在 {time_diff:.1f} 秒前处理过，跳过")
                        return False, f"锚点 {current_anchor} 已处理"
                    
                    logger.info(f"分钟匹配")
                    logger.info(f"当前是整点 {schedule_minute} 分，可以发送")
                    
                    # 记录处理状态
                    if broadcast_id not in self.anchor_processed:
                        self.anchor_processed[broadcast_id] = {}
                    self.anchor_processed[broadcast_id][current_anchor] = now
                    
                    return True, f"整点 {schedule_minute} 分发送"
                
                logger.info(f"不是发送时间点 {schedule_minute} 分，不发送")
                return False, f"不是发送时间点 {schedule_minute} 分"
            
            elif repeat_type == 'daily':
                # 每天的指定时间发送 - 只检查当前是否是指定时间
                logger.info(f"当前时间: {now.hour}:{now.minute}, 调度时间: {schedule_hour}:{schedule_minute}")
                
                # 构建当前锚点标识
                current_anchor = f"{now.year}-{now.month}-{now.day}-{schedule_hour}:{schedule_minute}"
                
                if now.hour == schedule_hour and now.minute == schedule_minute:
                    # 检查此锚点是否已处理过
                    if broadcast_id in self.anchor_processed and current_anchor in self.anchor_processed[broadcast_id]:
                        last_process_time = self.anchor_processed[broadcast_id][current_anchor]
                        time_diff = (now - last_process_time).total_seconds()
                        logger.info(f"此锚点 {current_anchor} 已在 {time_diff:.1f} 秒前处理过，跳过")
                        return False, f"锚点 {current_anchor} 已处理"
                        
                    logger.info(f"小时和分钟都匹配")
                    logger.info(f"当前是每日 {schedule_hour}:{schedule_minute} 时间点，可以发送")
                    
                    # 记录处理状态
                    if broadcast_id not in self.anchor_processed:
                        self.anchor_processed[broadcast_id] = {}
                    self.anchor_processed[broadcast_id][current_anchor] = now
                    
                    return True, f"每日 {schedule_hour}:{schedule_minute} 发送"
                
                logger.info(f"不是发送时间点 {schedule_hour}:{schedule_minute}，不发送")
                return False, f"不是发送时间点 {schedule_hour}:{schedule_minute}"
            
            else:  # custom - 自定义间隔，锚点式发送
                # 计算当前时间在一天中的分钟数
                current_minutes = now.hour * 60 + now.minute
                    
                # 计算基准锚点（从当天0点开始计算的分钟数）
                base_anchor = schedule_hour * 60 + schedule_minute  # 基准锚点（比如19:00）
                
                # 简化锚点计算逻辑 - 直接找出最近的锚点时间
                minutes_since_base = (current_minutes - base_anchor) % (24 * 60)
                anchor_count = minutes_since_base // interval_minutes
                
                # 计算当前最接近的锚点时间（分钟数）
                current_anchor_minutes = (base_anchor + anchor_count * interval_minutes) % (24 * 60)
                anchor_hour = current_anchor_minutes // 60
                anchor_minute = current_anchor_minutes % 60
                
                # 计算当前时间与锚点时间的差距（分钟）
                minutes_diff = abs(current_minutes - current_anchor_minutes)
                if minutes_diff > interval_minutes / 2:
                    minutes_diff = interval_minutes - minutes_diff
                
                # 设置允许的误差范围（分钟）
                ANCHOR_TOLERANCE_MINUTES = 1  # 允许正负1分钟的误差
                # 判断是否在锚点附近
                is_anchor = minutes_diff <= ANCHOR_TOLERANCE_MINUTES
                
                logger.info(f"当前时间分钟数: {current_minutes}")
                logger.info(f"基准锚点分钟数: {base_anchor}")
                logger.info(f"计算的当前锚点时间: {anchor_hour:02d}:{anchor_minute:02d}")
                logger.info(f"与锚点时间差距: {minutes_diff}分钟")
                logger.info(f"是否是锚点: {is_anchor}")
                

                if is_anchor:
                    logger.info(f"检测到当前是锚点时间点")
                    
                    # 构建当前锚点的唯一标识（使用日期+实际锚点时间）
                    current_anchor_id = f"{now.strftime('%Y-%m-%d')}-{anchor_hour:02d}:{anchor_minute:02d}"
                    
                    # 添加额外的时间范围检查，避免日期变更导致的错误匹配
                    from datetime import time
                    actual_time = now.timestamp()
                    anchor_time = datetime.combine(now.date(), 
                                                  time(hour=anchor_hour, minute=anchor_minute)).timestamp()
                    
                    # 如果锚点时间大于当前时间，可能是昨天的锚点
                    if anchor_time > actual_time + 60*60:  # 加一小时的缓冲，避免时区问题
                        anchor_time = datetime.combine(now.date() - timedelta(days=1), 
                                                     time(hour=anchor_hour, minute=anchor_minute)).timestamp()
                    # 如果锚点时间小于当前时间太多，可能是明天的锚点
                    elif actual_time > anchor_time + 60*60*22:  # 如果差超过22小时，可能是明天的锚点
                        anchor_time = datetime.combine(now.date() + timedelta(days=1), 
                                                     time(hour=anchor_hour, minute=anchor_minute)).timestamp()
                    
                    # 检查差值是否在合理范围内（例如10分钟）
                    time_diff_minutes = abs(actual_time - anchor_time) / 60
                    if time_diff_minutes > 10:  # 设置一个合理的阈值，如10分钟
                        logger.info(f"锚点时间 {anchor_hour:02d}:{anchor_minute:02d} 与实际时间相差 {time_diff_minutes:.1f} 分钟，超出合理范围，跳过")
                        return False, f"时间差异过大（{time_diff_minutes:.1f}分钟）"
                    
                    # 增强锚点检查逻辑
                    # 1. 检查是否已在内存中记录处理过这个锚点
                    if broadcast_id in self.anchor_processed and current_anchor_id in self.anchor_processed[broadcast_id]:
                        last_process_time = self.anchor_processed[broadcast_id][current_anchor_id]
                        time_diff = (now - last_process_time).total_seconds()
                        logger.info(f"此锚点 {current_anchor_id} 已在 {time_diff:.1f} 秒前处理过，跳过")
                        return False, f"锚点 {current_anchor_id} 已处理"
                        
                    # 2. 检查数据库中记录的上次发送时间和锚点
                    if last_broadcast:
                        # 计算上次发送到现在的时间差(分钟)
                        time_diff_minutes = (now - last_broadcast).total_seconds() / 60
                        
                        # 如果距离上次发送时间小于间隔的20%，则跳过
                        if time_diff_minutes < (interval_minutes * 0.2):
                            logger.info(f"距上次发送仅 {time_diff_minutes:.1f} 分钟，小于间隔的20% ({interval_minutes * 0.2:.1f} 分钟)，跳过")
                            return False, f"发送间隔过短"
                            
                        # 检查上次发送的锚点ID是否与当前一致
                        last_anchor_id = broadcast.get('last_anchor_id')
                        if last_anchor_id == current_anchor_id:
                            logger.info(f"此锚点 {current_anchor_id} 已经处理过，跳过")
                            return False, f"锚点 {current_anchor_id} 已处理"
                    
                    logger.info(f"当前是锚点时间 {anchor_hour:02d}:{anchor_minute:02d}，可以发送")
                    
                    # 记录处理状态到内存
                    if broadcast_id not in self.anchor_processed:
                        self.anchor_processed[broadcast_id] = {}
                    self.anchor_processed[broadcast_id][current_anchor_id] = now
                    
                    return True, f"锚点时间 {anchor_hour:02d}:{anchor_minute:02d} 发送", current_anchor_id
                                    
                # 找到下一个锚点时间，用于日志
                next_anchor_minutes = current_anchor_minutes + interval_minutes
                next_anchor_hour = (next_anchor_minutes // 60) % 24
                next_anchor_minute = next_anchor_minutes % 60
                
                logger.info(f"不是锚点时间，下一个锚点: {next_anchor_hour:02d}:{next_anchor_minute:02d}，不发送")
                return False, f"不是锚点时间，下一个锚点: {next_anchor_hour:02d}:{next_anchor_minute:02d}"
                
        except Exception as e:
            logger.error(f"解析调度时间出错: {e}, broadcast_id={broadcast_id}", exc_info=True)
            return False, f"调度时间错误: {e}"
    
    async def _cleanup_cache(self):
        """定期清理过期缓存和锚点记录"""
        while self.running:
            try:
                # 每10分钟执行一次清理
                await asyncio.sleep(600)
                
                now = datetime.now()
                if (now - self.last_cache_cleanup).total_seconds() > 600:
                    # 清理过期的缓存
                    to_remove = []
                    for broadcast_id, data in self.broadcast_cache.items():
                        if 'timestamp' in data and (now - data['timestamp']).total_seconds() > self.CACHE_TTL:
                            to_remove.append(broadcast_id)
                    
                    # 删除过期的缓存
                    for broadcast_id in to_remove:
                        if broadcast_id in self.broadcast_cache:
                            del self.broadcast_cache[broadcast_id]
                    
                    # 清理过期的锚点记录
                    anchors_removed = 0
                    for broadcast_id in list(self.anchor_processed.keys()):
                        # 找出过期的锚点记录
                        expired_anchors = []
                        for anchor, timestamp in self.anchor_processed[broadcast_id].items():
                            if (now - timestamp).total_seconds() > self.ANCHOR_RECORD_TTL:
                                expired_anchors.append(anchor)
                        
                        # 删除过期记录
                        for anchor in expired_anchors:
                            del self.anchor_processed[broadcast_id][anchor]
                            anchors_removed += 1
                        
                        # 如果没有记录了，删除整个broadcast_id键
                        if not self.anchor_processed[broadcast_id]:
                            del self.anchor_processed[broadcast_id]
                    
                    self.last_cache_cleanup = now
                    logger.debug(f"清理完成: 移除 {len(to_remove)} 项缓存, {anchors_removed} 项锚点记录")
            except asyncio.CancelledError:
                logger.info("清理任务被取消")
                break
            except Exception as e:
                logger.error(f"清理任务出错: {e}", exc_info=True)
                await asyncio.sleep(300)  # 出错后5分钟再试

