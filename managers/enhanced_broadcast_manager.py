"""
增强版轮播消息管理器，处理定时消息发送
支持固定时间发送和间隔发送两种模式
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
        """
        初始化轮播消息管理器
        
        参数:
            db: 数据库实例
            bot_instance: 机器人实例
            apply_defaults: 是否应用默认设置
        """
        self.db = db
        self.bot = bot_instance
        self.active_broadcasts = set()  # 用于跟踪正在处理的轮播消息
        self.broadcast_cache = {}  # 缓存轮播消息数据
        self.last_cache_cleanup = datetime.now()
        self.sending_lock = asyncio.Lock()  # 避免并发发送同一条轮播消息
        self.CACHE_TTL = 300  # 缓存有效期（秒）
        self.error_tracker = {}  # 记录发送错误
        self.retry_tracker = {}  # 新增：跟踪重试状态
        self.MAX_ERROR_COUNT = 6  # 最大错误次数
        self.RETRY_ATTEMPTS = 3   # 新增：最大重试次数
        self.RETRY_INTERVALS = [60, 180]  # 新增：重试间隔（秒），第一次1分钟，第二次3分钟
        
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
            
            # 强制所有轮播消息使用固定时间锚点
            broadcast_data['use_fixed_time'] = True
            
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
            # 强制所有轮播消息使用固定时间锚点
            broadcast_data['use_fixed_time'] = True
            
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
            
            # 检查是否使用固定时间
            use_fixed_time = broadcast.get('use_fixed_time', False)
            if not use_fixed_time:
                # 设置为使用固定时间
                await self.update_broadcast(broadcast_id, {'use_fixed_time': True})
                logger.info(f"已将轮播消息 {broadcast_id} 设置为使用固定时间发送")
            
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
                    if broadcast.get('use_fixed_time', False):
                        if repeat_type == 'hourly':
                            return "每小时固定时间发送"
                        elif repeat_type == 'daily':
                            return "每天固定时间发送"
                        else:
                            interval = broadcast.get('interval', 0)
                            return f"每{interval}分钟固定发送"
                    else:
                        if repeat_type == 'hourly':
                            return "每小时发送"
                        elif repeat_type == 'daily':
                            return "每天发送"
                        else:
                            interval = broadcast.get('interval', 0)
                            return f"每{interval}分钟发送"
    
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
        last_broadcast = broadcast.get('last_broadcast')
        repeat_type = broadcast.get('repeat_type')
        interval = broadcast.get('interval', 0)
        use_fixed_time = broadcast.get('use_fixed_time', False)
        
        # 检查消息是否在有效期内
        if not start_time or now < start_time:
            return start_time
        elif not end_time or now > end_time:
            return None
        
        # 单次发送
        if repeat_type == 'once':
            if last_broadcast:
                return None  # 已发送，不再发送
            else:
                return start_time  # 未发送，按开始时间发送
        
        # 固定时间发送
        if use_fixed_time:
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
        
        # 常规间隔发送
        if last_broadcast:
            # 根据上次发送时间和间隔计算下次发送时间
            if repeat_type == 'hourly':
                next_time = last_broadcast + timedelta(minutes=60)
            elif repeat_type == 'daily':
                next_time = last_broadcast + timedelta(minutes=1440)
            else:  # custom
                next_time = last_broadcast + timedelta(minutes=interval)
            return next_time
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
            
            # 等待所有任务完成，添加更好的日志记录
            if tasks:
                logger.info(f"开始执行 {len(tasks)} 个轮播处理任务")
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"轮播任务 {i} 出错: {result}")
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
                    should_send, reason = await self._should_send_broadcast(broadcast)
                    logger.info(f"轮播 {broadcast_id} 发送决策: {should_send}, 原因: {reason}")

                # 检查是否是强制发送标记
                if broadcast.get('force_sent'):
                    logger.info(f"检测到轮播消息 {broadcast_id} 有强制发送标记，不影响锚点判断")
                    
                    if not should_send:
                        logger.info(f"轮播消息 {broadcast_id} 不应发送: {reason}")
                        self.active_broadcasts.discard(broadcast_id)
                        return
                
                # 发送轮播消息
                logger.info(f"准备{'重试' if is_retry else ''}发送轮播消息: {broadcast_id}")
                success = await self.send_broadcast(broadcast)
                
                if success:
                    # 成功发送，更新最后发送时间
                    now = datetime.now()
                    await self.db.update_broadcast_time(broadcast_id, now)
                    logger.info(f"已发送轮播消息 {broadcast_id}, 更新最后发送时间为 {now}")
                    
                    # 清除错误记录和重试状态
                    if broadcast_id in self.error_tracker:
                        del self.error_tracker[broadcast_id]
                    if broadcast_id in self.retry_tracker:
                        del self.retry_tracker[broadcast_id]

                    # 清除强制发送标记
                    await self.db.update_broadcast(broadcast_id, {'force_sent': False})
                    logger.info(f"已清除轮播消息 {broadcast_id} 的强制发送标记")
    
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
        
        # 检查是否使用固定时间发送
        use_fixed_time = broadcast.get('use_fixed_time', False)
        logger.info(f"是否使用固定时间发送: {use_fixed_time}")
        
        # 固定时间发送
        if use_fixed_time:
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
                    if now.minute == schedule_minute:
                        logger.info(f"分钟匹配")
                        # 只需要简单防止在同一分钟多次发送的极端情况
                        if last_broadcast and (now - last_broadcast).total_seconds() < 55:
                            logger.info(f"已在当前分钟 {now.minute} 发送过，距上次发送仅 {(now - last_broadcast).total_seconds():.1f} 秒")
                            return False, f"已在当前分钟 {now.minute} 发送过"
                        logger.info(f"当前是整点 {schedule_minute} 分，可以发送")
                        return True, f"整点 {schedule_minute} 分发送"
                    logger.info(f"不是发送时间点 {schedule_minute} 分，不发送")
                    return False, f"不是发送时间点 {schedule_minute} 分"
                
                elif repeat_type == 'daily':
                    # 每天的指定时间发送 - 只检查当前是否是指定时间
                    logger.info(f"当前时间: {now.hour}:{now.minute}, 调度时间: {schedule_hour}:{schedule_minute}")
                    if now.hour == schedule_hour and now.minute == schedule_minute:
                        logger.info(f"小时和分钟都匹配")
                        # 只需要简单防止在同一分钟多次发送的极端情况
                        if last_broadcast and (now - last_broadcast).total_seconds() < 55:
                            logger.info(f"已在当前分钟发送过，距上次发送仅 {(now - last_broadcast).total_seconds():.1f} 秒")
                            return False, "已在当前分钟发送过"
                        logger.info(f"当前是每日 {schedule_hour}:{schedule_minute} 时间点，可以发送")
                        return True, f"每日 {schedule_hour}:{schedule_minute} 发送"
                    logger.info(f"不是发送时间点 {schedule_hour}:{schedule_minute}，不发送")
                    return False, f"不是发送时间点 {schedule_hour}:{schedule_minute}"
                
                else:  # custom - 自定义间隔，锚点式发送
                    # 计算当前时间在一天中的分钟数
                    current_minutes = now.hour * 60 + now.minute
                        
                    # 计算基准锚点（从当天0点开始计算的分钟数）
                    base_anchor = schedule_hour * 60 + schedule_minute  # 基准锚点（比如19:00）
                    
                    # 计算当前时间与基准锚点的偏移量，正确处理24小时周期
                    # 当current_minutes < base_anchor时，我们需要添加一天的分钟数
                    daily_minutes = 24 * 60
                    adjusted_current = current_minutes
                    if current_minutes < base_anchor:
                        adjusted_current = current_minutes + daily_minutes
                    
                    # 计算偏移量
                    offset = ((current_minutes - base_anchor) % (24 * 60)) % interval_minutes
                    
                    # 设置允许的误差范围（分钟）
                    ANCHOR_TOLERANCE_MINUTES = 1  # 允许正负1分钟的误差
                    # 判断是否在锚点附近
                    is_anchor = offset == 0 or offset <= ANCHOR_TOLERANCE_MINUTES or offset >= (interval_minutes - ANCHOR_TOLERANCE_MINUTES)
                    
                    logger.info(f"当前时间分钟数: {current_minutes}")
                    logger.info(f"基准锚点分钟数: {base_anchor}")
                    logger.info(f"偏移量: {offset}")
                    logger.info(f"是否是锚点: {is_anchor}")
                    
                    if is_anchor:
                        logger.info(f"检测到当前是锚点时间点")
                        # 只需防止同一分钟内多次发送
                        if last_broadcast and (now - last_broadcast).total_seconds() < 55:
                            logger.info(f"已在当前分钟发送过，距上次发送仅 {(now - last_broadcast).total_seconds():.1f} 秒")
                            return False, "已在当前分钟发送过"
                        
                        # 计算当前是哪个锚点
                        anchor_number = (current_minutes - base_anchor) // interval_minutes
                        if anchor_number < 0:
                            anchor_number += (24 * 60) // interval_minutes
                            
                        anchor_hour = (base_anchor + anchor_number * interval_minutes) // 60 % 24
                        anchor_minute = (base_anchor + anchor_number * interval_minutes) % 60
                        
                        logger.info(f"当前是锚点时间 {anchor_hour:02d}:{anchor_minute:02d}，可以发送")
                        return True, f"锚点时间 {anchor_hour:02d}:{anchor_minute:02d} 发送"
                       
                    # 找到下一个锚点时间，用于日志
                    next_anchor_minutes = current_minutes + (interval_minutes - offset) % interval_minutes
                    next_anchor_hour = (next_anchor_minutes // 60) % 24
                    next_anchor_minute = next_anchor_minutes % 60
                    
                    logger.info(f"不是锚点时间，下一个锚点: {next_anchor_hour:02d}:{next_anchor_minute:02d}，不发送")
                    return False, f"不是锚点时间，下一个锚点: {next_anchor_hour:02d}:{next_anchor_minute:02d}"
                    
            except Exception as e:
                logger.error(f"解析调度时间出错: {e}, broadcast_id={broadcast_id}", exc_info=True)
                return False, f"调度时间错误: {e}"
        
        # 常规间隔发送
        else:
            # 如果没有上次发送记录，现在就可以发送
            if not last_broadcast:
                logger.info(f"首次发送，可以发送")
                return True, "首次发送"
            
            # 计算是否达到发送间隔
            elapsed_minutes = (now - last_broadcast).total_seconds() / 60
            logger.info(f"距离上次发送已经过去: {elapsed_minutes:.1f} 分钟, 间隔设置: {interval_minutes} 分钟")
            
            if elapsed_minutes >= interval_minutes:
                logger.info(f"达到发送间隔，可以发送")
                return True, f"达到发送间隔 ({elapsed_minutes:.1f} >= {interval_minutes})"
            
            logger.info(f"发送间隔未到，不发送")
            return False, f"发送间隔未到 ({elapsed_minutes:.1f} < {interval_minutes})"
