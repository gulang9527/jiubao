"""
轮播消息管理器，处理定时消息发送
"""
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)

class BroadcastManager:
    """
    轮播消息管理器，处理定时消息的发送
    """
    def __init__(self, db, bot_instance):
        """
        初始化轮播消息管理器
        
        参数:
            db: 数据库实例
            bot_instance: 机器人实例
        """
        self.db = db
        self.bot = bot_instance
        
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
            
            # 添加到数据库
            broadcast_id = await self.db.add_broadcast(broadcast_data)
            logger.info(f"已添加轮播消息: {broadcast_id}")
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
        required_fields = ['group_id', 'start_time', 'end_time', 'interval']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"缺少必要字段: {field}")
                
        # 确保有内容
        if not data.get('text') and not data.get('media') and not data.get('buttons', []):
            raise ValueError("轮播消息必须包含文本、媒体或按钮中的至少一项")
            
        # 验证时间
        if data['start_time'] >= data['end_time']:
            raise ValueError("结束时间必须晚于开始时间")
            
        # 验证间隔
        import config
        min_interval = config.BROADCAST_SETTINGS.get('min_interval', 5)  # 默认最小5分钟
        if data['interval'] < min_interval:
            raise ValueError(f"间隔不能小于 {min_interval} 分钟")
            
    async def remove_broadcast(self, group_id: int, broadcast_id: str) -> bool:
        """
        删除轮播消息
        
        参数:
            group_id: 群组ID
            broadcast_id: 轮播消息ID
            
        返回:
            是否成功
        """
        try:
            await self.db.remove_broadcast(group_id, broadcast_id)
            logger.info(f"已删除轮播消息: {broadcast_id}")
            return True
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
            return await self.db.get_broadcasts(group_id)
        except Exception as e:
            logger.error(f"获取轮播消息失败: {e}", exc_info=True)
            return []
            
    async def process_broadcasts(self):
        """处理所有待发送的轮播消息"""
        try:
            logger.debug("开始处理轮播消息")
            from db.models import GroupPermission
            
            # 获取所有应发送的轮播消息
            due_broadcasts = await self.db.get_due_broadcasts()
            logger.debug(f"找到 {len(due_broadcasts)} 条待发送的轮播消息")
            
            for broadcast in due_broadcasts:
                group_id = broadcast['group_id']
                
                # 检查群组权限
                if not await self.bot.has_permission(group_id, GroupPermission.BROADCAST):
                    logger.debug(f"群组 {group_id} 没有轮播消息权限，跳过")
                    continue
                
                try:
                    # 发送轮播消息
                    await self._send_broadcast(broadcast)
                    
                    # 更新最后发送时间
                    await self.db.update_broadcast_time(str(broadcast['_id']), datetime.now())
                except Exception as e:
                    logger.error(f"发送轮播消息失败: {e}, broadcast_id={broadcast['_id']}", exc_info=True)
                
        except Exception as e:
            logger.error(f"处理轮播消息出错: {e}", exc_info=True)
            
    async def _send_broadcast(self, broadcast: Dict[str, Any]):
        """
        发送单条轮播消息
        
        参数:
            broadcast: 轮播消息数据
        """
        group_id = broadcast['group_id']
        text = broadcast.get('text', '')
        media = broadcast.get('media')
        buttons = broadcast.get('buttons', [])
        
        # 准备按钮
        reply_markup = None
        if buttons:
            keyboard = []
            for button in buttons:
                keyboard.append([InlineKeyboardButton(button['text'], url=button['url'])])
            reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 根据内容类型发送消息
        msg = None
        if media and media.get('type'):
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
            settings = await self.bot.db.get_group_settings(group_id)
            if settings.get('auto_delete', False):
                from utils.message_utils import validate_delete_timeout
                timeout = validate_delete_timeout(message_type='broadcast')
                asyncio.create_task(self._schedule_delete(msg, timeout))
                
        logger.info(f"已发送轮播消息: group_id={group_id}, broadcast_id={broadcast['_id']}")
        
    async def _schedule_delete(self, message, timeout: int):
        """计划删除消息"""
        await asyncio.sleep(timeout)
        try:
            await message.delete()
        except Exception as e:
            logger.error(f"删除消息失败: {e}")
            
    async def is_broadcast_active(self, broadcast_id: str) -> bool:
        """
        检查轮播消息是否处于活动状态
        
        参数:
            broadcast_id: 轮播消息ID
            
        返回:
            是否活动
        """
        try:
            broadcast = await self.db.get_broadcast_by_id(broadcast_id)
            if not broadcast:
                return False
                
            now = datetime.now()
            return broadcast['start_time'] <= now <= broadcast['end_time']
        except Exception as e:
            logger.error(f"检查轮播消息状态失败: {e}", exc_info=True)
            return False
