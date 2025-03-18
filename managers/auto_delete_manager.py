"""
自动删除管理器，负责管理消息的自动删除
"""
import asyncio
import logging
from typing import Dict, Any, Optional
from telegram import Message

from db.models import GroupPermission
from utils.message_utils import validate_delete_timeout, is_auto_delete_exempt

logger = logging.getLogger(__name__)

class AutoDeleteManager:
    """
    自动删除管理器
    负责处理消息的自动删除功能
    """
    
    def __init__(self, db, apply_defaults=True):
        """
        初始化自动删除管理器
        
        参数:
            db: 数据库实例
            apply_defaults: 是否应用默认设置
        """
        self.db = db
        self.delete_tasks = {}  # 存储删除任务 {message_id: task}
        
        # 只在首次初始化时应用默认设置
        if apply_defaults:
            asyncio.create_task(self._apply_default_settings())
        
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
                        'command': settings.get('auto_delete_timeout', 300)
                    }
                
                await self.db.update_group_settings(group_id, settings)
                logger.info(f"已更新群组 {group_id} 的自动删除设置")
        except Exception as e:
            logger.error(f"应用默认自动删除设置失败: {e}", exc_info=True)
        
    async def schedule_delete(self, message: Message, message_type: str = 'default', group_id: Optional[int] = None, custom_timeout: Optional[int] = None):
        """
        计划删除消息
        
        参数:
            message: 要删除的消息
            message_type: 消息类型 (broadcast, keyword, ranking, default)
            group_id: 群组ID，如果为None则从message获取
            custom_timeout: 自定义超时时间，覆盖其他设置
        """
        if not message:
            logger.warning("无法计划删除None消息")
            return
            
        # 获取群组ID
        if group_id is None and message.chat:
            group_id = message.chat.id
            
        if not group_id:
            logger.warning("无法获取群组ID，取消自动删除")
            return
            
        try:
            # 获取群组设置
            settings = await self.db.get_group_settings(group_id)
            
            # 检查是否启用自动删除
            if not settings.get('auto_delete', False):
                logger.debug(f"群组 {group_id} 未启用自动删除")
                return
            
            # 检查用户是否豁免自动删除（如管理员）
            if message.from_user:
                user = await self.db.get_user(message.from_user.id)
                if user and is_auto_delete_exempt(user.get('role', ''), message.text):
                    logger.debug(f"用户 {message.from_user.id} 免除自动删除")
                    return
                
            # 获取超时时间
            if custom_timeout is not None:
                timeout = custom_timeout
            else:
                # 从设置中获取对应消息类型的超时时间
                settings = await self.db.get_group_settings(group_id)
                timeouts = settings.get('auto_delete_timeouts', {})
                timeout = timeouts.get(message_type, settings.get('auto_delete_timeout', 300))
            
            # 创建删除任务
            task = asyncio.create_task(self._delete_after(message, timeout))
            
            # 保存任务引用
            message_id = f"{group_id}_{message.message_id}"  # 确保唯一性
            self.delete_tasks[message_id] = task
            
            logger.info(f"已计划删除消息 {message.message_id}，类型: {message_type}，超时: {timeout}秒")
            
        except Exception as e:
            logger.error(f"计划删除消息时出错: {e}", exc_info=True)
    
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
    
    async def _delete_after(self, message: Message, timeout: int):
        """
        延迟删除消息
        
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
                
    async def handle_command_response(self, message: Message, group_id: int):
        """
        处理命令响应消息的自动删除
        
        参数:
            message: 命令响应消息
            group_id: 群组ID
        """
        await self.schedule_delete(message, 'default', group_id)
        
    async def handle_keyword_response(self, message: Message, group_id: int):
        """
        处理关键词响应消息的自动删除
        
        参数:
            message: 关键词响应消息
            group_id: 群组ID
        """
        await self.schedule_delete(message, 'keyword', group_id)
        
    async def handle_broadcast_message(self, message: Message, group_id: int):
        """
        处理轮播消息的自动删除
        
        参数:
            message: 轮播消息
            group_id: 群组ID
        """
        await self.schedule_delete(message, 'broadcast', group_id)
        
    async def handle_ranking_message(self, message: Message, group_id: int):
        """
        处理排行榜消息的自动删除
        
        参数:
            message: 排行榜消息
            group_id: 群组ID
        """
        await self.schedule_delete(message, 'ranking', group_id)
        
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
            await self.schedule_delete(message, 'default', group_id)

    async def shutdown(self):
        """
        关闭管理器，取消所有等待中的删除任务
        """
        logger.info("正在关闭自动删除管理器...")
        for message_id, task in list(self.delete_tasks.items()):
            try:
                logger.info(f"取消删除任务: {message_id}")
                task.cancel()
            except Exception as e:
                logger.error(f"取消删除任务 {message_id} 时出错: {e}")
        
        self.delete_tasks.clear()
        logger.info("自动删除管理器已关闭")
