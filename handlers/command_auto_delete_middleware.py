
"""
命令自动删除中间件，处理用户命令的自动删除
"""
import logging
import asyncio
from telegram import Update
from telegram.ext import CallbackContext

logger = logging.getLogger(__name__)

async def command_auto_delete_middleware(update: Update, context: CallbackContext):
    """
    处理用户命令的自动删除 - 统一处理群组和私聊
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    # 检查是否是有效消息
    if not update.effective_message or not update.effective_message.text:
        return
        
    message = update.effective_message
    
    # 检查是否是命令
    if not message.text.startswith('/'):
        return
        
    # 获取机器人实例
    bot_instance = context.application.bot_data.get('bot_instance')
    if not bot_instance:
        return
    
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 简单延迟5秒后删除命令
    try:
        # 延迟5秒
        await asyncio.sleep(5)
        # 尝试删除消息
        await message.delete()
        logger.info(f"已删除命令消息: {message.text} 在聊天 {chat_id}")
    except Exception as e:
        logger.error(f"删除命令消息失败: {e}")


"""
# 注册命令自动删除中间件
from handlers.command_auto_delete_middleware import command_auto_delete_middleware
application.add_handler(MessageHandler(filters.COMMAND, command_auto_delete_middleware), group=-1)
"""
