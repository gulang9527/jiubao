# 这是一个可选的功能，用于在用户发送命令时自动删除命令
# 可以在 __init__.py 中添加一个中间件来处理所有命令

# 1. 创建一个新的中间件函数，命名为 command_auto_delete_middleware.py
"""
命令自动删除中间件，处理用户命令的自动删除
"""
import logging
from telegram import Update
from telegram.ext import CallbackContext

from db.models import GroupPermission

logger = logging.getLogger(__name__)

async def command_auto_delete_middleware(update: Update, context: CallbackContext):
    """
    处理用户命令的自动删除
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    # 检查是否是命令
    if not update.effective_message or not update.effective_chat or not update.effective_message.text:
        return
        
    message = update.effective_message
    
    # 检查是否是命令
    if not message.text.startswith('/'):
        return
        
    # 不处理私聊命令
    if update.effective_chat.type == 'private':
        return
        
    # 获取机器人实例
    bot_instance = context.application.bot_data.get('bot_instance')
    if not bot_instance or not bot_instance.auto_delete_manager:
        return
        
    # 获取群组ID
    group_id = update.effective_chat.id
    
    # 检查群组设置
    settings = await bot_instance.db.get_group_settings(group_id)
    if not settings.get('auto_delete', False):
        return
        
    # 处理命令自动删除
    await bot_instance.auto_delete_manager.handle_user_command(message)
    logger.debug(f"已设置命令 {message.text} 的自动删除")

# 2. 在 __init__.py 的 register_all_handlers 函数中添加此中间件
# 在已有的处理器注册后添加:
"""
# 注册命令自动删除中间件
from handlers.command_auto_delete_middleware import command_auto_delete_middleware
application.add_handler(MessageHandler(filters.COMMAND, command_auto_delete_middleware), group=-1)
"""
