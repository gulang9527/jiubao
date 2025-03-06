import re
import logging
import asyncio
from functools import wraps
from typing import Callable, Optional

from telegram import Update
from telegram.ext import CallbackContext

logger = logging.getLogger(__name__)

def error_handler(func: Callable) -> Callable:
    """统一的错误处理装饰器"""
    @wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            
            # 获取机器人实例
            bot_instance = context.application.bot_data.get('bot_instance')
            
            # 处理错误
            if bot_instance and hasattr(bot_instance, 'error_handler'):
                await bot_instance.error_handler.handle_error(update, context)
            
            # 显示友好的错误消息
            if update and update.effective_message:
                try:
                    await update.effective_message.reply_text(
                        "❌ 操作过程中出现错误，请稍后重试或联系管理员。"
                    )
                except Exception as msg_error:
                    logger.error(f"发送错误消息失败: {msg_error}")
    return wrapper

def require_admin(func: Callable) -> Callable:
    """要求管理员权限的装饰器"""
    @wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_user:
            return
        
        bot_instance = context.application.bot_data.get('bot_instance')
        if not bot_instance:
            return
            
        user_id = update.effective_user.id
        if not await bot_instance.is_admin(user_id):
            await update.message.reply_text("❌ 该命令仅管理员可用")
            return
            
        return await func(update, context, *args, **kwargs)
    return wrapper

def require_superadmin(func: Callable) -> Callable:
    """要求超级管理员权限的装饰器"""
    @wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_user:
            return
        
        bot_instance = context.application.bot_data.get('bot_instance')
        if not bot_instance:
            return
            
        user_id = update.effective_user.id
        if not await bot_instance.is_superadmin(user_id):
            await update.message.reply_text("❌ 该命令仅超级管理员可用")
            return
            
        return await func(update, context, *args, **kwargs)
    return wrapper

def require_group_permission(permission):
    """要求群组有特定权限的装饰器"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
            if not update.effective_chat:
                return
            
            bot_instance = context.application.bot_data.get('bot_instance')
            if not bot_instance:
                return
                
            group_id = update.effective_chat.id
            if not await bot_instance.has_permission(group_id, permission):
                await update.message.reply_text(f"❌ 此群组未启用{permission.value}功能")
                return
                
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

def check_command_usage(func: Callable) -> Callable:
    """检查命令使用格式的装饰器"""
    @wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_message:
            return
        
        from utils import CommandHelper
        
        bot_instance = context.application.bot_data.get('bot_instance')
        if not bot_instance:
            return
            
        message = update.effective_message
        command = message.text.split()[0].lstrip('/').split('@')[0]
        
        # 获取命令使用说明
        usage = CommandHelper.get_usage(command)
        if not usage:
            return await func(update, context, *args, **kwargs)
            
        # 检查权限
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            return
            
        if usage['admin_only'] and not await bot_instance.is_admin(user_id):
            await update.message.reply_text("❌ 该命令仅管理员可用")
            return
            
        # 检查参数
        if '<' in usage['usage'] and not context.args:
            await update.message.reply_text(f"❌ 命令使用方法不正确\n{CommandHelper.format_usage(command)}")
            return
            
        return await func(update, context, *args, **kwargs)
    return wrapper

def handle_callback_errors(func: Callable) -> Callable:
    """回调错误处理装饰器"""
    @wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"回调处理错误 {func.__name__}: {e}", exc_info=True)
            logger.error(f"回调数据: {update.callback_query.data if update.callback_query else 'None'}")
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text("❌ 操作出错，请重试")
                except Exception as answer_error:
                    logger.error(f"无法回应回调查询: {answer_error}")
    return wrapper

class MessageMiddleware:
    def __init__(self, bot):
        self.bot = bot
        
    async def __call__(self, update, context):
        """中间件主函数"""
        if not update.effective_message:
            return
        try:
            if not await self._check_basic_security(update):
                return
            if not await self._check_permissions(update):
                return
            await context.application.process_update(update)
        except Exception as e:
            logger.error(f"中间件处理错误: {e}")
            
    async def _check_basic_security(self, update: Update) -> bool:
        """检查基本安全限制"""
        message = update.effective_message
        if message.text and len(message.text) > 4096:
            await message.reply_text("❌ 消息内容过长")
            return False
        if message.document and message.document.file_size > 20 * 1024 * 1024:
            await message.reply_text("❌ 文件大小超过限制")
            return False
        return True
        
    async def _check_permissions(self, update: Update) -> bool:
        """检查权限"""
        if not update.effective_chat or not update.effective_user:
            return False
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        if await self.bot.db.is_user_banned(user_id):
            return False
        if not await self.bot.db.get_group(chat_id):
            return False
        return True
        
    async def _clean_message(self, update: Update) -> Optional[str]:
        """清理消息内容"""
        message = update.effective_message
        if not message.text:
            return None
        cleaned_text = re.sub(r'[^\w\s\-.,?!@#$%^&*()]', '', message.text)
        return cleaned_text

class ErrorHandlingMiddleware:
    def __init__(self, error_handler):
        self.error_handler = error_handler
        
    async def __call__(self, update, context):
        """错误处理中间件"""
        try:
            return await context.application.process_update(update)
        except Exception as e:
            await self.error_handler.handle_error(update, context)
            raise
