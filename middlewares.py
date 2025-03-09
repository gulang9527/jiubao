import re
import logging
import asyncio
from functools import wraps
from typing import Callable, Optional

from telegram import Update
from telegram.ext import CallbackContext

logger = logging.getLogger(__name__)

#######################################
# 装饰器
#######################################

def error_handler(func: Callable) -> Callable:
    """
    统一的错误处理装饰器
    
    此装饰器会捕获函数执行过程中的所有异常，
    并通过机器人的错误处理器处理
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
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
    """
    要求管理员权限的装饰器
    
    此装饰器会验证用户是否具有管理员权限，
    如果没有权限则拒绝执行命令
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
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
    """
    要求超级管理员权限的装饰器
    
    此装饰器会验证用户是否具有超级管理员权限，
    如果没有权限则拒绝执行命令
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
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
    """
    要求群组有特定权限的装饰器
    
    此装饰器会验证群组是否具有特定功能权限，
    如果没有权限则拒绝执行命令
    
    参数:
        permission: 需要的权限类型
        
    返回:
        装饰器函数
    """
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
    """
    检查命令使用格式的装饰器
    
    此装饰器会验证命令的使用格式是否正确，
    如果格式不正确则提供帮助信息
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
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
    """
    回调错误处理装饰器
    
    此装饰器会捕获回调处理过程中的所有异常
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
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

#######################################
# 中间件类
#######################################

class MessageMiddleware:
    """
    消息处理中间件
    
    处理所有消息的通用逻辑，如安全检查、权限验证等
    """
    def __init__(self, bot):
        """
        初始化消息中间件
        
        参数:
            bot: 机器人实例
        """
        self.bot = bot
        
    async def __call__(self, update, context):
        """
        中间件主函数
        
        参数:
            update: 更新对象
            context: 回调上下文
        """
        if not update.effective_message:
            return
        try:
            # 基本安全检查
            if not await self._check_basic_security(update):
                return
                
            # 权限检查
            if not await self._check_permissions(update):
                return
                
            # 处理更新
            await context.application.process_update(update)
        except Exception as e:
            logger.error(f"中间件处理错误: {e}", exc_info=True)
            
    async def _check_basic_security(self, update: Update) -> bool:
        """
        检查基本安全限制
        
        参数:
            update: 更新对象
            
        返回:
            是否通过检查
        """
        message = update.effective_message
        
        # 检查消息内容长度
        if message.text and len(message.text) > 4096:
            await message.reply_text("❌ 消息内容过长")
            return False
            
        # 检查文件大小
        if message.document and message.document.file_size > 20 * 1024 * 1024:
            await message.reply_text("❌ 文件大小超过限制")
            return False
            
        return True
        
    async def _check_permissions(self, update: Update) -> bool:
        """
        检查权限
        
        参数:
            update: 更新对象
            
        返回:
            是否通过检查
        """
        if not update.effective_chat or not update.effective_user:
            return False
            
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # 检查用户是否被封禁
        if await self.bot.db.is_user_banned(user_id):
            logger.warning(f"封禁用户 {user_id} 尝试发送消息")
            return False
            
        # 检查群组是否授权
        if not await self.bot.db.get_group(chat_id):
            logger.warning(f"未授权群组 {chat_id}")
            return False
            
        return True
        
    async def _clean_message(self, update: Update) -> Optional[str]:
        """
        清理消息内容
        
        参数:
            update: 更新对象
            
        返回:
            清理后的消息文本
        """
        message = update.effective_message
        if not message.text:
            return None
            
        # 移除特殊字符
        cleaned_text = re.sub(r'[^\w\s\-.,?!@#$%^&*()]', '', message.text)
        return cleaned_text

class ErrorHandlingMiddleware:
    """
    错误处理中间件
    
    处理所有更新过程中的错误
    """
    def __init__(self, error_handler):
        """
        初始化错误处理中间件
        
        参数:
            error_handler: 错误处理器
        """
        self.error_handler = error_handler
        
    async def __call__(self, update, context):
        """
        错误处理中间件
        
        参数:
            update: 更新对象
            context: 回调上下文
        """
        try:
            return await context.application.process_update(update)
        except Exception as e:
            logger.error(f"更新处理出错: {e}", exc_info=True)
            await self.error_handler.handle_error(update, context)
            raise  # 重新抛出异常，以便上层处理
