"""
装饰器工具，提供权限检查和错误处理装饰器
"""
import logging
import functools
from typing import Callable, Any, Optional

from telegram import Update
from telegram.ext import CallbackContext

logger = logging.getLogger(__name__)

def error_handler(func: Callable) -> Callable:
    """
    错误处理装饰器
    
    捕获函数执行过程中的所有异常并通过错误处理器处理
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            
            # 获取机器人实例
            bot_instance = context.application.bot_data.get('bot_instance')
            
            # 通过错误处理器处理
            if bot_instance and hasattr(bot_instance, 'error_handler'):
                await bot_instance.error_handler.handle_error(update, context)
            else:
                # 直接显示友好的错误消息
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
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_user:
            return
        
        bot_instance = context.application.bot_data.get('bot_instance')
        if not bot_instance:
            logger.error("无法获取机器人实例")
            await update.effective_message.reply_text("❌ 系统错误，请重试")
            return
            
        user_id = update.effective_user.id
        if not await bot_instance.is_admin(user_id):
            await update.effective_message.reply_text("❌ 该命令仅管理员可用")
            return
            
        return await func(update, context, *args, **kwargs)
    
    return wrapper

def require_superadmin(func: Callable) -> Callable:
    """
    要求超级管理员权限的装饰器
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_user:
            return
        
        bot_instance = context.application.bot_data.get('bot_instance')
        if not bot_instance:
            logger.error("无法获取机器人实例")
            await update.effective_message.reply_text("❌ 系统错误，请重试")
            return
            
        user_id = update.effective_user.id
        if not await bot_instance.is_superadmin(user_id):
            await update.effective_message.reply_text("❌ 该命令仅超级管理员可用")
            return
            
        return await func(update, context, *args, **kwargs)
    
    return wrapper

def require_group_permission(permission_type):
    """
    要求群组有特定权限的装饰器
    
    参数:
        permission_type: 权限类型
        
    返回:
        装饰器函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
            if not update.effective_chat:
                return
            
            bot_instance = context.application.bot_data.get('bot_instance')
            if not bot_instance:
                logger.error("无法获取机器人实例")
                await update.effective_message.reply_text("❌ 系统错误，请重试")
                return
                
            group_id = update.effective_chat.id
            if not await bot_instance.has_permission(group_id, permission_type):
                await update.effective_message.reply_text(f"❌ 此群组未启用{permission_type.value}功能")
                return
                
            return await func(update, context, *args, **kwargs)
        
        return wrapper
    
    return decorator

def check_command_usage(func: Callable) -> Callable:
    """
    检查命令使用格式的装饰器
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_message:
            return
        
        from utils.command_helper import CommandHelper
        
        bot_instance = context.application.bot_data.get('bot_instance')
        if not bot_instance:
            logger.error("无法获取机器人实例")
            await update.effective_message.reply_text("❌ 系统错误，请重试")
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
            await message.reply_text("❌ 该命令仅管理员可用")
            return
            
        # 检查参数
        if '<' in usage['usage'] and not context.args:
            await message.reply_text(f"❌ 命令使用方法不正确\n{CommandHelper.format_usage(command)}")
            return
            
        return await func(update, context, *args, **kwargs)
    
    return wrapper

def handle_callback_errors(func: Callable) -> Callable:
    """
    处理回调错误的装饰器
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in callback {func.__name__}: {e}", exc_info=True)
            
            # 处理回调查询错误
            if update.callback_query:
                try:
                    await update.callback_query.answer("处理回调时出错，请重试")
                    
                    # 尝试编辑消息
                    try:
                        await update.callback_query.edit_message_text("❌ 操作出错，请重试")
                    except Exception:
                        # 如果无法编辑原消息，发送新消息
                        if update.effective_chat:
                            await context.bot.send_message(
                                chat_id=update.effective_chat.id,
                                text="❌ 操作出错，请重试"
                            )
                except Exception as answer_error:
                    logger.error(f"处理回调错误时出错: {answer_error}")
    
    return wrapper

def rate_limit(limit_per_user: int, time_window: int):
    """
    速率限制装饰器
    
    参数:
        limit_per_user: 每个用户在时间窗口内的最大操作次数
        time_window: 时间窗口大小（秒）
        
    返回:
        装饰器函数
    """
    # 用户操作计数器
    user_counters = {}
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
            if not update.effective_user:
                return await func(update, context, *args, **kwargs)
                
            user_id = update.effective_user.id
            current_time = int(context.bot.request.event_loop.time())
            
            # 初始化计数器
            if user_id not in user_counters:
                user_counters[user_id] = {"count": 0, "reset_time": current_time + time_window}
            
            # 检查是否需要重置计数器
            if current_time >= user_counters[user_id]["reset_time"]:
                user_counters[user_id] = {"count": 0, "reset_time": current_time + time_window}
            
            # 检查是否超过限制
            if user_counters[user_id]["count"] >= limit_per_user:
                remaining_time = user_counters[user_id]["reset_time"] - current_time
                await update.effective_message.reply_text(
                    f"❌ 操作过于频繁，请等待 {remaining_time} 秒后再试"
                )
                return
                
            # 更新计数器
            user_counters[user_id]["count"] += 1
            
            # 执行原函数
            return await func(update, context, *args, **kwargs)
            
        return wrapper
    
    return decorator

def require_private_chat(func: Callable) -> Callable:
    """
    要求私聊的装饰器
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_chat or update.effective_chat.type != 'private':
            await update.effective_message.reply_text("❌ 该命令仅在私聊中可用")
            return
            
        return await func(update, context, *args, **kwargs)
    
    return wrapper

def require_group_chat(func: Callable) -> Callable:
    """
    要求群聊的装饰器
    
    参数:
        func: 被装饰的函数
        
    返回:
        装饰后的函数
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_chat or update.effective_chat.type not in ['group', 'supergroup']:
            await update.effective_message.reply_text("❌ 该命令仅在群聊中可用")
            return
            
        return await func(update, context, *args, **kwargs)
    
    return wrapper
