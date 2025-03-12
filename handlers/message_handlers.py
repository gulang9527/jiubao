"""
消息处理函数，处理非命令消息
"""
import logging
import asyncio
from typing import Optional, Any, Dict, List

from telegram import Update, Message
from telegram.ext import CallbackContext

from utils.decorators import error_handler
from utils.message_utils import get_media_type, validate_delete_timeout
from db.models import GroupPermission

logger = logging.getLogger(__name__)

@error_handler
async def handle_message(update: Update, context: CallbackContext):
    """处理非命令消息"""
    logger.debug("进入handle_message方法")
    
    # 基本检查
    if not update.effective_message or not update.effective_user or not update.effective_chat:
        logger.debug("消息缺少基本属性")
        return
        
    bot_instance = context.application.bot_data.get('bot_instance')
    message = update.effective_message
    user_id = update.effective_user.id
    group_id = update.effective_chat.id
    
    if message.text:
        logger.info(f"处理消息 - 用户ID: {user_id}, 消息内容: {message.text}")
    else:
        media_type = get_media_type(message)
        logger.info(f"处理消息 - 用户ID: {user_id}, 消息类型: {media_type or '未知'}")
    if update.effective_user:
        logger.info(f"用户 {user_id} 的上下文数据: {context.user_data}")
    
    logger.debug(f"处理消息 - 用户ID: {user_id}, 群组ID: {group_id}, 消息类型: {get_media_type(message) or 'text'}")
    
    # 处理表单输入
    if await handle_form_input(update, context):
        logger.debug(f"消息被表单处理器处理")
        return
    
    # 处理设置输入
    if await handle_settings_input(update, context):
        logger.debug(f"消息被设置处理器处理")
        return
    
    # 私聊消息单独处理
    if update.effective_chat.type == 'private':
        await handle_private_message(update, context)
        return
    
    # 处理群组消息
    await handle_group_message(update, context)

async def handle_form_input(update: Update, context: CallbackContext) -> bool:
    """
    处理表单输入
    
    参数:
        update: 更新对象
        context: 上下文对象
        
    返回:
        是否处理了输入
    """
    user_id = update.effective_user.id
    waiting_for = context.user_data.get('waiting_for')
    
    if not waiting_for:
        logger.debug(f"用户 {user_id} 没有等待中的表单输入")
        return False
        
    message = update.effective_message
    logger.info(f"处理表单输入: {waiting_for}, 用户ID: {user_id}")
    logger.info(f"消息类型: {type(message)}, 消息内容: text={bool(message.text)}, photo={bool(message.photo)}")
    
    # 关键词表单处理
    if waiting_for.startswith('keyword_'):
        logger.info(f"处理关键词表单输入: {waiting_for}")
        from handlers.keyword_handlers import handle_keyword_form_input
        try:
            handled = await handle_keyword_form_input(update, context, waiting_for)
            logger.info(f"关键词表单处理结果: {handled}")
            return handled
        except Exception as e:
            logger.error(f"处理关键词表单输入出错: {e}", exc_info=True)
            return False
    
    # 轮播消息表单处理
    elif waiting_for.startswith('broadcast_'):
        logger.info(f"处理轮播消息表单输入: {waiting_for}")
        from handlers.broadcast_handlers import handle_broadcast_form_input
        try:
            handled = await handle_broadcast_form_input(update, context, waiting_for)
            logger.info(f"轮播消息表单处理结果: {handled}")
            return handled
        except Exception as e:
            logger.error(f"处理轮播消息表单输入出错: {e}", exc_info=True)
            return False
        
    logger.warning(f"未知的表单输入类型: {waiting_for}")
    return False

async def handle_settings_input(update: Update, context: CallbackContext) -> bool:
    """
    处理设置输入
    
    参数:
        update: 更新对象
        context: 上下文对象
        
    返回:
        是否处理了输入
    """
    bot_instance = context.application.bot_data.get('bot_instance')
    user_id = update.effective_user.id
    
    # 获取活动的设置
    active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
    if not active_settings:
        return False
    
    # 处理统计设置
    if 'stats_min_bytes' in active_settings:
        from handlers.settings_handlers import process_min_bytes_setting
        await bot_instance.settings_manager.process_setting(
            user_id, 'stats_min_bytes', update.effective_message, 
            lambda state, msg: process_min_bytes_setting(bot_instance, state, msg)
        )
        return True
        
    if 'stats_daily_rank' in active_settings:
        from handlers.settings_handlers import process_daily_rank_setting
        await bot_instance.settings_manager.process_setting(
            user_id, 'stats_daily_rank', update.effective_message, 
            lambda state, msg: process_daily_rank_setting(bot_instance, state, msg)
        )
        return True
        
    if 'stats_monthly_rank' in active_settings:
        from handlers.settings_handlers import process_monthly_rank_setting
        await bot_instance.settings_manager.process_setting(
            user_id, 'stats_monthly_rank', update.effective_message, 
            lambda state, msg: process_monthly_rank_setting(bot_instance, state, msg)
        )
        return True
        
    # 处理自动删除设置
    if 'auto_delete_timeout' in active_settings:
        from handlers.settings_handlers import process_auto_delete_timeout
        await bot_instance.settings_manager.process_setting(
            user_id, 'auto_delete_timeout', update.effective_message, 
            lambda state, msg: process_auto_delete_timeout(bot_instance, state, msg)
        )
        return True
    
    return False

async def handle_private_message(update: Update, context: CallbackContext):
    """
    处理私聊消息
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    bot_instance = context.application.bot_data.get('bot_instance')
    user_id = update.effective_user.id
    message = update.effective_message
    
    logger.info(f"处理私聊消息 - 用户ID: {user_id}, 消息: {message.text}")
    
    # 检查用户是否被封禁
    is_banned = await bot_instance.db.is_user_banned(user_id)
    logger.info(f"用户 {user_id} 封禁状态: {is_banned}")
    
    if is_banned:
        logger.warning(f"已封禁用户 {user_id} 尝试使用机器人")
        await message.reply_text("❌ 你已被封禁，无法使用此机器人")
        return
    
    # 检查用户是否有等待中的表单输入
    waiting_for = context.user_data.get('waiting_for')
    logger.info(f"用户 {user_id} 的等待状态: {waiting_for}")
    
    if waiting_for:
        logger.info(f"用户 {user_id} 在私聊中有等待输入: {waiting_for}")
        
        # 关键词表单处理
        if waiting_for.startswith('keyword_'):
            logger.info(f"尝试处理关键词表单输入: {waiting_for}")
            from handlers.keyword_handlers import handle_keyword_form_input
            try:
                handled = await handle_keyword_form_input(update, context, waiting_for)
                logger.info(f"关键词表单处理结果: {handled}")
                if handled:
                    return
            except Exception as e:
                logger.error(f"处理关键词表单输入出错: {e}", exc_info=True)
        
        # 轮播消息表单处理
        elif waiting_for.startswith('broadcast_'):
            logger.info(f"尝试处理轮播消息表单输入: {waiting_for}, 消息类型: {message.content_type}")
            from handlers.broadcast_handlers import handle_broadcast_form_input
            try:
                logger.info(f"调用 handle_broadcast_form_input 之前, 消息有photo: {bool(message.photo)}")
                handled = await handle_broadcast_form_input(update, context, waiting_for)
                logger.info(f"轮播消息表单处理结果: {handled}")
                if handled:
                    return
                else:
                    logger.warning(f"轮播消息表单未处理成功, 继续执行后续代码")
            except Exception as e:
                logger.error(f"处理轮播消息表单输入出错: {e}", exc_info=True)
    
    # 检查管理员状态
    is_admin = await bot_instance.is_admin(user_id)
    logger.info(f"用户 {user_id} 的管理员状态: {is_admin}")
    
    # 如果非管理员，提示使用/start
    if not is_admin:
        try:
            await message.reply_text("请使用 /start 命令获取帮助信息")
            logger.info(f"已向用户 {user_id} 发送使用/start的提示")
        except Exception as e:
            logger.error(f"向用户 {user_id} 发送提示时出错: {e}")
        return
    
    # 管理员处理
    if message.text:
        try:
            await message.reply_text("请使用 /settings 或 /admingroups 管理您的群组")
            logger.info(f"已向管理员 {user_id} 发送操作提示")
        except Exception as e:
            logger.error(f"向管理员 {user_id} 发送提示时出错: {e}")

    # 在管理员处理代码后添加检查未完成表单的逻辑
    if 'keyword_form' in context.user_data and not context.user_data.get('waiting_for'):
        from handlers.keyword_handlers import show_keyword_response_options
        await message.reply_text("您有一个未完成的关键词表单。请继续完成或使用 /cancel 取消。")
        await show_keyword_response_options(update, context)
        return
        
async def handle_group_message(update: Update, context: CallbackContext):
    """
    处理群组消息
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    bot_instance = context.application.bot_data.get('bot_instance')
    message = update.effective_message
    user_id = update.effective_user.id
    group_id = update.effective_chat.id
    
    # 处理关键词回复
    if message.text and await bot_instance.has_permission(group_id, GroupPermission.KEYWORDS):
        logger.debug(f"检查关键词匹配 - 群组: {group_id}, 文本: {message.text[:20]}...")
        try:
            keyword_id = await bot_instance.keyword_manager.match_keyword(group_id, message.text, message)
            if keyword_id:
                logger.info(f"找到匹配关键词: {keyword_id}")
                await send_keyword_response(bot_instance, message, keyword_id, group_id)
        except Exception as e:
            logger.error(f"关键词匹配过程出错: {e}", exc_info=True)
    
    # 处理消息统计
    if await bot_instance.has_permission(group_id, GroupPermission.STATS):
        try:
            await bot_instance.stats_manager.add_message_stat(group_id, user_id, message)
        except Exception as e:
            logger.error(f"添加消息统计失败: {e}", exc_info=True)

async def send_keyword_response(bot_instance, original_message: Message, keyword_id: str, group_id: int):
    """
    发送关键词回复
    
    参数:
        bot_instance: 机器人实例
        original_message: 原始消息
        keyword_id: 关键词ID
        group_id: 群组ID
    """
    try:
        # 获取关键词数据
        keyword = await bot_instance.keyword_manager.get_keyword_by_id(group_id, keyword_id)
        if not keyword:
            logger.error(f"关键词 {keyword_id} 不存在")
            return
                    
        # 检查是否为命令关键词
        if keyword.get('is_command', False) and keyword.get('command'):
            command = keyword.get('command')
            logger.info(f"执行命令关键词: {command}")
            
            # 创建一个模拟的命令更新对象
            from telegram.ext import ContextTypes
            context = ContextTypes.DEFAULT_TYPE.context.copy_with(bot_instance.application)
            context.args = []  # 默认无参数
            
            # 创建假的Update对象,模拟命令调用
            fake_update = Update(
                update_id=original_message.message_id,
                message=original_message
            )
            
            # 执行对应的命令
            if command == '/tongji' or command == '/tongji30':
                from handlers.command_handlers import handle_rank_command
                # 发送"正在查询"的消息
                await original_message.reply_text(keyword.get('response', '正在查询...'))
                # 执行命令
                await handle_rank_command(fake_update, context)
                return
                
        # 准备消息内容
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        
        text = keyword.get('response', '')
        media = keyword.get('media')
        buttons = keyword.get('buttons', [])
        
        # 创建内联键盘（如果有按钮）
        reply_markup = None
        if buttons:
            keyboard = []
            for button in buttons:
                keyboard.append([InlineKeyboardButton(button['text'], url=button['url'])])
            reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 根据内容组合发送不同类型的消息
        if media and media.get('type'):
            if media['type'] == 'photo':
                msg = await original_message.reply_photo(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'video':
                msg = await original_message.reply_video(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'document':
                msg = await original_message.reply_document(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'animation':
                msg = await original_message.reply_animation(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            else:
                # 默认作为文档发送
                msg = await original_message.reply_document(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
        else:
            # 纯文本消息或者只有按钮的消息
            msg = await original_message.reply_text(
                text or "关键词回复", reply_markup=reply_markup
            )
            
        # 处理自动删除
        settings = await bot_instance.db.get_group_settings(group_id)
        if settings.get('auto_delete', False):
            timeout = validate_delete_timeout(message_type='keyword')
            asyncio.create_task(bot_instance._schedule_delete(msg, timeout))
            
    except Exception as e:
        logger.error(f"发送关键词回复出错: {e}", exc_info=True)
