"""
ID命令处理模块，提供查询用户和群组ID的功能
"""
import logging
import re
import html
from typing import Optional, Union
from telegram import Update, User, Chat, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ContextTypes
from telegram.error import BadRequest, Forbidden, TelegramError
from utils.decorators import check_command_usage
from utils.message_utils import set_message_expiry

logger = logging.getLogger(__name__)

@check_command_usage
async def handle_id_command(update: Update, context: CallbackContext) -> None:
    """
    处理/id命令，查询用户和群组ID
    
    支持以下用法:
    - /id: 显示当前聊天的ID
    - 回复某人的消息并发送/id: 显示被回复用户的ID和用户名
    - /id @username: 查询指定用户或群组的ID
    - /id t.me/xxx 或 /id https://t.me/xxx: 查询群组ID
    """
    # 检查必要组件
    if not update.effective_chat or not update.effective_user or not update.effective_message:
        logger.warning("无法获取必要的信息")
        return
    
    try:
        # 获取消息参数
        args = context.args
        
        # 如果命令有参数，优先处理参数
        if args:
            query = " ".join(args)
            await handle_id_query(update, context, query)
            return
        
        # 检查是否是回复消息 - 这里是关键
        reply_to_message = update.effective_message.reply_to_message
        
        # 记录更多调试信息
        if reply_to_message:
            logger.info(f"处理回复消息: 当前用户={update.effective_user.id}, 回复消息ID={reply_to_message.message_id}")
            if reply_to_message.from_user:
                logger.info(f"被回复用户ID: {reply_to_message.from_user.id}")
        
        if reply_to_message and reply_to_message.from_user:
            # 如果是回复消息，显示被回复用户的ID和用户名
            await handle_reply_id(update, context)
        else:
            # 如果没有参数和回复，显示当前聊天的ID
            await handle_current_chat_id(update, context)
    except Exception as e:
        logger.error(f"处理ID命令时出错: {e}", exc_info=True)

async def handle_current_chat_id(update: Update, context: CallbackContext) -> None:
    """处理当前聊天的ID查询"""
    chat = update.effective_chat
    user = update.effective_user
    
    if not chat or not user:
        logger.warning("无法获取聊天或用户信息")
        return
    
    # 构建消息文本
    text = ""
    
    # 添加用户信息
    text += f"👤 <b>用户信息</b>\n"
    text += f"ID: <code>{user.id}</code>\n"
    if user.username:
        text += f"用户名: @{html.escape(user.username)}\n"
    else:
        text += f"名称: {html.escape(user.first_name)}"
        if user.last_name:
            text += f" {html.escape(user.last_name)}"
        text += "\n"
        
    # 添加聊天信息
    text += f"\n💬 <b>当前聊天</b>\n"
    text += f"ID: <code>{chat.id}</code>\n"
    
    # 群组特有信息
    if chat.type in ['group', 'supergroup']:
        text += f"类型: {'超级群组' if chat.type == 'supergroup' else '普通群组'}\n"
        text += f"标题: {html.escape(chat.title)}\n"
        if chat.username:
            text += f"群组用户名: @{html.escape(chat.username)}\n"
            text += f"链接: https://t.me/{html.escape(chat.username)}\n"
    elif chat.type == 'private':
        text += "类型: 私聊\n"
    elif chat.type == 'channel':
        text += "类型: 频道\n"
        if chat.username:
            text += f"频道用户名: @{html.escape(chat.username)}\n"
    
    # 直接发送消息，不使用回复
    try:
        msg = await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            parse_mode='HTML'
        )
        
        # 在群组中自动删除
        if chat.type in ['group', 'supergroup']:
            await set_message_expiry(
                context=context,
                chat_id=chat.id,
                message_id=msg.message_id,
                feature="command_response"
            )
    except Exception as e:
        logger.error(f"发送ID信息失败: {e}", exc_info=True)

async def handle_reply_id(update: Update, context: CallbackContext) -> None:
    """处理回复消息的ID查询"""
    chat = update.effective_chat
    message = update.effective_message
    
    if not chat or not message:
        logger.warning("无法获取聊天或消息信息")
        return
        
    # 检查是否是回复消息
    reply_to_message = message.reply_to_message
    if not reply_to_message:
        logger.warning("不是回复消息")
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text="❌ 无法确定被回复的消息"
            )
        except Exception as e:
            logger.error(f"发送错误消息失败: {e}")
        return
    
    # 获取被回复消息的发送者
    replied_user = reply_to_message.from_user
    if not replied_user:
        logger.warning("无法获取被回复消息的发送者")
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text="❌ 无法获取被回复用户的信息"
            )
        except Exception as e:
            logger.error(f"发送错误消息失败: {e}")
        return
    
    # 记录详细信息以进行调试
    logger.info(f"当前用户ID: {update.effective_user.id}, 被回复用户ID: {replied_user.id}")
    
    # 构建消息文本
    text = f"👤 <b>被回复用户信息</b>\n"
    text += f"ID: <code>{replied_user.id}</code>\n"
    
    if replied_user.username:
        text += f"用户名: @{html.escape(replied_user.username)}\n"
    else:
        text += f"名称: {html.escape(replied_user.first_name)}"
        if replied_user.last_name:
            text += f" {html.escape(replied_user.last_name)}"
        text += "\n"
    
    # 检查用户是否是机器人
    if replied_user.is_bot:
        text += "类型: 机器人\n"
    
    # 直接发送消息，不使用回复
    try:
        msg = await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            parse_mode='HTML'
        )
        
        # 在群组中自动删除
        if chat.type in ['group', 'supergroup']:
            await set_message_expiry(
                context=context,
                chat_id=chat.id,
                message_id=msg.message_id,
                feature="command_response"
            )
    except Exception as e:
        logger.error(f"发送ID信息失败: {e}", exc_info=True)

async def handle_id_query(update: Update, context: CallbackContext, query: str) -> None:
    """
    处理ID查询
    
    参数:
        update: 更新对象
        context: 上下文对象
        query: 查询字符串
    """
    chat = update.effective_chat
    
    if not chat:
        return
    
    # 准备回复文本
    text = ""
    
    # 检查是否是Telegram链接
    link_match = re.match(r'^(https?://)?(t\.me|telegram\.me)/(@)?([a-zA-Z0-9_]+)$', query)
    if link_match:
        username = link_match.group(4)
        # 移除可能的链接前缀
        if username.startswith('joinchat/'):
            username = username[9:]
        await fetch_entity_info(update, context, f"@{username}")
        return
    
    # 检查是否是用户名
    if query.startswith('@'):
        await fetch_entity_info(update, context, query)
        return
    
    # 尝试作为用户ID或群组ID查询
    try:
        entity_id = int(query.strip())
        await fetch_entity_by_id(update, context, entity_id)
        return
    except ValueError:
        # 如果不是数字，可能是一个普通查询
        await fetch_entity_info(update, context, query)

async def fetch_entity_info(update: Update, context: CallbackContext, entity_query: str) -> None:
    """
    通过用户名或其他标识获取实体信息
    
    参数:
        update: 更新对象
        context: 上下文对象
        entity_query: 实体查询字符串
    """
    chat = update.effective_chat
    
    if not chat:
        return
    
    # 尝试获取实体信息
    try:
        # 移除可能的@前缀
        username = entity_query.strip('@')
        
        # 尝试获取信息
        chat_info = None
        user_info = None
        error_msg = None
        
        try:
            # 优先尝试获取聊天信息
            chat_info = await context.bot.get_chat(f"@{username}")
        except (BadRequest, Forbidden) as e:
            # 如果不是聊天，可能是用户
            try:
                # 尝试获取用户信息（注意：获取用户信息可能受限）
                chat_info = await context.bot.get_chat(f"@{username}")
            except Exception as sub_e:
                error_msg = f"找不到与 @{html.escape(username)} 相关的用户或群组"
        
        # 构建回复文本
        if chat_info:
            text = await format_entity_info(chat_info)
        elif user_info:
            text = await format_entity_info(user_info)
        else:
            text = f"❌ {error_msg or f'无法找到 @{html.escape(username)}'}"
        
        # 发送结果
        msg = await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            parse_mode='HTML'
        )
        
        # 在群组中自动删除
        if chat.type in ['group', 'supergroup']:
            await set_message_expiry(
                context=context,
                chat_id=chat.id,
                message_id=msg.message_id,
                feature="command_response"
            )
        
    except Exception as e:
        logger.error(f"获取实体信息时出错: {e}", exc_info=True)
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"❌ 查询实体信息时出错"
            )
        except Exception as sub_e:
            logger.error(f"发送错误信息失败: {sub_e}", exc_info=True)

async def fetch_entity_by_id(update: Update, context: CallbackContext, entity_id: int) -> None:
    """
    通过ID获取实体信息
    
    参数:
        update: 更新对象
        context: 上下文对象
        entity_id: 实体ID
    """
    chat = update.effective_chat
    
    if not chat:
        return
    
    # 尝试获取实体信息
    try:
        entity_info = await context.bot.get_chat(entity_id)
        
        # 格式化信息
        text = await format_entity_info(entity_info)
        
        # 发送结果
        msg = await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            parse_mode='HTML'
        )
        
        # 在群组中自动删除
        if chat.type in ['group', 'supergroup']:
            await set_message_expiry(
                context=context,
                chat_id=chat.id,
                message_id=msg.message_id,
                feature="command_response"
            )
            
    except BadRequest:
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"❌ 找不到ID为 {entity_id} 的用户或群组"
            )
        except Exception as e:
            logger.error(f"发送错误信息失败: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"通过ID获取实体信息时出错: {e}", exc_info=True)
        try:
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"❌ 查询实体信息时出错"
            )
        except Exception as sub_e:
            logger.error(f"发送错误信息失败: {sub_e}", exc_info=True)

async def format_entity_info(entity: Union[User, Chat]) -> str:
    """
    格式化实体信息
    
    参数:
        entity: 用户或聊天对象
        
    返回:
        格式化的HTML文本
    """
    # 判断实体类型
    is_user = isinstance(entity, User)
    
    text = ""
    
    if is_user:
        # 用户信息
        text += f"👤 <b>用户信息</b>\n"
        text += f"ID: <code>{entity.id}</code>\n"
        if entity.username:
            text += f"用户名: @{html.escape(entity.username)}\n"
        else:
            text += f"名称: {html.escape(entity.first_name)}"
            if entity.last_name:
                text += f" {html.escape(entity.last_name)}"
            text += "\n"
        
        # 检查是否是机器人
        if entity.is_bot:
            text += "类型: 机器人\n"
    else:
        # 聊天信息
        text += f"💬 <b>{'群组' if entity.type in ['group', 'supergroup'] else '频道' if entity.type == 'channel' else '聊天'}</b>\n"
        text += f"ID: <code>{entity.id}</code>\n"
        
        # 根据类型显示不同信息
        if entity.type in ['group', 'supergroup']:
            text += f"类型: {'超级群组' if entity.type == 'supergroup' else '普通群组'}\n"
            text += f"标题: {html.escape(entity.title)}\n"
            if entity.username:
                text += f"群组用户名: @{html.escape(entity.username)}\n"
                text += f"链接: https://t.me/{html.escape(entity.username)}\n"
            # 可能需要获取成员数量
            try:
                if hasattr(entity, 'members_count'):
                    text += f"成员数: {entity.members_count}\n"
            except:
                pass
        elif entity.type == 'channel':
            text += "类型: 频道\n"
            text += f"标题: {html.escape(entity.title)}\n"
            if entity.username:
                text += f"频道用户名: @{html.escape(entity.username)}\n"
                text += f"链接: https://t.me/{html.escape(entity.username)}\n"
    
    return text
