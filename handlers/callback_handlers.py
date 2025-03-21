"""
回调处理函数，处理按钮回调
"""
import logging
from typing import Optional, Any, Dict, List

from telegram import Update
from telegram.ext import CallbackContext
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from utils.decorators import handle_callback_errors

logger = logging.getLogger(__name__)

@handle_callback_errors
async def handle_callback(update: Update, context: CallbackContext):
    """
    处理回调查询
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    query = update.callback_query
    
    if not query:
        return
        
    # 获取机器人实例和回调处理器
    bot_instance = context.application.bot_data.get('bot_instance')
    callback_handler = bot_instance.callback_handler
    
    # 记录回调信息
    logger.info(f"收到回调查询: {query.data}")
    
    # 使用回调处理器处理
    handled = await callback_handler.handle(update, context)
    
    # 如果没有处理，应答回调以避免加载图标一直显示
    if not handled:
        logger.warning(f"未处理的回调查询: {query.data}")
        await query.answer("未知的操作")

@handle_callback_errors
async def handle_manageable_groups_callback(update: Update, context: CallbackContext, data: str = None):
    query = update.callback_query
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 立即应答回调查询
    await query.answer()
    
    # 获取用户ID
    user_id = update.effective_user.id
    
    # 获取用户可管理的群组
    try:
        # 获取用户可管理的群组 - 修改这里
        manageable_groups = await bot_instance.db.get_manageable_groups(user_id)
        # 检查是否为超级管理员 - 修改这里
        superadmin = await bot_instance.is_superadmin(user_id)
        
        # 无需使用 get_all_authorized_groups - 直接使用已有的方法
        if not manageable_groups:
            # 没有可管理的群组
            await query.edit_message_text(
                "您没有可管理的群组权限。\n\n"
                "如果您是群组管理员，请确保已将机器人添加到群组，并使用 /authgroup 命令授权机器人。"
            )
            return
            
        # 构建群组选择按钮
        keyboard = []
        for group in manageable_groups:
            group_id = group['group_id']
            # 尝试获取群组名称
            try:
                group_info = await context.bot.get_chat(group_id)
                group_title = group_info.title or f"群组 {group_id}"
            except Exception:
                group_title = f"群组 {group_id}"
                
            keyboard.append([InlineKeyboardButton(group_title, callback_data=f"settings_select_{group_id}")])
        
        # 添加一个返回按钮
        keyboard.append([InlineKeyboardButton("返回", callback_data="settings")])
        
        # 显示群组列表
        await query.edit_message_text(
            "📋 您可以管理的群组列表：\n\n"
            "请选择要管理的群组",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"获取可管理群组出错: {str(e)}", exc_info=True)
        await query.edit_message_text(f"❌ 获取可管理群组出错: {str(e)}")
