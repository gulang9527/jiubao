"""
回调处理函数，处理按钮回调
"""
import logging
from typing import Optional, Any, Dict, List

from telegram import Update
from telegram.ext import CallbackContext

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
