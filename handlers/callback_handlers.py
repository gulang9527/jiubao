from telegram import Update
from telegram.ext import CallbackQueryHandler, CallbackContext
from utils.error_handler import handle_errors

def callback_handlers():
    """定义回调处理器"""
    handlers = []

    async def _handle_callback(update: Update, context: CallbackContext):
        """处理回调查询"""
        query = update.callback_query
        await query.answer("操作完成！")

    handlers.append(CallbackQueryHandler(_handle_callback))

    return handlers