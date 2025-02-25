from telegram import Update
from telegram.ext import CallbackQueryHandler, CallbackContext
from utils.error_handler import handle_errors

def callback_handlers():
    """����ص�������"""
    handlers = []

    async def _handle_callback(update: Update, context: CallbackContext):
        """����ص���ѯ"""
        query = update.callback_query
        await query.answer("������ɣ�")

    handlers.append(CallbackQueryHandler(_handle_callback))

    return handlers