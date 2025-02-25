from telegram import Update, Message
from telegram.ext import MessageHandler, CallbackContext
from utils.error_handler import handle_errors
from managers.keyword_manager import KeywordManager

def message_handlers():
    """定义消息处理器"""
    handlers = []

    async def _handle_text_message(update: Update, context: CallbackContext):
        """处理文本消息"""
        message = update.effective_message
        if not message.text:
            return

        keyword_manager = KeywordManager(context.bot_data['db'])
        response = await keyword_manager.match_keyword(
            update.effective_chat.id,
            message.text,
            message
        )

        if response:
            await message.reply_text(response)

    handlers.append(MessageHandler(filters.TEXT, _handle_text_message))

    return handlers