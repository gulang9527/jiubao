from telegram import Update
from telegram.ext import CommandHandler, CallbackContext
from utils.error_handler import handle_errors
from managers.settings_manager import SettingsManager

def command_handlers():
    """�����������"""
    handlers = []

    async def _handle_start(update: Update, context: CallbackContext):
        """���� /start ����"""
        await update.message.reply_text("��ӭʹ�û����ˣ�")

    handlers.append(CommandHandler("start", _handle_start))

    async def _handle_settings(update: Update, context: CallbackContext):
        """���� /settings ����"""
        user_id = update.effective_user.id
        await settings_manager.start_setting(user_id, 'settings', update.effective_chat.id)
        await update.message.reply_text("��ѡ������ѡ�")

    handlers.append(CommandHandler("settings", _handle_settings))

    return handlers