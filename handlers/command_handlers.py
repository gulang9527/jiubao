from telegram import Update
from telegram.ext import CommandHandler, CallbackContext
from utils.error_handler import handle_errors
from managers.settings_manager import SettingsManager

def command_handlers():
    """定义命令处理器"""
    handlers = []

    async def _handle_start(update: Update, context: CallbackContext):
        """处理 /start 命令"""
        await update.message.reply_text("欢迎使用机器人！")

    handlers.append(CommandHandler("start", _handle_start))

    async def _handle_settings(update: Update, context: CallbackContext):
        """处理 /settings 命令"""
        user_id = update.effective_user.id
        await settings_manager.start_setting(user_id, 'settings', update.effective_chat.id)
        await update.message.reply_text("请选择设置选项：")

    handlers.append(CommandHandler("settings", _handle_settings))

    return handlers