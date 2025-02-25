from telegram import Update
from telegram.ext import CallbackContext
from typing import Any

class ErrorHandler:
    """错误处理类"""
    def __init__(self):
        self.error_handlers = {}

    def register_error_handler(self, error_type: type, handler: callable):
        """注册错误处理器"""
        self.error_handlers[error_type] = handler

    async def handle_error(self, update: Update, context: CallbackContext, error: Exception):
        """处理错误"""
        error_type = type(error)
        if error_type in self.error_handlers:
            await self.error_handlers[error_type](update, context, error)
        else:
            await self.default_error_handler(update, context, error)

    async def default_error_handler(self, update: Update, context: CallbackContext, error: Exception):
        """默认错误处理器"""
        message = "❌ 发生了一个错误，请稍后再试。"
        if update.effective_message:
            await update.effective_message.reply_text(message)
        print(f"未处理的错误: {error}")