from telegram import Update
from telegram.ext import CallbackContext
from utils.error_handler import ErrorHandler

class ErrorMiddleware:
    """错误中间件类"""
    def __init__(self, error_handler: ErrorHandler):
        self.error_handler = error_handler

    async def __call__(self, update: Update, context: CallbackContext):
        """处理更新"""
        try:
            await context.application.process_update(update)
        except Exception as e:
            await self.error_handler.handle_error(update, context, e)