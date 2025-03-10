"""
错误处理模块，处理机器人运行时的各种错误
"""
import logging
from typing import Any

from telegram import Update
from telegram.ext import CallbackContext

class ErrorHandler:
    """
    错误处理类，负责处理和记录机器人运行时的错误
    """
    def __init__(self, logger):
        """
        初始化错误处理器
        
        参数:
            logger: 日志记录器
        """
        self.logger = logger
        
    async def handle_error(self, update: Update, context: CallbackContext):
        """
        处理错误
        
        参数:
            update: 更新对象
            context: 上下文对象
        """
        error = context.error
        self.logger.error(f"处理请求时出错: {error}", exc_info=True)
        
        # 尝试向用户发送错误通知
        if update and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    "❌ 操作过程中出现错误，请稍后重试或联系管理员。"
                )
            except Exception as e:
                self.logger.error(f"发送错误消息失败: {e}")
