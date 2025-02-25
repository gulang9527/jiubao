from telegram import Update
from telegram.ext import CallbackContext
from db import Database

class MessageMiddleware:
    """消息中间件类"""
    def __init__(self):
        self.db = Database()

    async def __call__(self, update: Update, context: CallbackContext):
        """处理更新"""
        if not update.effective_message:
            return

        try:
            # 基本安全检查
            if not await self._check_basic_security(update):
                return

            # 权限检查
            if not await self._check_permissions(update):
                return

            # 继续处理消息
            await context.application.process_update(update)

        except Exception as e:
            print(f"中间件处理错误: {e}")

    async def _check_basic_security(self, update: Update) -> bool:
        """基本安全检查"""
        message = update.effective_message

        # 检查消息大小
        if message.text and len(message.text) > 4096:
            await message.reply_text("❌ 消息内容过长")
            return False

        # 检查文件大小
        if message.document and message.document.file_size > 20 * 1024 * 1024:
            await message.reply_text("❌ 文件大小超过限制")
            return False

        return True

    async def _check_permissions(self, update: Update) -> bool:
        """权限检查"""
        if not update.effective_chat or not update.effective_user:
            return False

        chat_id = update.effective_chat.id
        user_id = update.effective_user.id

        # 检查用户是否被封禁
        if await self.db.is_user_banned(user_id):
            return False

        # 检查群组是否已授权
        if not await self.db.get_group(chat_id):
            return False

        return True