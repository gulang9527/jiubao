import logging
from telegram import Update
from telegram.ext import (
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters
)

from db import GroupPermission
from utils import parse_command_args, format_error_message

logger = logging.getLogger(__name__)

class BotHandlers:
    def __init__(self, bot):
        self.bot = bot

    def _add_handlers(self):
        """添加所有命令处理器"""
        # 消息处理器
        self.bot.application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message)
        )
        
        # 统计命令
        self.bot.application.add_handler(
            CommandHandler(["tongji", "tongji30"], self._handle_rank_command)
        )
        
        # 设置命令
        self.bot.application.add_handler(
            CommandHandler("settings", self._handle_settings)
        )
        
        # 回调查询处理器
        self.bot.application.add_handler(
            CallbackQueryHandler(self._handle_settings_callback, pattern="^settings_")
        )
        self.bot.application.add_handler(
            CallbackQueryHandler(self._handle_keyword_callback, pattern="^keyword_")
        )
        self.bot.application.add_handler(
            CallbackQueryHandler(self._handle_broadcast_callback, pattern="^broadcast_")
        )
        self.bot.application.add_handler(
            CallbackQueryHandler(self._handle_stats_callback, pattern="^stats_")
        )

    async def _handle_message(self, update: Update, context):
        """处理普通消息"""
        if not update.effective_chat or not update.effective_user or not update.message:
            return
            
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        try:
            # 处理关键词匹配
            if await self.bot.has_permission(chat_id, GroupPermission.KEYWORDS):
                if update.message.text:
                    # 尝试匹配关键词
                    response = await self.bot.keyword_manager.match_keyword(
                        chat_id,
                        update.message.text,
                        update.message
                    )
                    if response:
                        if response.startswith('__media__'):
                            # 处理媒体响应
                            _, media_type, file_id = response.split('__')
                            if media_type == 'photo':
                                await context.bot.send_photo(chat_id, file_id)
                            elif media_type == 'video':
                                await context.bot.send_video(chat_id, file_id)
                            elif media_type == 'document':
                                await context.bot.send_document(chat_id, file_id)
                        else:
                            # 处理文本响应
                            await update.message.reply_text(response)
            
            # 处理消息统计
            if await self.bot.has_permission(chat_id, GroupPermission.STATS):
                await self.bot.stats_manager.add_message_stat(chat_id, user_id, update.message)
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await update.message.reply_text(format_error_message(e))

    async def _handle_rank_command(self, update: Update, context):
        """处理统计命令（tongji/tongji30）"""
        if not update.effective_chat or not update.message:
            return
            
        try:
            command = update.message.text.split('@')[0][1:]  # 移除 / 和机器人用户名
            group_id = update.effective_chat.id
            
            # 检查权限
            if not await self.bot.has_permission(group_id, GroupPermission.STATS):
                await update.message.reply_text("❌ 此群组未启用统计功能")
                return
                
            # 获取页码
            page = 1
            if context.args:
                try:
                    page = int(context.args[0])
                    if page < 1:
                        raise ValueError("页码必须大于0")
                except ValueError as e:
                    await update.message.reply_text(f"❌ {str(e)}")
                    return
                    
            # 获取统计数据
            await self.bot.stats_manager.send_rank_message(
                update,
                context,
                group_id,
                page,
                is_monthly=(command == "tongji30")
            )
            
        except Exception as e:
            logger.error(f"Error handling rank command: {e}")
            await update.message.reply_text(format_error_message(e))

    async def _handle_settings(self, update: Update, context):
        """处理settings命令"""
        if not update.effective_user or not update.message:
            return
            
        try:
            # 检查权限
            if not await self.bot.is_admin(update.effective_user.id):
                await update.message.reply_text("❌ 需要管理员权限")
                return
                
            # 显示群组选择界面
            await self.bot.settings_manager.show_group_selection(update, context)
            
        except Exception as e:
            logger.error(f"Error handling settings command: {e}")
            await update.message.reply_text(format_error_message(e))

    async def _handle_settings_callback(self, update: Update, context):
        """处理设置相关的回调查询"""
        query = update.callback_query
        await query.answer()
        
        try:
            # 将处理委托给设置管理器
            await self.bot.settings_manager.handle_callback(update, context)
        except Exception as e:
            logger.error(f"Error handling settings callback: {e}")
            await query.edit_message_text(format_error_message(e))

    async def _handle_keyword_callback(self, update: Update, context):
        """处理关键词相关的回调查询"""
        query = update.callback_query
        await query.answer()
        
        try:
            # 将处理委托给关键词管理器
            await self.bot.keyword_manager.handle_callback(update, context)
        except Exception as e:
            logger.error(f"Error handling keyword callback: {e}")
            await query.edit_message_text(format_error_message(e))

    async def _handle_broadcast_callback(self, update: Update, context):
        """处理轮播消息相关的回调查询"""
        query = update.callback_query
        await query.answer()
        
        try:
            # 将处理委托给轮播管理器
            await self.bot.broadcast_manager.handle_callback(update, context)
        except Exception as e:
            logger.error(f"Error handling broadcast callback: {e}")
            await query.edit_message_text(format_error_message(e))

    async def _handle_stats_callback(self, update: Update, context):
        """处理统计设置相关的回调查询"""
        query = update.callback_query
        await query.answer()
        
        try:
            # 将处理委托给统计管理器
            await self.bot.stats_manager.handle_callback(update, context)
        except Exception as e:
            logger.error(f"Error handling stats callback: {e}")
            await query.edit_message_text(format_error_message(e))