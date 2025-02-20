import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from utils import format_file_size, get_media_type, format_error_message
from config import DEFAULT_SETTINGS

logger = logging.getLogger(__name__)

class StatsManager:
    def __init__(self, db):
        self.db = db
        self._temp_settings = {}
        
    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """显示统计设置界面"""
        query = update.callback_query
        try:
            settings = await self.db.get_group_settings(group_id)
            text = "📊 统计设置\n\n"
            text += f"最低字节数: {format_file_size(settings.get('min_bytes', DEFAULT_SETTINGS['min_bytes']))}\n"
            text += f"统计多媒体: {'是' if settings.get('count_media', DEFAULT_SETTINGS['count_media']) else '否'}\n"
            text += f"日排行显示: {settings.get('daily_rank_size', DEFAULT_SETTINGS['daily_rank_size'])}条\n"
            text += f"月排行显示: {settings.get('monthly_rank_size', DEFAULT_SETTINGS['monthly_rank_size'])}条\n"

            keyboard = [
                [
                    InlineKeyboardButton(
                        "修改最低字节",
                        callback_data=f"stats_min_bytes_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "切换多媒体统计",
                        callback_data=f"stats_toggle_media_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "修改排行显示数",
                        callback_data=f"stats_rank_size_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "返回设置",
                        callback_data=f"settings_back_{group_id}"
                    )
                ]
            ]

            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Error showing stats settings: {e}")
            await query.edit_message_text(format_error_message(e))

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理统计设置相关的回调查询"""
        query = update.callback_query
        data = query.data
        parts = data.split('_')
        action = parts[1]
        group_id = int(parts[2])

        try:
            settings = await self.db.get_group_settings(group_id)
            
            if action == "min_bytes":
                await self._start_set_min_bytes(update, context, group_id)
            elif action == "toggle_media":
                new_value = not settings.get('count_media', DEFAULT_SETTINGS['count_media'])
                settings['count_media'] = new_value
                await self.db.update_group_settings(group_id, settings)
                await self.show_settings(update, context, group_id)
            elif action == "rank_size":
                await self._show_rank_size_settings(update, context, group_id)
        except Exception as e:
            logger.error(f"Error handling stats callback: {e}")
            await query.edit_message_text(format_error_message(e))

    async def _start_set_min_bytes(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """开始设置最低字节数流程"""
        query = update.callback_query
        text = (
            "请输入统计的最低字节数：\n"
            "（0表示不限制）\n\n"
            "示例：\n"
            "100 - 最少100字节\n"
            "1024 - 最少1KB\n"
            "0 - 不限制大小"
        )

        keyboard = [[
            InlineKeyboardButton(
                "取消",
                callback_data=f"settings_stats_{group_id}"
            )
        ]]

        self._start_temp_setting(group_id, 'min_bytes')
        context.user_data['waiting_for'] = f"stats_min_bytes_{group_id}"

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_rank_size_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """显示排行榜显示数量设置"""
        query = update.callback_query
        settings = await self.db.get_group_settings(group_id)

        text = (
            "请选择要修改的排行榜显示数量：\n\n"
            f"当前设置：\n"
            f"日排行：{settings.get('daily_rank_size', DEFAULT_SETTINGS['daily_rank_size'])}条\n"
            f"月排行：{settings.get('monthly_rank_size', DEFAULT_SETTINGS['monthly_rank_size'])}条"
        )

        keyboard = [
            [
                InlineKeyboardButton(
                    "修改日排行显示数",
                    callback_data=f"stats_daily_size_{group_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "修改月排行显示数",
                    callback_data=f"stats_monthly_size_{group_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "返回",
                    callback_data=f"settings_stats_{group_id}"
                )
            ]
        ]

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def add_message_stat(self, group_id: int, user_id: int, message: Message):
        """添加消息统计"""
        settings = await self.db.get_group_settings(group_id)
        min_bytes = settings.get('min_bytes', DEFAULT_SETTINGS['min_bytes'])
        count_media = settings.get('count_media', DEFAULT_SETTINGS['count_media'])

        # 计算消息大小
        if message.text:
            size = len(message.text.encode('utf-8'))
        elif count_media:
            media_type = get_media_type(message)
            if media_type == 'photo':
                size = message.photo[-1].file_size
            elif media_type == 'video':
                size = message.video.file_size
            elif media_type == 'document':
                size = message.document.file_size
            else:
                return  # 不统计其他类型的消息
        else:
            return  # 不统计多媒体消息

        # 检查最低字节数
        if size < min_bytes:
            return

        # 添加统计
        await self.db.add_message_stat({
            'group_id': group_id,
            'user_id': user_id,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'size': size,
            'message_type': 'text' if message.text else get_media_type(message)
        })

    async def get_daily_stats(self, group_id: int, page: int = 1) -> Tuple[list, int]:
        """获取日排行统计"""
        settings = await self.db.get_group_settings(group_id)
        limit = settings.get('daily_rank_size', DEFAULT_SETTINGS['daily_rank_size'])
        return await self.db.get_daily_stats(group_id, page, limit)

    async def get_monthly_stats(self, group_id: int, page: int = 1) -> Tuple[list, int]:
        """获取月排行统计"""
        settings = await self.db.get_group_settings(group_id)
        limit = settings.get('monthly_rank_size', DEFAULT_SETTINGS['monthly_rank_size'])
        return await self.db.get_monthly_stats(group_id, page, limit)

    async def send_rank_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                              group_id: int, page: int, is_monthly: bool = False):
        """发送排行榜消息"""
        if is_monthly:
            stats, total_pages = await self.get_monthly_stats(group_id, page)
            title = "📊 近30天发言排行"
            command = "tongji30"
        else:
            stats, total_pages = await self.get_daily_stats(group_id, page)
            title = "📊 今日发言排行"
            command = "tongji"

        if not stats:
            await update.message.reply_text("📊 暂无统计数据")
            return

        # 生成排行榜文本
        text = f"{title}\n\n"
        settings = await self.db.get_group_settings(group_id)
        min_bytes = settings.get('min_bytes', DEFAULT_SETTINGS['min_bytes'])

        for i, stat in enumerate(stats, start=(page-1)*15+1):
            try:
                user = await context.bot.get_chat_member(group_id, stat['_id'])
                name = user.user.full_name
            except Exception:
                name = f"用户{stat['_id']}"

            text += f"{i}. {name}\n"
            text += f"   消息数: {stat['total_messages']}\n"
            text += f"   总字节: {format_file_size(stat['total_size'])}\n\n"

        if min_bytes > 0:
            text += f"\n注：仅统计大于 {format_file_size(min_bytes)} 的消息"

        # 添加分页信息
        text += f"\n\n第 {page}/{total_pages} 页"
        if total_pages > 1:
            text += f"\n使用 /{command} <页码> 查看其他页"

        await update.message.reply_text(text)

    def _start_temp_setting(self, group_id: int, setting_type: str):
        """开始临时设置"""
        key = f"{group_id}_{setting_type}"
        self._temp_settings[key] = {
            'step': 1,
            'data': {}
        }

    def _get_temp_setting(self, group_id: int, setting_type: str) -> Optional[Dict[str, Any]]:
        """获取临时设置"""
        key = f"{group_id}_{setting_type}"
        return self._temp_settings.get(key)

    def _clear_temp_setting(self, group_id: int, setting_type: str):
        """清除临时设置"""
        key = f"{group_id}_{setting_type}"
        if key in self._temp_settings:
            del self._temp_settings[key]