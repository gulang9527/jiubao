import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from utils import (
    validate_time_format,
    validate_interval,
    format_duration,
    get_media_type,
    format_error_message
)
from config import BROADCAST_SETTINGS

logger = logging.getLogger(__name__)

class BroadcastManager:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot
        self.running = False
        self.task = None
        self._temp_broadcast = {}

    async def start(self):
        """启动轮播任务"""
        if self.running:
            return

        self.running = True
        self.task = asyncio.create_task(self._broadcast_loop())
        logger.info("Broadcast manager started")

    async def stop(self):
        """停止轮播任务"""
        if not self.running:
            return

        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("Broadcast manager stopped")

    async def _broadcast_loop(self):
        """轮播消息循环"""
        while self.running:
            try:
                # 获取所有活跃的轮播消息
                now = datetime.now()
                broadcasts = await self.db.broadcasts.find({
                    'start_time': {'$lte': now.isoformat()},
                    'end_time': {'$gt': now.isoformat()}
                }).to_list(None)

                for bc in broadcasts:
                    try:
                        # 检查是否需要发送
                        last_broadcast = bc.get('last_broadcast')
                        if last_broadcast:
                            last_time = datetime.fromisoformat(last_broadcast)
                            if (now - last_time).total_seconds() < bc['interval']:
                                continue

                        # 发送消息
                        await self._send_broadcast(bc)

                        # 更新发送时间
                        await self.db.broadcasts.update_one(
                            {'_id': bc['_id']},
                            {'$set': {'last_broadcast': now.isoformat()}}
                        )

                    except Exception as e:
                        logger.error(f"Error sending broadcast {bc['_id']}: {e}")

                # 清理过期的轮播消息
                await self.db.broadcasts.delete_many({
                    'end_time': {'$lte': now.isoformat()}
                })

            except Exception as e:
                logger.error(f"Error in broadcast loop: {e}")

            await asyncio.sleep(BROADCAST_SETTINGS['check_interval'])

    async def _send_broadcast(self, broadcast: Dict[str, Any]):
        """发送轮播消息"""
        try:
            content_type = broadcast['content_type']
            content = broadcast['content']
            group_id = broadcast['group_id']

            if content_type == 'text':
                await self.bot.application.bot.send_message(group_id, content)
            elif content_type == 'photo':
                await self.bot.application.bot.send_photo(group_id, content)
            elif content_type == 'video':
                await self.bot.application.bot.send_video(group_id, content)
            elif content_type == 'document':
                await self.bot.application.bot.send_document(group_id, content)
            else:
                logger.error(f"Unknown content type: {content_type}")

        except Exception as e:
            logger.error(f"Error sending broadcast: {e}")

    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """显示轮播设置界面"""
        query = update.callback_query
        try:
            broadcasts = await self.get_broadcasts(group_id)
            page = self._get_page(group_id)
            total_pages = (len(broadcasts) + 2) // 3  # 每页显示3条消息

            text = "📢 轮播消息管理\n\n"
            if broadcasts:
                start = (page - 1) * 3
                end = start + 3
                current_broadcasts = broadcasts[start:end]

                for i, bc in enumerate(current_broadcasts, start=1):
                    text += f"{i}. 内容: {bc['content'][:50]}...\n"
                    text += f"   开始: {bc['start_time']}\n"
                    text += f"   结束: {bc['end_time']}\n"
                    text += f"   间隔: {format_duration(bc['interval'])}\n\n"

                text += f"\n第 {page}/{total_pages} 页"
            else:
                text += "暂无轮播消息"

            # 创建操作键盘
            keyboard = [
                [
                    InlineKeyboardButton(
                        "添加轮播",
                        callback_data=f"broadcast_add_{group_id}"
                    ),
                    InlineKeyboardButton(
                        "删除轮播",
                        callback_data=f"broadcast_del_{group_id}"
                    )
                ]
            ]

            # 添加分页按钮
            if total_pages > 1:
                nav_row = []
                if page > 1:
                    nav_row.append(
                        InlineKeyboardButton(
                            "◀️ 上一页",
                            callback_data=f"broadcast_page_{group_id}_{page-1}"
                        )
                    )
                if page < total_pages:
                    nav_row.append(
                        InlineKeyboardButton(
                            "下一页 ▶️",
                            callback_data=f"broadcast_page_{group_id}_{page+1}"
                        )
                    )
                if nav_row:
                    keyboard.append(nav_row)

            # 添加返回按钮
            keyboard.append([
                InlineKeyboardButton(
                    "返回设置",
                    callback_data=f"settings_back_{group_id}"
                )
            ])

            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Error showing broadcast settings: {e}")
            await query.edit_message_text(format_error_message(e))

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理轮播相关的回调查询"""
        query = update.callback_query
        data = query.data
        parts = data.split('_')
        action = parts[1]
        group_id = int(parts[2])

        try:
            if action == "add":
                await self._start_add_broadcast(update, context, group_id)
            elif action == "del":
                await self._show_delete_broadcast(update, context, group_id)
            elif action == "page":
                page = int(parts[3])
                await self._change_page(update, context, group_id, page)
        except Exception as e:
            logger.error(f"Error handling broadcast callback: {e}")
            await query.edit_message_text(format_error_message(e))

    async def _start_add_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """开始添加轮播消息流程"""
        query = update.callback_query
        
        # 检查轮播消息数量限制
        broadcasts = await self.get_broadcasts(group_id)
        if len(broadcasts) >= BROADCAST_SETTINGS['max_broadcasts']:
            await query.edit_message_text(
                f"❌ 每个群组最多只能设置{BROADCAST_SETTINGS['max_broadcasts']}条轮播消息",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "返回",
                        callback_data=f"settings_broadcast_{group_id}"
                    )
                ]])
            )
            return

        text = (
            "请发送需要轮播的消息内容：\n\n"
            "支持的格式：\n"
            "1. 文本消息\n"
            "2. 图片\n"
            "3. 视频\n"
            "4. 文件"
        )

        keyboard = [[
            InlineKeyboardButton(
                "取消",
                callback_data=f"settings_broadcast_{group_id}"
            )
        ]]

        self._start_temp_broadcast(group_id)
        context.user_data['waiting_for'] = f"broadcast_content_{group_id}"

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_delete_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """显示可删除的轮播消息列表"""
        query = update.callback_query
        broadcasts = await self.get_broadcasts(group_id)

        if not broadcasts:
            await query.edit_message_text(
                "❌ 没有可删除的轮播消息",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "返回",
                        callback_data=f"settings_broadcast_{group_id}"
                    )
                ]])
            )
            return

        keyboard = []
        for bc in broadcasts:
            content_preview = bc['content'][:30] + "..." if len(bc['content']) > 30 else bc['content']
            keyboard.append([
                InlineKeyboardButton(
                    content_preview,
                    callback_data=f"bc_del_{group_id}_{bc['_id']}"
                )
            ])

        keyboard.append([
            InlineKeyboardButton(
                "返回",
                callback_data=f"settings_broadcast_{group_id}"
            )
        ])

        await query.edit_message_text(
            "选择要删除的轮播消息：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def get_broadcasts(self, group_id: int) -> list:
        """获取群组的轮播消息列表"""
        return await self.db.broadcasts.find({
            'group_id': group_id
        }).to_list(None)

    def _get_page(self, group_id: int) -> int:
        """获取当前页码"""
        return self._temp_broadcast.get(f"page_{group_id}", 1)

    def _set_page(self, group_id: int, page: int):
        """设置当前页码"""
        self._temp_broadcast[f"page_{group_id}"] = page

    def _start_temp_broadcast(self, group_id: int):
        """开始临时轮播消息"""
        self._temp_broadcast[str(group_id)] = {
            'step': 1,
            'data': {}
        }

    def _get_temp_broadcast(self, group_id: int) -> Optional[Dict[str, Any]]:
        """获取临时轮播消息"""
        return self._temp_broadcast.get(str(group_id))

    def _clear_temp_broadcast(self, group_id: int):
        """清除临时轮播消息"""
        if str(group_id) in self._temp_broadcast:
            del self._temp_broadcast[str(group_id)]

    async def _change_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, page: int):
        """切换页码"""
        self._set_page(group_id, page)
        await self.show_settings(update, context, group_id)