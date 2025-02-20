import logging
from typing import Optional, Dict, Any
import re
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from utils import validate_regex, format_error_message
from config import KEYWORD_SETTINGS

logger = logging.getLogger(__name__)

class KeywordManager:
    def __init__(self, db):
        self.db = db
        self._built_in_keywords = {}
        self._temp_keywords = {}

    def register_built_in_keyword(self, pattern: str, handler: callable):
        """注册内置关键词"""
        self._built_in_keywords[pattern] = handler

    async def match_keyword(self, group_id: int, text: str, message: Message) -> Optional[str]:
        """匹配关键词并返回回复"""
        # 首先检查内置关键词
        for pattern, handler in self._built_in_keywords.items():
            if text == pattern:
                return await handler(message)

        # 然后检查自定义关键词
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            try:
                if kw['type'] == 'regex':
                    pattern = re.compile(kw['pattern'])
                    if pattern.search(text):
                        return self._format_response(kw)
                else:  # exact match
                    if text == kw['pattern']:
                        return self._format_response(kw)
            except Exception as e:
                logger.error(f"Error matching keyword {kw['pattern']}: {e}")
                continue

        return None

    def _format_response(self, keyword: Dict[str, Any]) -> str:
        """格式化关键词回复"""
        if keyword['response_type'] == 'text':
            return keyword['response']
        elif keyword['response_type'] in ['photo', 'video', 'document']:
            return f"__media__{keyword['response_type']}__{keyword['response']}"
        else:
            return "❌ 不支持的回复类型"

    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """显示关键词设置界面"""
        query = update.callback_query
        try:
            keywords = await self.get_keywords(group_id)
            page = self._get_page(group_id)
            total_pages = (len(keywords) + 4) // 5  # 每页显示5个关键词

            text = "📝 关键词管理\n\n"
            if keywords:
                start = (page - 1) * 5
                end = start + 5
                current_keywords = keywords[start:end]

                for i, kw in enumerate(current_keywords, start=1):
                    text += f"{i}. 类型: {'正则' if kw['type'] == 'regex' else '精确'}\n"
                    text += f"   触发: {kw['pattern']}\n"
                    text += f"   回复: {kw['response'][:50]}...\n\n"

                text += f"\n第 {page}/{total_pages} 页"
            else:
                text += "暂无关键词"

            # 创建操作键盘
            keyboard = [
                [
                    InlineKeyboardButton(
                        "添加关键词",
                        callback_data=f"keyword_add_{group_id}"
                    ),
                    InlineKeyboardButton(
                        "删除关键词",
                        callback_data=f"keyword_del_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "导入关键词",
                        callback_data=f"keyword_import_{group_id}"
                    ),
                    InlineKeyboardButton(
                        "导出关键词",
                        callback_data=f"keyword_export_{group_id}"
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
                            callback_data=f"keyword_page_{group_id}_{page-1}"
                        )
                    )
                if page < total_pages:
                    nav_row.append(
                        InlineKeyboardButton(
                            "下一页 ▶️",
                            callback_data=f"keyword_page_{group_id}_{page+1}"
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
            logger.error(f"Error showing keyword settings: {e}")
            await query.edit_message_text(format_error_message(e))

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理关键词相关的回调查询"""
        query = update.callback_query
        data = query.data
        parts = data.split('_')
        action = parts[1]
        group_id = int(parts[2])

        try:
            if action == "add":
                await self._start_add_keyword(update, context, group_id)
            elif action == "del":
                await self._show_delete_keyword(update, context, group_id)
            elif action == "import":
                await self._start_import_keywords(update, context, group_id)
            elif action == "export":
                await self._export_keywords(update, context, group_id)
            elif action == "page":
                page = int(parts[3])
                await self._change_page(update, context, group_id, page)
        except Exception as e:
            logger.error(f"Error handling keyword callback: {e}")
            await query.edit_message_text(format_error_message(e))

    async def _start_add_keyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """开始添加关键词流程"""
        query = update.callback_query
        keyboard = [
            [
                InlineKeyboardButton(
                    "精确匹配",
                    callback_data=f"kw_type_{group_id}_exact"
                ),
                InlineKeyboardButton(
                    "正则匹配",
                    callback_data=f"kw_type_{group_id}_regex"
                )
            ],
            [
                InlineKeyboardButton(
                    "返回",
                    callback_data=f"settings_keywords_{group_id}"
                )
            ]
        ]

        text = (
            "请选择关键词类型：\n\n"
            "1. 精确匹配 - 完全匹配消息文本\n"
            "2. 正则匹配 - 使用正则表达式匹配"
        )

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_delete_keyword(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """显示可删除的关键词列表"""
        query = update.callback_query
        keywords = await self.get_keywords(group_id)

        if not keywords:
            await query.edit_message_text(
                "❌ 没有可删除的关键词",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "返回",
                        callback_data=f"settings_keywords_{group_id}"
                    )
                ]])
            )
            return

        keyboard = []
        for kw in keywords:
            pattern_preview = kw['pattern'][:30] + "..." if len(kw['pattern']) > 30 else kw['pattern']
            keyboard.append([
                InlineKeyboardButton(
                    f"{'[正则] ' if kw['type'] == 'regex' else ''}{pattern_preview}",
                    callback_data=f"kw_del_{group_id}_{kw['_id']}"
                )
            ])

        keyboard.append([
            InlineKeyboardButton(
                "返回",
                callback_data=f"settings_keywords_{group_id}"
            )
        ])

        await query.edit_message_text(
            "选择要删除的关键词：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def get_keywords(self, group_id: int) -> list:
        """获取群组的关键词列表"""
        return await self.db.keywords.find({
            'group_id': group_id
        }).to_list(None)

    def _get_page(self, group_id: int) -> int:
        """获取当前页码"""
        return self._temp_keywords.get(f"page_{group_id}", 1)

    def _set_page(self, group_id: int, page: int):
        """设置当前页码"""
        self._temp_keywords[f"page_{group_id}"] = page

    async def _change_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, page: int):
        """切换页码"""
        self._set_page(group_id, page)
        await self.show_settings(update, context, group_id)

    async def _export_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """导出关键词"""
        query = update.callback_query
        keywords = await self.get_keywords(group_id)

        if not keywords:
            await query.edit_message_text(
                "❌ 没有可导出的关键词",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "返回",
                        callback_data=f"settings_keywords_{group_id}"
                    )
                ]])
            )
            return

        content = "类型,关键词,回复内容\n"
        for kw in keywords:
            content += f"{kw['type']},{kw['pattern']},{kw['response']}\n"

        # 创建临时文件
        filename = f"keywords_{group_id}_{context.user_data['export_count']}.csv"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)

        # 发送文件
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(filename, 'rb'),
            caption="关键词导出文件"
        )

        # 返回设置界面
        await self.show_settings(update, context, group_id)

    async def _start_import_keywords(self, update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int):
        """开始导入关键词流程"""
        query = update.callback_query
        text = (
            "请发送关键词文件，文件格式为：\n\n"
            "类型,关键词,回复内容\n"
            "exact,你好,你也好\n"
            "regex,^早安.*,早安！\n"
        )

        keyboard = [[
            InlineKeyboardButton(
                "取消",
                callback_data=f"settings_keywords_{group_id}"
            )
        ]]

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))