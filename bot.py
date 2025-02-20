import os
import signal
import asyncio
import logging
from datetime import datetime
from aiohttp import web
from telegram.ext import Application, MessageHandler, filters

from db import Database, UserRole, GroupPermission
from bot_settings import SettingsManager
from bot_keywords import KeywordManager
from bot_broadcast import BroadcastManager
from bot_stats import StatsManager

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self):
        self.db = Database()
        self.application = None
        self.web_runner = None
        self.cleanup_task = None
        self.shutdown_event = asyncio.Event()
        self.running = False
        
        # 初始化各个管理器
        self.settings_manager = SettingsManager(self.db)
        self.keyword_manager = KeywordManager(self.db)
        self.broadcast_manager = BroadcastManager(self.db, self)
        self.stats_manager = StatsManager(self.db)
        
    async def initialize(self):
        """初始化机器人"""
        logger.info("Initializing bot...")
        
        # 初始化数据库
        await self.db.init_indexes()
        
        # 确保默认超级管理员存在
        from config import DEFAULT_SUPERADMINS
        for admin_id in DEFAULT_SUPERADMINS:
            await self.db.add_user({
                'user_id': admin_id,
                'role': UserRole.SUPERADMIN.value,
                'created_at': datetime.now().isoformat(),
                'created_by': None
            })
        
        # 初始化 Telegram 应用
        self.application = (
            Application.builder()
            .token(os.getenv('TELEGRAM_TOKEN'))
            .build()
        )
        
        # 启动轮播管理器
        await self.broadcast_manager.start()
        
        # 添加处理器
        self._add_handlers()
        
        # 启动后台任务
        self.cleanup_task = asyncio.create_task(self.cleanup_old_stats())
        
        logger.info("Bot initialization completed")
        
    async def start(self):
        """启动机器人"""
        try:
            logger.info("Starting services...")
            
            # 首先启动 web 服务器
            logger.info("Starting web server...")
            await self.setup_web_server()
            
            # 然后启动机器人
            logger.info("Starting Telegram bot...")
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            self.running = True
            logger.info("All services started successfully")
            
            # 保持运行直到收到停止信号
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error during startup: {e}")
            raise
        finally:
            logger.info("Initiating shutdown...")
            await self.shutdown()

    async def stop(self):
        """停止机器人"""
        logger.info("Stop signal received")
        self.running = False

    async def shutdown(self):
        """优雅关闭所有服务"""
        if not self.running:
            return

        logger.info("Initiating shutdown sequence...")
        
        # 设置关闭事件
        self.shutdown_event.set()
        
        # 取消清理任务
        if self.cleanup_task:
            logger.info("Cancelling cleanup task...")
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        # 停止轮播管理器
        await self.broadcast_manager.stop()
        
        # 关闭 Telegram 应用
        if self.application:
            logger.info("Shutting down Telegram bot...")
            try:
                if self.application.updater:
                    await self.application.updater.stop()
                await self.application.stop()
                await self.application.shutdown()
            except Exception as e:
                logger.error(f"Error during bot shutdown: {e}")
        
        # 关闭 Web 服务器
        if self.web_runner:
            logger.info("Shutting down web server...")
            try:
                await self.web_runner.cleanup()
            except Exception as e:
                logger.error(f"Error during web server shutdown: {e}")
        
        # 关闭数据库连接
        try:
            logger.info("Closing database connection...")
            self.db.close()
        except Exception as e:
            logger.error(f"Error during database shutdown: {e}")
        
        self.running = False
        logger.info("Shutdown completed")

    async def setup_web_server(self):
        """设置Web服务器"""
        try:
            # 创建应用
            app = web.Application()
            
            # 添加路由
            async def health_check(request):
                return web.Response(text="Bot is running", status=200)
            
            app.router.add_get('/', health_check)
            
            # 设置runner
            self.web_runner = web.AppRunner(app)
            await self.web_runner.setup()
            
            # 获取端口
            from config import WEB_PORT
            
            # 创建站点并启动
            site = web.TCPSite(self.web_runner, host='0.0.0.0', port=WEB_PORT)
            await site.start()
            
            logger.info(f"Web server started successfully on port {WEB_PORT}")
        except Exception as e:
            logger.error(f"Failed to start web server: {e}")
            raise

    async def is_superadmin(self, user_id: int) -> bool:
        """检查是否是超级管理员"""
        user = await self.db.get_user(user_id)
        return user and user['role'] == UserRole.SUPERADMIN.value
        
    async def is_admin(self, user_id: int) -> bool:
        """检查是否是管理员"""
        user = await self.db.get_user(user_id)
        return user and user['role'] in {UserRole.ADMIN.value, UserRole.SUPERADMIN.value}
        
    async def has_permission(self, group_id: int, permission: GroupPermission) -> bool:
        """检查群组权限"""
        group = await self.db.get_group(group_id)
        return group and permission.value in group.get('permissions', [])

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

import logging
from typing import Dict, Any
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from utils import validate_settings, format_error_message
from db import UserRole

logger = logging.getLogger(__name__)

class SettingsManager:
    def __init__(self, db):
        self.db = db
        self._temp_settings = {}
        self._pages = {}
        
    def get_current_page(self, group_id: int, section: str) -> int:
        """获取当前页码"""
        key = f"{group_id}_{section}"
        return self._pages.get(key, 1)
        
    def set_current_page(self, group_id: int, section: str, page: int):
        """设置当前页码"""
        key = f"{group_id}_{section}"
        self._pages[key] = page
        
    def start_setting(self, user_id: int, setting_type: str, group_id: int):
        """开始设置过程"""
        key = f"{user_id}_{setting_type}"
        self._temp_settings[key] = {
            'group_id': group_id,
            'step': 1,
            'data': {}
        }
        
    def get_setting_state(self, user_id: int, setting_type: str) -> Dict[str, Any]:
        """获取设置状态"""
        key = f"{user_id}_{setting_type}"
        return self._temp_settings.get(key, {})
        
    def update_setting_state(self, user_id: int, setting_type: str, data: Dict[str, Any]):
        """更新设置状态"""
        key = f"{user_id}_{setting_type}"
        if key in self._temp_settings:
            self._temp_settings[key]['data'].update(data)
            self._temp_settings[key]['step'] += 1
            
    def clear_setting_state(self, user_id: int, setting_type: str):
        """清除设置状态"""
        key = f"{user_id}_{setting_type}"
        if key in self._temp_settings:
            del self._temp_settings[key]

    async def show_group_selection(self, update: Update, context):
        """显示群组选择界面"""
        try:
            # 获取用户可管理的群组列表
            user_groups = await self.db.get_manageable_groups(update.effective_user.id)
            
            if not user_groups:
                await update.message.reply_text("❌ 没有可管理的群组")
                return
                
            # 创建群组选择键盘
            keyboard = []
            for group in user_groups:
                try:
                    group_info = await context.bot.get_chat(group['group_id'])
                    button_text = group_info.title or f"群组 {group['group_id']}"
                    keyboard.append([
                        InlineKeyboardButton(
                            button_text,
                            callback_data=f"settings_select_{group['group_id']}"
                        )
                    ])
                except Exception as e:
                    logger.error(f"Error getting group info: {e}")
                    continue
            
            await update.message.reply_text(
                "⚙️ 机器人设置\n"
                "请选择要管理的群组：",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error showing group selection: {e}")
            raise

    async def show_settings_menu(self, update: Update, context, group_id: int):
        """显示设置菜单"""
        try:
            query = update.callback_query
            
            keyboard = [
                [
                    InlineKeyboardButton(
                        "关键词管理",
                        callback_data=f"settings_keywords_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "轮播设置", 
                        callback_data=f"settings_broadcast_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "统计设置",
                        callback_data=f"settings_stats_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "返回群组选择",
                        callback_data="settings_groups"
                    )
                ]
            ]
            
            try:
                group_info = await context.bot.get_chat(group_id)
                group_name = group_info.title or f"群组 {group_id}"
            except Exception:
                group_name = f"群组 {group_id}"
            
            await query.edit_message_text(
                f"⚙️ {group_name} 的设置\n"
                "请选择要修改的设置项：",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error showing settings menu: {e}")
            raise

    async def handle_callback(self, update: Update, context):
        """处理设置回调"""
        query = update.callback_query
        data = query.data
        parts = data.split('_')
        action = parts[1]
        
        try:
            if action == "select":
                # 处理群组选择
                group_id = int(parts[2])
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                await self.show_settings_menu(update, context, group_id)
                
            elif action == "groups":
                # 返回群组选择界面
                await self.show_group_selection(update, context)
                
            elif action in ["keywords", "broadcast", "stats"]:
                # 处理具体设置项
                group_id = int(parts[2])
                # 委托给对应的管理器处理
                if action == "keywords":
                    await context.bot.keyword_manager.show_settings(update, context, group_id)
                elif action == "broadcast":
                    await context.bot.broadcast_manager.show_settings(update, context, group_id)
                elif action == "stats":
                    await context.bot.stats_manager.show_settings(update, context, group_id)
                
        except Exception as e:
            logger.error(f"Error handling settings callback: {e}")
            await query.edit_message_text(format_error_message(e))

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