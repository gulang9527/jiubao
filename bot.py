import os
import signal
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from bson import ObjectId

from aiohttp import web
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters
)
from dotenv import load_dotenv

from db import Database, UserRole, GroupPermission
from utils import (
    validate_time_format,
    validate_interval,
    format_file_size,
    validate_regex,
    get_media_type,
    format_duration
)
from config import (
    TELEGRAM_TOKEN, 
    MONGODB_URI, 
    MONGODB_DB, 
    DEFAULT_SUPERADMINS,
    DEFAULT_SETTINGS,
    BROADCAST_SETTINGS,
    KEYWORD_SETTINGS
)

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

class BroadcastManager:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot

class StatsManager:
    def __init__(self, db):
        self.db = db

    async def add_message_stat(self, group_id: int, user_id: int, message: Message):
        """添加消息统计"""
        media_type = get_media_type(message)
        message_size = len(message.text or '') if message.text else 0
        
        if media_type and message.effective_attachment:
            try:
                file_size = getattr(message.effective_attachment, 'file_size', 0) or 0
                message_size += file_size
            except Exception:
                pass

        stat_data = {
            'group_id': group_id,
            'user_id': user_id,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_messages': 1,
            'total_size': message_size,
            'media_type': media_type
        }
        await self.db.add_message_stat(stat_data)

    async def get_daily_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取每日统计"""
        today = datetime.now().strftime('%Y-%m-%d')
        pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': today
            }},
            {'$group': {
                '_id': '$user_id',
                'total_messages': {'$sum': '$total_messages'},
                'total_size': {'$sum': '$total_size'}
            }},
            {'$sort': {'total_messages': -1}},
            {'$skip': (page - 1) * 15},
            {'$limit': 15}
        ]
        stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
        
        total_count_pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': today
            }},
            {'$group': {
                '_id': '$user_id'
            }},
            {'$count': 'total_users'}
        ]
        total_count_result = await self.db.db.message_stats.aggregate(total_count_pipeline).to_list(1)
        total_pages = (total_count_result[0]['total_users'] + 14) // 15 if total_count_result else 1
        
        return stats, total_pages

    async def get_monthly_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取月度统计"""
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': {'$gte': thirty_days_ago}
            }},
            {'$group': {
                '_id': '$user_id',
                'total_messages': {'$sum': '$total_messages'},
                'total_size': {'$sum': '$total_size'}
            }},
            {'$sort': {'total_messages': -1}},
            {'$skip': (page - 1) * 15},
            {'$limit': 15}
        ]
        stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
        
        total_count_pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': {'$gte': thirty_days_ago}
            }},
            {'$group': {
                '_id': '$user_id'
            }},
            {'$count': 'total_users'}
        ]
        total_count_result = await self.db.db.message_stats.aggregate(total_count_pipeline).to_list(1)
        total_pages = (total_count_result[0]['total_users'] + 14) // 15 if total_count_result else 1
        
        return stats, total_pages

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
        
    def get_setting_state(self, user_id: int, setting_type: str) -> dict:
        """获取设置状态"""
        key = f"{user_id}_{setting_type}"
        return self._temp_settings.get(key)
        
    def update_setting_state(self, user_id: int, setting_type: str, data: dict):
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

class KeywordManager:
    def __init__(self, db):
        self.db = db
        self._built_in_keywords = {}
        
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
                import re
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
        
    def _format_response(self, keyword: dict) -> str:
        """格式化关键词回复"""
        if keyword['response_type'] == 'text':
            return keyword['response']
        elif keyword['response_type'] in ['photo', 'video', 'document']:
            return f"__media__{keyword['response_type']}__{keyword['response']}"
        else:
            return "❌ 不支持的回复类型"
            
    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        """获取群组的关键词列表"""
        return await self.db.get_keywords(group_id)

    async def get_keyword_by_id(self, group_id: int, keyword_id: str) -> Optional[Dict[str, Any]]:
        """通过ID获取关键词"""
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            if str(kw['_id']) == keyword_id:
                return kw
        return None

class TelegramBot:
    def __init__(self):
        self.db = Database()
        self.application = None
        self.web_runner = None
        self.cleanup_task = None
        self.shutdown_event = asyncio.Event()
        self.running = False
        
        # 初始化管理器
        self.settings_manager = SettingsManager(self.db)
        self.keyword_manager = KeywordManager(self.db)
        self.broadcast_manager = BroadcastManager(self.db, self)
        self.stats_manager = StatsManager(self.db)

    async def initialize(self):
        """初始化机器人"""
        # 连接数据库
        await self.db.connect(MONGODB_URI, MONGODB_DB)
        
        # 初始化超级管理员
        for admin_id in DEFAULT_SUPERADMINS:
            user = await self.db.get_user(admin_id)
            if not user:
                await self.db.add_user({
                    'user_id': admin_id,
                    'role': UserRole.SUPERADMIN.value
                })
        
        # 创建Telegram Bot应用
        self.application = (
            Application.builder()
            .token(TELEGRAM_TOKEN)
            .build()
        )
        
        # 注册处理器
        await self._register_handlers()
        
    async def _register_handlers(self):
        """注册各种事件处理器"""
        # 命令处理器
        self.application.add_handler(CommandHandler("settings", self._handle_settings))
        
        # 消息处理器
        self.application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            self._handle_message
        ))
        
        # 回调查询处理器
        self.application.add_handler(CallbackQueryHandler(
            self._handle_settings_callback, 
            pattern=r'^settings_'
        ))
        self.application.add_handler(CallbackQueryHandler(
            self._handle_keyword_callback, 
            pattern=r'^keyword_'
        ))
        self.application.add_handler(CallbackQueryHandler(
            self._handle_keyword_response_type_callback, 
            pattern=r'^keyword_response_'
        ))
        self.application.add_handler(CallbackQueryHandler(
            self._handle_broadcast_callback, 
            pattern=r'^broadcast_'
        ))
        self.application.add_handler(CallbackQueryHandler(
            self._handle_stats_edit_callback, 
            pattern=r'^stats_'
        ))
        
    async def start(self):
        """启动机器人"""
        if not self.application:
            raise RuntimeError("Bot not initialized. Call initialize() first.")
        
        await self.application.initialize()
        await self.application.start()
        self.running = True
        
        # 启动轮播消息和清理任务
        await self._start_broadcast_task()
        await self._start_cleanup_task()
        
        # 等待关闭信号
        await self.shutdown_event.wait()
        
    async def _start_broadcast_task(self):
        """启动轮播消息任务"""
        # TODO: 实现轮播消息逻辑
        pass
    
    async def _start_cleanup_task(self):
        """启动数据清理任务"""
        # 每天清理一次旧的统计数据
        async def cleanup_routine():
            while self.running:
                try:
                    await self.db.cleanup_old_stats(
                        days=DEFAULT_SETTINGS.get('cleanup_days', 30)
                    )
                    await asyncio.sleep(24 * 60 * 60)  # 每24小时运行一次
                except Exception as e:
                    logger.error(f"Cleanup task error: {e}")
                    await asyncio.sleep(1 * 60 * 60)  # 如果出错，等待1小时后重试
        
        self.cleanup_task = asyncio.create_task(cleanup_routine())
        
    async def stop(self):
        """停止机器人"""
        self.running = False
        self.shutdown_event.set()
        
        # 停止清理任务
        if self.cleanup_task:
            self.cleanup_task.cancel()
        
        # 停止应用
        if self.application:
            await self.application.stop()
            await self.application.shutdown()
        
        # 关闭数据库连接
        self.db.close()
        
    async def shutdown(self):
        """完全关闭机器人"""
        await self.stop()

    # 以下为消息处理方法
    async def _handle_settings(self, update: Update, context):
        """处理设置命令"""
        if not update.effective_user:
            return

        try:
            # 获取用户可管理的群组
            manageable_groups = await self.db.get_manageable_groups(update.effective_user.id)
            
            if not manageable_groups:
                await update.message.reply_text("❌ 你没有权限管理任何群组")
                return
            
            # 创建群组选择键盘
            keyboard = []
            for group in manageable_groups:
                try:
                    group_info = await context.bot.get_chat(group['group_id'])
                    group_name = group_info.title or f"群组 {group['group_id']}"
                except Exception:
                    group_name = f"群组 {group['group_id']}"
                
                keyboard.append([
                    InlineKeyboardButton(
                        group_name, 
                        callback_data=f"settings_select_{group['group_id']}"
                    )
                ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "请选择要管理的群组：", 
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Error in settings command: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 处理设置命令时出错")

    async def _handle_settings_callback(self, update: Update, context):
        """处理设置回调"""
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
            
            if action == "select":
                # 处理群组选择
                group_id = int(parts[2])
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                # 显示设置菜单
                await self._show_settings_menu(query, context, group_id)
                
            elif action in ["keywords", "broadcast", "stats"]:
                # 处理具体设置项
                group_id = int(parts[2])
                await self._handle_settings_section(query, context, group_id, action)
                
        except Exception as e:
            logger.error(f"Error handling settings callback: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理设置回调时出错")

    async def _show_settings_menu(self, query, context, group_id: int):
        """显示设置菜单"""
        try:
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
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 显示设置菜单时出错")

    async def _handle_settings_section(self, query, context, group_id: int, section: str):
        """处理具体设置分区"""
        try:
            if section == "keywords":
                # 关键词管理逻辑
                keywords = await self.db.get_keywords(group_id)
                
                keyboard = []
                for kw in keywords:
                    keyword_text = kw['pattern'][:20] + '...' if len(kw['pattern']) > 20 else kw['pattern']
                    keyboard.append([
                        InlineKeyboardButton(
                            f"🔑 {keyword_text}", 
                            callback_data=f"keyword_detail_{group_id}_{kw['_id']}"
                        )
                    ])
                
                keyboard.append([
                    InlineKeyboardButton(
                        "➕ 添加关键词", 
                        callback_data=f"keyword_add_{group_id}"
                    )
                ])
                
                keyboard.append([
                    InlineKeyboardButton(
                        "返回设置菜单", 
                        callback_data=f"settings_select_{group_id}"
                    )
                ])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    f"群组 {group_id} 的关键词管理", 
                    reply_markup=reply_markup
                )
            
            elif section == "broadcast":
                # 轮播消息管理逻辑
                broadcasts = await self.db.db.broadcasts.find({
                    'group_id': group_id
                }).to_list(None)
                
                keyboard = []
                for bc in broadcasts:
                    # 截取消息预览
                    preview = (bc['content'][:20] + '...') if len(bc['content']) > 20 else bc['content']
                    keyboard.append([
                        InlineKeyboardButton(
                            f"📢 {bc['content_type']}: {preview}", 
                            callback_data=f"broadcast_detail_{group_id}_{bc['_id']}"
                        )
                    ])
                
                keyboard.append([
                    InlineKeyboardButton(
                        "➕ 添加轮播消息", 
                        callback_data=f"broadcast_add_{group_id}"
                    )
                ])
                
                keyboard.append([
                    InlineKeyboardButton(
                        "返回设置菜单", 
                        callback_data=f"settings_select_{group_id}"
                    )
                ])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    f"群组 {group_id} 的轮播消息", 
                    reply_markup=reply_markup
                )
            
            elif section == "stats":
                # 统计设置管理逻辑
                await self._handle_stats_section(query, context, group_id)
            
        except Exception as e:
            logger.error(f"Error handling settings section {section}: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text(f"❌ 处理{section}设置时出错")

    async def _handle_keyword_callback(self, update: Update, context):
        """处理关键词相关回调"""
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
            group_id = int(parts[2])
            
            if action == "add":
                # 开始添加关键词流程
                self.settings_manager.start_setting(
                    update.effective_user.id, 
                    'keyword', 
                    group_id
                )
                await query.edit_message_text("请输入关键词模式（精确匹配或正则）")
            
            elif action == "detail":
                # 显示关键词详情
                keyword_id = parts[3]
                keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "删除", 
                            callback_data=f"keyword_delete_{group_id}_{keyword_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "返回", 
                            callback_data=f"settings_keywords_{group_id}"
                        )
                    ]
                ]
                
                await query.edit_message_text(
                    f"关键词详情：\n"
                    f"模式：{keyword['pattern']}\n"
                    f"类型：{keyword['type']}\n"
                    f"响应：{keyword['response']}",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif action == "delete":
                # 删除关键词
                keyword_id = parts[3]
                await self.db.remove_keyword(group_id, keyword_id)
                
                await query.edit_message_text(
                    "关键词已删除",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "返回", 
                            callback_data=f"settings_keywords_{group_id}"
                        )
                    ]])
                )
            
        except Exception as e:
            logger.error(f"Error handling keyword callback: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理关键词回调时出错")

    async def _handle_keyword_response_type_callback(self, update: Update, context):
        """处理关键词响应类型选择"""
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            response_type = data.split('_')[-1]
            
            # 获取当前关键词添加流程的状态
            user_id = update.effective_user.id
            setting_state = self.settings_manager.get_setting_state(user_id, 'keyword')
            
            if not setting_state:
                await query.edit_message_text("❌ 关键词添加流程已过期，请重新开始")
                return
            
            # 更新响应类型
            setting_state['data']['response_type'] = response_type
            
            # 根据响应类型引导用户
            if response_type == 'text':
                await query.edit_message_text("请输入关键词的文本响应：")
            elif response_type in ['photo', 'video', 'document']:
                await query.edit_message_text(f"请发送或上传要作为响应的{response_type}")
            
            # 推进设置流程
            self.settings_manager.update_setting_state(user_id, 'keyword', setting_state['data'])
            
        except Exception as e:
            logger.error(f"Error handling keyword response type: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理关键词响应类型时出错")

    async def _handle_broadcast_callback(self, update: Update, context):
        """处理轮播消息回调"""
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
            group_id = int(parts[2])
            
            if action == "add":
                # 开始添加轮播消息流程
                self.settings_manager.start_setting(
                    update.effective_user.id, 
                    'broadcast', 
                    group_id
                )
                keyboard = [
                    [
                        InlineKeyboardButton("文本", callback_data=f"broadcast_type_text_{group_id}"),
                        InlineKeyboardButton("图片", callback_data=f"broadcast_type_photo_{group_id}")
                    ],
                    [
                        InlineKeyboardButton("视频", callback_data=f"broadcast_type_video_{group_id}"),
                        InlineKeyboardButton("文件", callback_data=f"broadcast_type_document_{group_id}")
                    ]
                ]
                await query.edit_message_text(
                    "请选择轮播消息类型：", 
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif action == "type":
                # 选择轮播消息类型
                content_type = parts[2]
                self.settings_manager.update_setting_state(
                    update.effective_user.id, 
                    'broadcast', 
                    {'content_type': content_type}
                )
                await query.edit_message_text(f"请输入{content_type}类型的内容：")
            
            elif action == "detail":
                # 显示轮播消息详情
                broadcast_id = parts[3]
                broadcast = await self.db.db.broadcasts.find_one({
                    '_id': ObjectId(broadcast_id)
                })
                
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "删除", 
                            callback_data=f"broadcast_delete_{group_id}_{broadcast_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "返回", 
                            callback_data=f"settings_broadcast_{group_id}"
                        )
                    ]
                ]
                
                await query.edit_message_text(
                    f"轮播消息详情：\n"
                    f"类型：{broadcast['content_type']}\n"
                    f"内容：{broadcast['content']}\n"
                    f"开始时间：{broadcast['start_time']}\n"
                    f"结束时间：{broadcast['end_time']}\n"
                    f"间隔：{broadcast['interval']}秒",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif action == "delete":
                # 删除轮播消息
                broadcast_id = parts[3]
                await self.db.db.broadcasts.delete_one({
                    '_id': ObjectId(broadcast_id)
                })
                
                await query.edit_message_text(
                    "轮播消息已删除",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "返回", 
                            callback_data=f"settings_broadcast_{group_id}"
                        )
                    ]])
                )
            
        except Exception as e:
            logger.error(f"Error handling broadcast callback: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理轮播消息回调时出错")

    async def _handle_stats_edit_callback(self, update: Update, context):
        """处理统计设置编辑"""
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            action = parts[2]
            group_id = int(parts[3])
            
            # 获取当前设置
            settings = await self.db.get_group_settings(group_id)
            
            if action == "min_bytes":
                # 编辑最小统计字节数
                await query.edit_message_text("请输入最小统计字节数：")
                self.settings_manager.start_setting(
                    update.effective_user.id, 
                    'stats_min_bytes', 
                    group_id
                )
            
            elif action == "toggle_media":
                # 切换是否统计多媒体
                current = settings.get('count_media', False)
                settings['count_media'] = not current
                await self.db.update_group_settings(group_id, settings)
                
                await self._handle_stats_section(query, context, group_id)
            
            elif action == "daily_rank":
                # 编辑日排行显示数量
                await query.edit_message_text("请输入日排行显示数量：")
                self.settings_manager.start_setting(
                    update.effective_user.id, 
                    'stats_daily_rank', 
                    group_id
                )
            
            elif action == "monthly_rank":
                # 编辑月排行显示数量
                await query.edit_message_text("请输入月排行显示数量：")
                self.settings_manager.start_setting(
                    update.effective_user.id, 
                    'stats_monthly_rank', 
                    group_id
                )
            
        except Exception as e:
            logger.error(f"Error handling stats edit callback: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理统计设置编辑时出错")

    async def _handle_stats_section(self, query, context, group_id: int):
        """处理统计设置"""
        try:
            # 获取当前群组的统计设置
            settings = await self.db.get_group_settings(group_id)
            
            # 创建设置展示和修改的键盘
            keyboard = [
                [
                    InlineKeyboardButton(
                        f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", 
                        callback_data=f"stats_edit_min_bytes_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"统计多媒体: {'是' if settings.get('count_media', False) else '否'}", 
                        callback_data=f"stats_toggle_media_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"日排行显示数量: {settings.get('daily_rank_size', 15)}", 
                        callback_data=f"stats_edit_daily_rank_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"月排行显示数量: {settings.get('monthly_rank_size', 15)}", 
                        callback_data=f"stats_edit_monthly_rank_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "返回设置菜单", 
                        callback_data=f"settings_select_{group_id}"
                    )
                ]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                f"群组 {group_id} 的统计设置", 
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Error handling stats settings: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理统计设置时出错")

    async def _handle_message(self, update: Update, context):
        """处理消息，包括关键词添加流程和多媒体关键词响应"""
        if not update.effective_chat or not update.effective_user or not update.message:
            return
        
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        try:
            # 检查是否正在进行关键词添加流程
            setting_state = self.settings_manager.get_setting_state(user_id, 'keyword')
            if setting_state and setting_state['group_id'] == chat_id:
                await self._process_keyword_adding(update, context, setting_state)
                return
            
            # 检查是否正在进行轮播消息添加流程
            broadcast_state = self.settings_manager.get_setting_state(user_id, 'broadcast')
            if broadcast_state and broadcast_state['group_id'] == chat_id:
                await self._process_broadcast_adding(update, context, broadcast_state)
                return
            
            # 检查是否正在进行统计设置编辑
            for setting_type in ['stats_min_bytes', 'stats_daily_rank', 'stats_monthly_rank']:
                stats_state = self.settings_manager.get_setting_state(user_id, setting_type)
                if stats_state and stats_state['group_id'] == chat_id:
                    await self._process_stats_setting(update, context, stats_state, setting_type)
                    return
            
            # 处理关键词匹配
            if await self.has_permission(chat_id, GroupPermission.KEYWORDS):
                if update.message.text:
                    # 尝试匹配关键词
                    response = await self.keyword_manager.match_keyword(
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
            if await self.has_permission(chat_id, GroupPermission.STATS):
                await self.stats_manager.add_message_stat(chat_id, user_id, update.message)
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            logger.error(traceback.format_exc())

    async def _process_keyword_adding(self, update: Update, context, setting_state):
        """处理关键词添加流程的各个步骤"""
        try:
            step = setting_state['step']
            group_id = setting_state['group_id']
            
            if step == 1:
                # 获取关键词模式
                pattern = update.message.text
                
                # 验证关键词模式
                if len(pattern) > KEYWORD_SETTINGS['max_pattern_length']:
                    await update.message.reply_text(f"❌ 关键词过长，请不要超过 {KEYWORD_SETTINGS['max_pattern_length']} 个字符")
                    return
                
                setting_state['data']['pattern'] = pattern
                setting_state['data']['type'] = 'regex' if validate_regex(pattern) else 'exact'
                
                # 询问关键词响应类型
                keyboard = [
                    [
                        InlineKeyboardButton("文本", callback_data="keyword_response_text"),
                        InlineKeyboardButton("图片", callback_data="keyword_response_photo"),
                        InlineKeyboardButton("视频", callback_data="keyword_response_video"),
                        InlineKeyboardButton("文件", callback_data="keyword_response_document")
                    ]
                ]
                
                await update.message.reply_text(
                    "请选择关键词响应的类型：", 
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif step == 2:
                # 获取关键词响应
                response_type = setting_state['data'].get('response_type')
                
                if response_type == 'text':
                    response = update.message.text
                    if len(response) > KEYWORD_SETTINGS['max_response_length']:
                        await update.message.reply_text(f"❌ 响应内容过长，请不要超过 {KEYWORD_SETTINGS['max_response_length']} 个字符")
                        return
                    file_id = response
                elif response_type in ['photo', 'video', 'document']:
                    media_methods = {
                        'photo': lambda m: m.photo[-1].file_id if m.photo else None,
                        'video': lambda m: m.video.file_id if m.video else None,
                        'document': lambda m: m.document.file_id if m.document else None
                    }
                    
                    file_id = media_methods[response_type](update.message)
                    
                    if not file_id:
                        await update.message.reply_text(f"❌ 请发送一个{response_type}")
                        return
                else:
                    await update.message.reply_text("❌ 未知的响应类型")
                    return
                
                # 检查关键词数量是否超过限制
                keywords = await self.db.get_keywords(group_id)
                if len(keywords) >= KEYWORD_SETTINGS['max_keywords']:
                    await update.message.reply_text(f"❌ 关键词数量已达到上限 {KEYWORD_SETTINGS['max_keywords']} 个")
                    return
                
                # 添加关键词
                await self.db.add_keyword({
                    'group_id': group_id,
                    'pattern': setting_state['data']['pattern'],
                    'type': setting_state['data']['type'],
                    'response': file_id,
                    'response_type': response_type
                })
                
                await update.message.reply_text("✅ 关键词添加成功！")
                
                # 清除设置状态
                self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
        
        except Exception as e:
            logger.error(f"Error processing keyword adding: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 添加关键词时出错")

    async def _process_broadcast_adding(self, update: Update, context, setting_state):
        """处理轮播消息添加流程"""
        try:
            step = setting_state['step']
            group_id = setting_state['group_id']
            content_type = setting_state['data'].get('content_type')
            
            if step == 1:
                # 获取内容
                if content_type == 'text':
                    content = update.message.text
                elif content_type == 'photo':
                    if not update.message.photo:
                        await update.message.reply_text("❌ 请发送图片")
                        return
                    content = update.message.photo[-1].file_id
                elif content_type == 'video':
                    if not update.message.video:
                        await update.message.reply_text("❌ 请发送视频")
                        return
                    content = update.message.video.file_id
                elif content_type == 'document':
                    if not update.message.document:
                        await update.message.reply_text("❌ 请发送文件")
                        return
                    content = update.message.document.file_id
                else:
                    await update.message.reply_text("❌ 未知的内容类型")
                    return
                
                setting_state['data']['content'] = content
                
                # 询问开始时间
                await update.message.reply_text("请输入轮播开始时间（格式：YYYY-MM-DD HH:MM）：")
            
            elif step == 2:
                # 获取开始时间
                start_time_str = update.message.text
                start_time = validate_time_format(start_time_str)
                
                if not start_time:
                    await update.message.reply_text("❌ 时间格式错误，请使用 YYYY-MM-DD HH:MM 格式")
                    return
                
                setting_state['data']['start_time'] = start_time.isoformat()
                
                # 询问结束时间
                await update.message.reply_text("请输入轮播结束时间（格式：YYYY-MM-DD HH:MM）：")
            
            elif step == 3:
                # 获取结束时间
                end_time_str = update.message.text
                end_time = validate_time_format(end_time_str)
                
                if not end_time:
                    await update.message.reply_text("❌ 时间格式错误，请使用 YYYY-MM-DD HH:MM 格式")
                    return
                
                setting_state['data']['end_time'] = end_time.isoformat()
                
                # 询问轮播间隔（秒）
                await update.message.reply_text("请输入轮播间隔（秒）：")
            
            elif step == 4:
                # 获取轮播间隔
                interval_str = update.message.text
                interval = validate_interval(interval_str)
                
                if not interval:
                    await update.message.reply_text("❌ 间隔时间必须是正整数")
                    return
                
                # 检查轮播消息数量是否超过限制
                broadcasts = await self.db.db.broadcasts.find({
                    'group_id': group_id
                }).to_list(None)
                if len(broadcasts) >= BROADCAST_SETTINGS['max_broadcasts']:
                    await update.message.reply_text(f"❌ 轮播消息数量已达到上限 {BROADCAST_SETTINGS['max_broadcasts']} 个")
                    return
                
                # 检查间隔是否符合最小要求
                if interval < BROADCAST_SETTINGS['min_interval']:
                    await update.message.reply_text(f"❌ 轮播间隔不能小于 {BROADCAST_SETTINGS['min_interval']} 秒")
                    return
                
                # 添加轮播消息
                await self.db.db.broadcasts.insert_one({
                    'group_id': group_id,
                    'content_type': setting_state['data']['content_type'],
                    'content': setting_state['data']['content'],
                    'start_time': setting_state['data']['start_time'],
                    'end_time': setting_state['data']['end_time'],
                    'interval': interval,
                    'last_broadcast': None
                })
                
                await update.message.reply_text("✅ 轮播消息添加成功！")
                
                # 清除设置状态
                self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')
        
        except Exception as e:
            logger.error(f"Error processing broadcast adding: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 添加轮播消息时出错")

    async def _process_stats_setting(self, update: Update, context, setting_state, setting_type):
        """处理统计设置编辑"""
        try:
            group_id = setting_state['group_id']
            
            # 获取用户输入的值
            try:
                value = int(update.message.text)
                if value < 0:
                    raise ValueError
            except ValueError:
                await update.message.reply_text("❌ 请输入一个有效的正整数")
                return
            
            # 获取当前设置
            settings = await self.db.get_group_settings(group_id)
            
            # 根据不同的设置类型更新配置
            if setting_type == 'stats_min_bytes':
                settings['min_bytes'] = value
                tips = f"最小统计字节数已设置为 {value} 字节"
            elif setting_type == 'stats_daily_rank':
                settings['daily_rank_size'] = value
                tips = f"日排行显示数量已设置为 {value}"
            elif setting_type == 'stats_monthly_rank':
                settings['monthly_rank_size'] = value
                tips = f"月排行显示数量已设置为 {value}"
            
            # 更新群组设置
            await self.db.update_group_settings(group_id, settings)
            
            # 发送成功提示
            await update.message.reply_text(f"✅ {tips}")
            
            # 清除设置状态
            self.settings_manager.clear_setting_state(update.effective_user.id, setting_type)
        
        except Exception as e:
            logger.error(f"Error processing stats setting: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 处理统计设置时出错")

    async def _handle_rank_command(self, update: Update, context):
        """处理统计命令（tongji/tongji30）"""
        if not update.effective_chat or not update.effective_user or not update.message:
            return
            
        try:
            command = update.message.text.split('@')[0][1:]  # 移除 / 和机器人用户名
            group_id = update.effective_chat.id
            
            # 检查权限
            if not await self.has_permission(group_id, GroupPermission.STATS):
                await update.message.reply_text("❌ 此群组未启用统计功能")
                return
                
            # 获取页码
            page = 1
            if context.args:
                try:
                    page = int(context.args[0])
                    if page < 1:
                        raise ValueError
                except ValueError:
                    await update.message.reply_text("❌ 无效的页码")
                    return

            # 获取统计数据
            if command == "tongji":
                stats, total_pages = await self.stats_manager.get_daily_stats(group_id, page)
                title = "📊 今日发言排行"
            else:  # tongji30
                stats, total_pages = await self.stats_manager.get_monthly_stats(group_id, page)
                title = "📊 近30天发言排行"
                
            if not stats:
                await update.message.reply_text("📊 暂无统计数据")
                return
                
            # 生成排行榜文本
            text = f"{title}\n\n"
            settings = await self.db.get_group_settings(group_id)
            min_bytes = settings.get('min_bytes', 0)
            
            for i, stat in enumerate(stats, start=(page-1)*15+1):
                try:
                    user = await context.bot.get_chat_member(group_id, stat['_id'])
                    name = user.user.full_name or user.user.username or f"用户{stat['_id']}"
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
            
            keyboard = self._create_navigation_keyboard(
                page, 
                total_pages, 
                f"{'today' if command == 'tongji' else 'monthly'}_{group_id}"
            )
            
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )
            
        except Exception as e:
            logger.error(f"Error handling rank command: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 获取排行榜时出错")

    async def _handle_rank_callback(self, update: Update, context):
        """处理排行榜分页回调"""
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            page = int(parts[2])
            
            # 获取群组ID和统计类型（今日或30天）
            group_id = int(parts[1])
            
            # 获取统计数据
            if parts[0] == "today":
                stats, total_pages = await self.stats_manager.get_daily_stats(group_id, page)
                title = "📊 今日发言排行"
                callback_base = f"today_{group_id}"
            else:  # monthly
                stats, total_pages = await self.stats_manager.get_monthly_stats(group_id, page)
                title = "📊 近30天发言排行"
                callback_base = f"monthly_{group_id}"
            
            # 生成排行榜文本
            text = f"{title}\n\n"
            settings = await self.db.get_group_settings(group_id)
            min_bytes = settings.get('min_bytes', 0)
            
            for i, stat in enumerate(stats, start=(page-1)*15+1):
                try:
                    user = await context.bot.get_chat_member(group_id, stat['_id'])
                    name = user.user.full_name or user.user.username or f"用户{stat['_id']}"
                except Exception:
                    name = f"用户{stat['_id']}"
                
                text += f"{i}. {name}\n"
                text += f"   消息数: {stat['total_messages']}\n"
                text += f"   总字节: {format_file_size(stat['total_size'])}\n\n"
            
            if min_bytes > 0:
                text += f"\n注：仅统计大于 {format_file_size(min_bytes)} 的消息"
            
            # 添加分页信息
            text += f"\n\n第 {page}/{total_pages} 页"
            
            # 创建导航键盘
            keyboard = self._create_navigation_keyboard(
                page, 
                total_pages, 
                callback_base
            )
            
            # 更新消息
            await query.edit_message_text(
                text, 
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )
            
        except Exception as e:
            logger.error(f"Error handling rank callback: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理排行榜回调时出错")

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
        
    def _create_navigation_keyboard(
        self,
        current_page: int,
        total_pages: int,
        base_callback: str
    ) -> List[List[InlineKeyboardButton]]:
        """创建分页导航键盘"""
        keyboard = []
        nav_row = []
        
        if current_page > 1:
            nav_row.append(
                InlineKeyboardButton(
                    "◀️ 上一页",
                    callback_data=f"{base_callback}_{current_page-1}"
                )
            )
            
        if current_page < total_pages:
            nav_row.append(
                InlineKeyboardButton(
                    "下一页 ▶️",
                    callback_data=f"{base_callback}_{current_page+1}"
                )
            )
            
        if nav_row:
            keyboard.append(nav_row)
            
        return keyboard

async def handle_signals(bot):
    """处理系统信号"""
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(
                sig,
                lambda: asyncio.create_task(bot.stop())
            )
        logger.info("Signal handlers set up")
    except NotImplementedError:
        # Windows 不支持 add_signal_handler
        logger.warning("Signal handlers not supported on this platform")

async def main():
    """主函数"""
    bot = None
    try:
        # 创建机器人实例
        bot = TelegramBot()
               
        # 初始化
        await bot.initialize()
        
        # 设置信号处理
        await handle_signals(bot)
        
        # 启动机器人
        await bot.start()
        
    except Exception as e:
        logger.error(f"Bot startup failed: {e}")
        logger.error(traceback.format_exc())
        if bot:
            await bot.stop()
        raise
    finally:
        if bot and bot.running:
            await bot.shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot stopped due to error: {e}")
        logger.error(traceback.format_exc())
        raise
