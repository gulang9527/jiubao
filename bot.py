import os
import json
import signal
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
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
    format_duration,
    validate_delete_timeout,
    is_auto_delete_exempt,
    get_message_metadata,
    parse_command_args,
    escape_markdown
)
from config import (
    TELEGRAM_TOKEN, 
    MONGODB_URI, 
    MONGODB_DB, 
    DEFAULT_SUPERADMINS,
    DEFAULT_SETTINGS,
    BROADCAST_SETTINGS,
    KEYWORD_SETTINGS,
    AUTO_DELETE_SETTINGS,
    WEB_HOST,
    WEB_PORT
)

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

class SettingsManager:
    def __init__(self, db):
        self.db = db
        self._temp_settings = {}
        self._pages = {}
        
    def get_current_page(self, group_id: int, section: str) -> int:
        key = f"{group_id}_{section}"
        return self._pages.get(key, 1)
        
    def set_current_page(self, group_id: int, section: str, page: int):
        key = f"{group_id}_{section}"
        self._pages[key] = page
        
    def start_setting(self, user_id: int, setting_type: str, group_id: int):
        key = f"{user_id}_{setting_type}"
        self._temp_settings[key] = {
            'group_id': group_id,
            'step': 1,
            'data': {}
        }
        
    def get_setting_state(self, user_id: int, setting_type: str) -> dict:
        key = f"{user_id}_{setting_type}"
        return self._temp_settings.get(key)
        
    def update_setting_state(self, user_id: int, setting_type: str, data: dict):
        key = f"{user_id}_{setting_type}"
        if key in self._temp_settings:
            self._temp_settings[key]['data'].update(data)
            self._temp_settings[key]['step'] += 1
            
    def clear_setting_state(self, user_id: int, setting_type: str):
        key = f"{user_id}_{setting_type}"
        if key in self._temp_settings:
            del self._temp_settings[key]

class StatsManager:
    def __init__(self, db):
        self.db = db

    async def add_message_stat(self, group_id: int, user_id: int, message: Message):
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

class BroadcastManager:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot

class KeywordManager:
    def __init__(self, db):
        self.db = db
        self._built_in_keywords = {}
        
    def register_built_in_keyword(self, pattern: str, handler: callable):
        self._built_in_keywords[pattern] = handler
        
    async def match_keyword(self, group_id: int, text: str, message: Message) -> Optional[str]:
        for pattern, handler in self._built_in_keywords.items():
            if text == pattern:
                return await handler(message)
        
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            try:
                import re
                if kw['type'] == 'regex':
                    pattern = re.compile(kw['pattern'])
                    if pattern.search(text):
                        return self._format_response(kw)
                else:
                    if text == kw['pattern']:
                        return self._format_response(kw)
            except Exception as e:
                logger.error(f"Error matching keyword {kw['pattern']}: {e}")
                continue
        
        return None
        
    def _format_response(self, keyword: dict) -> str:
        if keyword['response_type'] == 'text':
            return keyword['response']
        elif keyword['response_type'] in ['photo', 'video', 'document']:
            return f"__media__{keyword['response_type']}__{keyword['response']}"
        else:
            return "❌ 不支持的回复类型"
            
    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        return await self.db.get_keywords(group_id)

    async def get_keyword_by_id(self, group_id: int, keyword_id: str) -> Optional[Dict[str, Any]]:
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            if str(kw['_id']) == keyword_id:
                return kw
        return None

class TelegramBot:
    class MessageDeletionManager:
        def __init__(self, bot):
            self.bot = bot
            self.deletion_tasks = {}
        
        async def schedule_message_deletion(
            self, 
            message: Message, 
            timeout: int, 
            delete_original: bool = False
        ):
            if timeout <= 0:
                return
            
            task_key = f"delete_message_{message.chat.id}_{message.message_id}"
            
            async def delete_message_task():
                try:
                    await asyncio.sleep(timeout)
                    
                    if delete_original and message.reply_to_message:
                        await message.reply_to_message.delete()
                    
                    await message.delete()
                except Exception as e:
                    logger.warning(f"Error in message deletion: {e}")
                finally:
                    if task_key in self.deletion_tasks:
                        del self.deletion_tasks[task_key]
            
            task = asyncio.create_task(delete_message_task(), name=task_key)
            self.deletion_tasks[task_key] = task
        
        def cancel_deletion_task(self, message: Message):
            task_key = f"delete_message_{message.chat.id}_{message.message_id}"
            if task_key in self.deletion_tasks:
                task = self.deletion_tasks[task_key]
                task.cancel()
                del self.deletion_tasks[task_key]

    def __init__(self):
        self.db = Database()
        self.application = None
        self.web_app = None
        self.web_runner = None
        self.cleanup_task = None
        self.shutdown_event = asyncio.Event()
        self.running = False
        
        self.settings_manager = SettingsManager(self.db)
        self.keyword_manager = KeywordManager(self.db)
        self.broadcast_manager = BroadcastManager(self.db, self)
        self.stats_manager = StatsManager(self.db)
        self.message_deletion_manager = self.MessageDeletionManager(self)

    async def initialize(self):
        try:
            logger.info("开始初始化机器人")
        
            await self.db.connect(MONGODB_URI, MONGODB_DB)
            
            for admin_id in DEFAULT_SUPERADMINS:
                user = await self.db.get_user(admin_id)
                if not user:
                    await self.db.add_user({
                        'user_id': admin_id,
                        'role': UserRole.SUPERADMIN.value
                    })
            
            webhook_domain = os.getenv('WEBHOOK_DOMAIN')
            if not webhook_domain:
                logger.warning("WEBHOOK_DOMAIN环境变量未设置。使用默认值。")
                webhook_domain = 'your-render-app-name.onrender.com'
            
            self.application = (
                Application.builder()
                .token(TELEGRAM_TOKEN)
                .build()
            )
            
            await self._register_handlers()
            
            await self.setup_web_server()
            
            webhook_url = f"https://{webhook_domain}/webhook/{TELEGRAM_TOKEN}"
            webhook_path = f"/webhook/{TELEGRAM_TOKEN}"
            
            await self.application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
            
            self.application.updater = None
            self.web_app.router.add_post(webhook_path, self._handle_webhook)
            
            logger.info(f"Webhook已设置为 {webhook_url}")
            logger.info(f"处理器数量: {len(self.application.handlers.get(0, []))}")
        
            return True
        except Exception as e:
            logger.error(f"机器人初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False

    async def setup_web_server(self):
        self.web_app = web.Application()
        self.web_app.router.add_get('/', self.handle_healthcheck)
        self.web_app.router.add_get('/health', self.handle_healthcheck)
        
        self.web_runner = web.AppRunner(self.web_app)
        await self.web_runner.setup()
        
        site = web.TCPSite(self.web_runner, WEB_HOST, WEB_PORT)
        await site.start()
        logger.info(f"Web服务器已在 {WEB_HOST}:{WEB_PORT} 启动")

    async def handle_healthcheck(self, request):
        return web.Response(text="Healthy", status=200)

    async def _handle_webhook(self, request):
        try:
            update_data = await request.json()
            update = Update.de_json(update_data, self.application.bot)
            
            if update:
                await self.application.process_update(update)
            else:
                logger.warning("收到无效的更新")
            
            return web.Response(status=200)
        
        except json.JSONDecodeError:
            logger.error("Webhook请求中的JSON无效")
            return web.Response(status=400)
        
        except Exception as e:
            logger.error(f"处理Webhook时出错: {e}")
            logger.error(traceback.format_exc())
            return web.Response(status=500)

    async def _webhook_handler(self):
        async def webhook_callback(update, context):
            try:
                return web.Response(text="ok")
            except Exception as e:
                logger.error(f"Webhook处理器错误: {e}")
                return web.Response(status=500)
        return webhook_callback

    async def start(self):
        if not self.application:
            logger.error("机器人未初始化。初始化失败。")
            return False
        
        try:
            await self.application.initialize()
            await self.application.start()
            self.running = True
            
            await self._start_broadcast_task()
            await self._start_cleanup_task()
            
            logger.info("机器人成功启动")
            return True
        
        except Exception as e:
            logger.error(f"机器人启动失败: {e}")
            logger.error(traceback.format_exc())
            return False

    async def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("tongji", self._handle_rank_command))
        self.application.add_handler(CommandHandler("tongji30", self._handle_rank_command))
        
        self.application.add_handler(CommandHandler("settings", self._handle_settings))
        self.application.add_handler(CommandHandler("admingroups", self._handle_admin_groups))
        
        self.application.add_handler(CommandHandler("addsuperadmin", self._handle_add_superadmin))
        self.application.add_handler(CommandHandler("delsuperadmin", self._handle_del_superadmin))
        self.application.add_handler(CommandHandler("addadmin", self._handle_add_admin))
        self.application.add_handler(CommandHandler("deladmin", self._handle_del_admin))
        self.application.add_handler(CommandHandler("authgroup", self._handle_auth_group))
        self.application.add_handler(CommandHandler("deauthgroup", self._handle_deauth_group))
        
        self.application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            self._handle_message
        ))
        
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

    async def _handle_message(self, update: Update, context):
        if not update.effective_chat or not update.effective_user or not update.message:
            return
        
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        try:
            setting_state = self.settings_manager.get_setting_state(user_id, 'keyword')
            if setting_state and setting_state['group_id'] == chat_id:
                await self._process_keyword_adding(update, context, setting_state)
                return
            
            broadcast_state = self.settings_manager.get_setting_state(user_id, 'broadcast')
            if broadcast_state and broadcast_state['group_id'] == chat_id:
                await self._process_broadcast_adding(update, context, broadcast_state)
                return
            
            for setting_type in ['stats_min_bytes', 'stats_daily_rank', 'stats_monthly_rank']:
                stats_state = self.settings_manager.get_setting_state(user_id, setting_type)
                if stats_state and stats_state['group_id'] == chat_id:
                    await self._process_stats_setting(update, context, stats_state, setting_type)
                    return
            
            if await self.has_permission(chat_id, GroupPermission.KEYWORDS):
                if update.message.text:
                    response = await self.keyword_manager.match_keyword(
                        chat_id,
                        update.message.text,
                        update.message
                    )
                    if response:
                        await self._handle_keyword_response(chat_id, response, context, update.message)
            
            if await self.has_permission(chat_id, GroupPermission.STATS):
                await self.stats_manager.add_message_stat(chat_id, user_id, update.message)
                
        except Exception as e:
            logger.error(f"处理消息错误: {e}")
            logger.error(traceback.format_exc())

    async def _handle_keyword_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
            
            if action == "add":
                group_id = int(parts[2])
                
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
                    await query.edit_message_text("❌ 此群组未启用关键词功能")
                    return
                    
                self.settings_manager.start_setting(
                    update.effective_user.id,
                    'keyword',
                    group_id
                )
                
                await query.edit_message_text(
                    "请发送要添加的关键词模式：\n"
                    "- 支持正则表达式\n"
                    "- 最大长度100字符\n"
                    "- 发送 /cancel 取消"
                )
                
            elif action == "detail":
                group_id = int(parts[2])
                keyword_id = parts[3]
                
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                if not keyword:
                    await query.edit_message_text("❌ 关键词不存在")
                    return
                    
                keyboard = [[
                    InlineKeyboardButton(
                        "🗑️ 删除关键词",
                        callback_data=f"keyword_delete_{group_id}_{keyword_id}"
                    )
                ], [
                    InlineKeyboardButton(
                        "返回关键词列表",
                        callback_data=f"settings_keywords_{group_id}"
                    )
                ]]
                
                text = "📝 关键词详情：\n\n"
                text += f"模式：{keyword['pattern']}\n"
                text += f"类型：{'正则表达式' if keyword['type'] == 'regex' else '精确匹配'}\n"
                text += f"响应类型：{keyword['response_type']}"
                
                await query.edit_message_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
            elif action == "delete":
                group_id = int(parts[2])
                keyword_id = parts[3]
                
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                await self.db.remove_keyword(group_id, keyword_id)
                
                await self._handle_settings_section(
                    query,
                    context,
                    group_id,
                    "keywords"
                )
                
        except Exception as e:
            logger.error(f"处理关键词回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理关键词操作时出错")

    async def _handle_keyword_response_type_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            response_type = data.split('_')[-1]
            
            setting_state = self.settings_manager.get_setting_state(
                update.effective_user.id,
                'keyword'
            )
            
            if not setting_state:
                await query.edit_message_text("❌ 设置会话已过期，请重新开始")
                return
                
            setting_state['data']['response_type'] = response_type
            
            if response_type == 'text':
                prompt = "请发送关键词的文本回复内容："
            elif response_type == 'photo':
                prompt = "请发送关键词要回复的图片："
            elif response_type == 'video':
                prompt = "请发送关键词要回复的视频："
            elif response_type == 'document':
                prompt = "请发送关键词要回复的文件："
            else:
                await query.edit_message_text("❌ 不支持的响应类型")
                return
                
            await query.edit_message_text(
                f"{prompt}\n"
                "发送 /cancel 取消"
            )
            
            self.settings_manager.update_setting_state(
                update.effective_user.id,
                'keyword',
                {'response_type': response_type}
            )
            
        except Exception as e:
            logger.error(f"处理关键词响应类型回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理响应类型选择时出错")

    async def _handle_broadcast_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
            group_id = int(parts[2])
            
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return
                
            if not await self.has_permission(group_id, GroupPermission.BROADCAST):
                await query.edit_message_text("❌ 此群组未启用轮播功能")
                return
                
            if action == "add":
                keyboard = [
                    [
                        InlineKeyboardButton("文本", callback_data=f"broadcast_type_text_{group_id}"),
                        InlineKeyboardButton("图片", callback_data=f"broadcast_type_photo_{group_id}"),
                        InlineKeyboardButton("视频", callback_data=f"broadcast_type_video_{group_id}"),
                        InlineKeyboardButton("文件", callback_data=f"broadcast_type_document_{group_id}")
                    ],
                    [
                        InlineKeyboardButton("返回", callback_data=f"settings_broadcast_{group_id}")
                    ]
                ]
                
                await query.edit_message_text(
                    "请选择轮播消息类型：",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
            elif action == "type":
                content_type = parts[2]
                self.settings_manager.start_setting(
                    update.effective_user.id,
                    'broadcast',
                    group_id
                )
                self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'broadcast',
                    {'content_type': content_type}
                )
                
                if content_type == 'text':
                    prompt = "请发送轮播消息的文本内容："
                elif content_type == 'photo':
                    prompt = "请发送要轮播的图片："
                elif content_type == 'video':
                    prompt = "请发送要轮播的视频："
                elif content_type == 'document':
                    prompt = "请发送要轮播的文件："
                else:
                    await query.edit_message_text("❌ 不支持的消息类型")
                    return
                    
                await query.edit_message_text(
                    f"{prompt}\n"
                    "发送 /cancel 取消"
                )
                
            elif action == "detail":
                broadcast_id = ObjectId(parts[3])
                broadcast = await self.db.db.broadcasts.find_one({
                    '_id': broadcast_id,
                    'group_id': group_id
                })
                
                if not broadcast:
                    await query.edit_message_text("❌ 轮播消息不存在")
                    return
                    
                keyboard = [[
                    InlineKeyboardButton(
                        "🗑️ 删除",
                        callback_data=f"broadcast_delete_{group_id}_{broadcast_id}"
                    )
                ], [
                    InlineKeyboardButton(
                        "返回列表",
                        callback_data=f"settings_broadcast_{group_id}"
                    )
                ]]
                
                text = "📢 轮播消息详情：\n\n"
                text += f"类型：{broadcast['content_type']}\n"
                text += f"开始时间：{broadcast['start_time']}\n"
                text += f"结束时间：{broadcast['end_time']}\n"
                text += f"间隔：{format_duration(broadcast['interval'])}"
                
                await query.edit_message_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
            elif action == "delete":
                broadcast_id = ObjectId(parts[3])
                await self.db.db.broadcasts.delete_one({
                    '_id': broadcast_id,
                    'group_id': group_id
                })
                
                await self._handle_settings_section(query, context, group_id, "broadcast")
                
        except Exception as e:
            logger.error(f"处理轮播消息回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理轮播消息操作时出错")

    async def _handle_settings_section(self, query, context, group_id: int, section: str):
        try:
            if section == "stats":
                settings = await self.db.get_group_settings(group_id)
                
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
                            callback_data=f"stats_edit_toggle_media_{group_id}"
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
                
            elif section == "broadcast":
                broadcasts = await self.db.db.broadcasts.find({
                    'group_id': group_id
                }).to_list(None)
                
                keyboard = []
                for bc in broadcasts:
                    preview = (bc['content'][:20] + '...') if len(str(bc['content'])) > 20 else str(bc['content'])
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
                
                await query.edit_message_text(
                    f"群组 {group_id} 的轮播消息设置",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
            elif section == "keywords":
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
                
                await query.edit_message_text(
                    f"群组 {group_id} 的关键词设置",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
        except Exception as e:
            logger.error(f"处理设置分区显示错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 显示设置分区时出错")

    async def _handle_stats_edit_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            setting_type = parts[2]
            group_id = int(parts[3])
            
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return
                
            if not await self.has_permission(group_id, GroupPermission.STATS):
                await query.edit_message_text("❌ 此群组未启用统计功能")
                return
                
            if setting_type == "toggle_media":
                settings = await self.db.get_group_settings(group_id)
                settings['count_media'] = not settings.get('count_media', False)
                await self.db.update_group_settings(group_id, settings)
                
                await self._handle_stats_section(query, context, group_id, "stats")
                
            else:
                setting_descriptions = {
                    'min_bytes': '最小统计字节数',
                    'daily_rank': '日排行显示数量',
                    'monthly_rank': '月排行显示数量'
                }
                
                if setting_type not in setting_descriptions:
                    await query.edit_message_text("❌ 无效的设置类型")
                    return
                    
                self.settings_manager.start_setting(
                    update.effective_user.id,
                    f'stats_{setting_type}',
                    group_id
                )
                
                await query.edit_message_text(
                    f"请输入新的{setting_descriptions[setting_type]}：\n"
                    "发送 /cancel 取消"
                )
                
        except Exception as e:
            logger.error(f"处理统计设置编辑回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理统计设置编辑时出错")

    async def _start_broadcast_task(self):
        while self.running:
            try:
                now = datetime.now()
                broadcasts = await self.db.db.broadcasts.find({
                    'start_time': {'$lte': now},
                    'end_time': {'$gt': now},
                    '$or': [
                        {'last_broadcast': {'$exists': False}},
                        {'last_broadcast': {'$lte': now - timedelta(seconds=lambda b: b['interval'])}}
                    ]
                }).to_list(None)

                for broadcast in broadcasts:
                    try:
                        if broadcast['content_type'] == 'text':
                            await self.application.bot.send_message(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'photo':
                            await self.application.bot.send_photo(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'video':
                            await self.application.bot.send_video(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'document':
                            await self.application.bot.send_document(broadcast['group_id'], broadcast['content'])

                        await self.db.db.broadcasts.update_one(
                            {'_id': broadcast['_id']},
                            {'$set': {'last_broadcast': now}}
                        )
                    except Exception as e:
                        logger.error(f"发送轮播消息时出错: {e}")

                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"轮播任务出错: {e}")
                await asyncio.sleep(60)

    async def _start_cleanup_task(self):
        async def cleanup_routine():
            while self.running:
                try:
                    await self.db.cleanup_old_stats(
                        days=DEFAULT_SETTINGS.get('cleanup_days', 30)
                    )
                    await asyncio.sleep(24 * 60 * 60)
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
                    await asyncio.sleep(1 * 60 * 60)
        
        self.cleanup_task = asyncio.create_task(cleanup_routine())

    async def _handle_start(self, update: Update, context):
        if not update.effective_user or not update.message:
            return

        welcome_text = (
            f"👋 你好 {update.effective_user.first_name}！\n\n"
            "我是啤酒群专属机器人，主要功能包括：\n"
            "• 关键词自动回复\n"
            "• 消息统计\n"
            "• 轮播消息\n\n"
            "🔧 使用 /settings 来配置机器人\n"
            "📊 使用 /tongji 查看今日统计\n"
            "📈 使用 /tongji30 查看月度统计"
        )
        
        await update.message.reply_text(welcome_text)

    async def stop(self):
        self.running = False
        self.shutdown_event.set()
        
        if self.cleanup_task:
            self.cleanup_task.cancel()
        
        if self.web_runner:
            await self.web_runner.cleanup()
        
        if self.application:
            try:
                await self.application.stop()
                await self.application.shutdown()
            except Exception as e:
                logger.error(f"停止应用时出错: {e}")
        
        self.db.close()
        
        logger.info("机器人已停止")

    async def shutdown(self):
        await self.stop()

    async def is_superadmin(self, user_id: int) -> bool:
        user = await self.db.get_user(user_id)
        return user and user['role'] == UserRole.SUPERADMIN.value
        
    async def is_admin(self, user_id: int) -> bool:
        user = await self.db.get_user(user_id)
        return user and user['role'] in {UserRole.ADMIN.value, UserRole.SUPERADMIN.value}
        
    async def has_permission(self, group_id: int, permission: GroupPermission) -> bool:
        group = await self.db.get_group(group_id)
        return group and permission.value in group.get('permissions', [])

    async def update_stats_setting(self, group_id: int, setting_type: str, value: int):
        settings = await self.db.get_group_settings(group_id)
        if setting_type == 'stats_min_bytes':
            settings['min_bytes'] = value
            tips = f"最小统计字节数已设置为 {value} 字节"
        elif setting_type == 'stats_daily_rank':
            settings['daily_rank_size'] = value
            tips = f"日排行显示数量已设置为 {value}"
        elif setting_type == 'stats_monthly_rank':
            settings['monthly_rank_size'] = value
            tips = f"月排行显示数量已设置为 {value}"
        await self.db.update_group_settings(group_id, settings)
        return tips

    def _create_navigation_keyboard(
            self,
            current_page: int,
            total_pages: int,
            base_callback: str
        ) -> List[List[InlineKeyboardButton]]:
        keyboard = []
        nav_row = []
        
        if current_page > 1:
            nav_row.append(
                InlineKeyboardButton(
                    ◀️ 上一页",
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

# 主函数和信号处理
async def handle_signals(bot):
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(
                sig,
                lambda: asyncio.create_task(bot.stop())
            )
        logger.info("Signal handlers set up")
    except NotImplementedError:
        logger.warning("Signal handlers not supported on this platform")

async def main():
    bot = None
    try:
        bot = TelegramBot()
               
        if not await bot.initialize():
            logger.error("机器人初始化失败")
            return
        
        await handle_signals(bot)
        
        if not await bot.start():
            logger.error("机器人启动失败")
            return
        
        while bot.running:
            await asyncio.sleep(1)
        
    except Exception as e:
        logger.error(f"机器人启动失败: {e}")
        logger.error(traceback.format_exc())
    finally:
        if bot:
            await bot.shutdown()

def async_main():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("机器人被用户停止")
    except Exception as e:
        logger.error(f"机器人停止，错误原因: {e}")
        logger.error(traceback.format_exc())
        raise

if __name__ == '__main__':
    async_main()
