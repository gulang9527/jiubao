import os
import json
import signal
import asyncio
import logging
import traceback
import config
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple, Callable, Union
from enum import Enum
from functools import wraps
from bson import ObjectId

import re
from aiohttp import web
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes, CallbackContext
)
from dotenv import load_dotenv

from db import Database, UserRole, GroupPermission
from utils import (
    validate_time_format, validate_interval, format_file_size, validate_regex,
    get_media_type, format_duration, parse_command_args, escape_markdown,
    validate_settings, format_error_message, validate_delete_timeout,
    is_auto_delete_exempt, get_message_metadata, CallbackDataBuilder,
    KeyboardBuilder
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

# 装饰器和工具函数
def require_group_permission(permission: GroupPermission):
    """权限验证装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, update, context, *args, **kwargs):
            if not update.effective_chat:
                return
            if not await self.has_permission(update.effective_chat.id, permission):
                await update.message.reply_text("❌ 权限不足")
                return
            return await func(self, update, context, *args, **kwargs)
        return wrapper
    return decorator

def handle_callback_errors(func):
    """回调错误处理装饰器"""
    @wraps(func)
    async def wrapper(self, update, context, *args, **kwargs):
        try:
            return await func(self, update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Callback error in {func.__name__}: {e}")
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(
                    "❌ 操作出错，请重试"
                )
    return wrapper

def error_handler(func: Callable) -> Callable:
    """错误处理装饰器"""
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(self, update, context, *args, **kwargs)
        except Exception as e:
            await self.error_handler.handle_error(update, context)
            raise
    return wrapper

def check_command_usage(func: Callable) -> Callable:
    """命令使用检查装饰器"""
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_message:
            return
            
        message = update.effective_message
        command = message.text.split()[0].lstrip('/').split('@')[0]
        
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            return
            
        usage = CommandHelper.get_usage(command)
        if not usage:
            return await func(self, update, context, *args, **kwargs)
            
        if usage['admin_only'] and not await self.is_admin(user_id):
            await update.message.reply_text("❌ 该命令仅管理员可用")
            return
            
        if '<' in usage['usage'] and not context.args:
            await update.message.reply_text(
                f"❌ 命令使用方法不正确\n{CommandHelper.format_usage(command)}"
            )
            return
            
        return await func(self, update, context, *args, **kwargs)
    return wrapper

def register_middleware(application: Application, middlewares: list) -> None:
    """注册中间件"""
    for middleware in middlewares:
        application.post_init = middleware

# 设置管理模块
class SettingsManager:
    def __init__(self, db):
        self.db = db
        self._states = {}
        self._locks = {}
        self._state_locks = {}
        self._cleanup_task = None
        self._max_states_per_user = 5
        import config
        self.config = config
        
    async def start(self):
        """启动状态管理器"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("状态管理器已启动")
        
    async def stop(self):
        """停止状态管理器"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("状态管理器已停止")

    async def _get_state_lock(self, user_id: int):
        """获取用户状态锁"""
        if user_id not in self._state_locks:
            self._state_locks[user_id] = asyncio.Lock()
        return self._state_locks[user_id]

    async def _cleanup_loop(self):
        """清理过期状态"""
        while True:
            try:
                now = datetime.now(self.config.TIMEZONE)
                expired_keys = []
                async with asyncio.Lock():
                    for key, state in self._states.items():
                        if (now - state['timestamp']).total_seconds() > 300:
                            expired_keys.append(key)
                    
                    for key in expired_keys:
                        logger.info(f"清理过期状态: {key}")
                        await self._cleanup_state(key)
                        
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"状态清理错误: {e}")
                await asyncio.sleep(60)

    async def _cleanup_state(self, key: str):
        """清理特定状态"""
        if key in self._states:
            del self._states[key]
        if key in self._locks:
            del self._locks[key]
        logger.info(f"状态已清理: {key}")
                
    async def get_current_page(self, group_id: int, section: str) -> int:
        """获取当前页码"""
        state_key = f"page_{group_id}_{section}"
        async with asyncio.Lock():
            state = self._states.get(state_key, {})
            return state.get('page', 1)
        
    async def set_current_page(self, group_id: int, section: str, page: int):
        """设置当前页码"""
        state_key = f"page_{group_id}_{section}"
        async with asyncio.Lock():
            self._states[state_key] = {
                'page': page,
                'timestamp': datetime.now(config.TIMEZONE)
            }
            logger.info(f"设置页码: {state_key} => {page}")
            
    async def start_setting(self, user_id: int, setting_type: str, group_id: int):
        """开始设置过程"""
        state_lock = await self._get_state_lock(user_id)
        async with state_lock:
            user_states = sum(1 for k in self._states if k.startswith(f"setting_{user_id}"))
            if user_states >= self._max_states_per_user:
                raise ValueError(f"用户同时进行的设置操作不能超过 {self._max_states_per_user} 个")
            
            old_state_key = f"setting_{user_id}_{setting_type}"
            if old_state_key in self._states:
                del self._states[old_state_key]
                logger.info(f"清除旧状态: {old_state_key}")
            
            state_key = f"setting_{user_id}_{setting_type}"
            self._states[state_key] = {
                'group_id': group_id,
                'step': 1,
                'data': {},
                'timestamp': datetime.now(config.TIMEZONE)
            }
            logger.info(f"创建设置状态: {state_key}, 群组: {group_id}")
        
    async def get_setting_state(self, user_id: int, setting_type: str) -> Optional[dict]:
        """获取设置状态"""
        async with asyncio.Lock():
            state_key = f"setting_{user_id}_{setting_type}"
            state = self._states.get(state_key)
            logger.info(f"获取状态: {state_key} => {state}")
            return state
        
    async def update_setting_state(self, user_id: int, setting_type: str, data: dict, next_step: bool = False):
        """更新设置状态"""
        state_key = f"setting_{user_id}_{setting_type}"
        state_lock = await self._get_state_lock(user_id)
        
        async with state_lock:
            if state_key not in self._states:
                logger.warning(f"更新不存在的状态: {state_key}")
                return
                
            self._states[state_key]['data'].update(data)
            
            if next_step:
                self._states[state_key]['step'] += 1
                logger.info(f"状态 {state_key} 进入下一步: {self._states[state_key]['step']}")
            
            self._states[state_key]['timestamp'] = datetime.now(config.TIMEZONE)
            logger.info(f"更新状态: {state_key}, 步骤: {self._states[state_key]['step']}, 数据: {self._states[state_key]['data']}")
            
    async def clear_setting_state(self, user_id: int, setting_type: str):
        """清除设置状态"""
        state_key = f"setting_{user_id}_{setting_type}"
        state_lock = await self._get_state_lock(user_id)
        
        async with state_lock:
            if state_key in self._states:
                await self._cleanup_state(state_key)
                logger.info(f"清除设置状态: {state_key}")

    async def get_active_settings(self, user_id: int) -> list:
        """获取用户当前活动的设置列表"""
        async with asyncio.Lock():
            settings = [
                k.split('_')[2] 
                for k in self._states 
                if k.startswith(f"setting_{user_id}")
            ]
            logger.info(f"用户 {user_id} 的活动设置: {settings}")
            return settings

    async def check_setting_conflict(self, user_id: int, setting_type: str) -> bool:
        """检查是否存在设置冲突"""
        async with asyncio.Lock():
            conflicts = [
                k for k in self._states 
                if k.startswith(f"setting_{user_id}") 
                and setting_type in k
            ]
            has_conflict = bool(conflicts)
            if has_conflict:
                logger.warning(f"检测到设置冲突: 用户 {user_id}, 类型 {setting_type}, 冲突: {conflicts}")
            return has_conflict

# 统计管理模块
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
            'date': datetime.now(config.TIMEZONE).strftime('%Y-%m-%d'),
            'total_messages': 1,
            'total_size': message_size,
            'media_type': media_type
        }
        await self.db.add_message_stat(stat_data)

    async def get_daily_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取每日统计，仅统计消息数量"""
        today = datetime.now(config.TIMEZONE).strftime('%Y-%m-%d')
        limit = 15
        max_users = 100
        
        pipeline = [
            {'$match': {'group_id': group_id, 'date': today}},
            {'$group': {'_id': '$user_id', 'total_messages': {'$sum': '$total_messages'}}},
            {'$sort': {'total_messages': -1}},
            {'$limit': max_users}
        ]
        all_stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
        
        total_users = len(all_stats)
        total_pages = (total_users + limit - 1) // limit
        
        start_idx = (page - 1) * limit
        end_idx = min(start_idx + limit, total_users)
        stats = all_stats[start_idx:end_idx]
        
        return stats, total_pages

    async def get_monthly_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取近30日统计，仅统计消息数量"""
        thirty_days_ago = (datetime.now(config.TIMEZONE) - timedelta(days=30)).strftime('%Y-%m-%d')
        limit = 15
        max_users = 100
        
        pipeline = [
            {'$match': {'group_id': group_id, 'date': {'$gte': thirty_days_ago}}},
            {'$group': {'_id': '$user_id', 'total_messages': {'$sum': '$total_messages'}}},
            {'$sort': {'total_messages': -1}},
            {'$limit': max_users}
        ]
        all_stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
        
        total_users = len(all_stats)
        total_pages = (total_users + limit - 1) // limit
        
        start_idx = (page - 1) * limit
        end_idx = min(start_idx + limit, total_users)
        stats = all_stats[start_idx:end_idx]
        
        return stats, total_pages

# 广播管理模块
class BroadcastManager:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot
        
    async def get_broadcasts(self, group_id: int) -> List[Dict]:
        """获取群组的广播消息"""
        return await self.db.get_broadcasts(group_id)
        
    async def add_broadcast(self, broadcast_data: Dict) -> ObjectId:
        """添加广播消息"""
        result = await self.db.db.broadcasts.insert_one(broadcast_data)
        return result.inserted_id
        
    async def remove_broadcast(self, group_id: int, broadcast_id: str) -> bool:
        """删除广播消息"""
        try:
            result = await self.db.db.broadcasts.delete_one({
                'group_id': group_id,
                '_id': ObjectId(broadcast_id)
            })
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除广播消息错误: {e}")
            return False
            
    async def get_pending_broadcasts(self) -> List[Dict]:
        """获取待发送的广播消息"""
        now = datetime.now(config.TIMEZONE)
        return await self.db.db.broadcasts.find({
            'start_time': {'$lte': now},
            'end_time': {'$gt': now},
            '$or': [
                {'last_broadcast': {'$exists': False}},
                {'last_broadcast': {'$lt': now - timedelta(seconds='$interval')}}
            ]
        }).to_list(None)
        
    async def update_last_broadcast(self, broadcast_id: ObjectId) -> bool:
        """更新最后发送时间"""
        try:
            result = await self.db.db.broadcasts.update_one(
                {'_id': broadcast_id},
                {'$set': {'last_broadcast': datetime.now(config.TIMEZONE)}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"更新广播发送时间错误: {e}")
            return False

# 关键词管理模块
class KeywordManager:
    def __init__(self, db):
        self.db = db
        self._built_in_keywords = {}
        
    def register_built_in_keyword(self, pattern: str, handler: callable):
        """注册内置关键词"""
        self._built_in_keywords[pattern] = handler
        
    async def match_keyword(self, group_id: int, text: str, message: Message) -> Optional[str]:
        """匹配关键词并返回回复"""
        for pattern, handler in self._built_in_keywords.items():
            if text == pattern:
                return await handler(message)
        
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            try:
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

# 错误处理模块
class ErrorHandler:
    def __init__(self, logger):
        self.logger = logger
        self._error_handlers = {}
        self._setup_default_handlers()
        
    def _setup_default_handlers(self):
        self._error_handlers.update({
            'InvalidToken': self._handle_invalid_token,
            'Unauthorized': self._handle_unauthorized,
            'TimedOut': self._handle_timeout,
            'NetworkError': self._handle_network_error,
            'ChatMigrated': self._handle_chat_migrated,
            'TelegramError': self._handle_telegram_error,
            'MessageTooLong': self._handle_message_too_long,
            'FloodWait': self._handle_flood_wait,
            'RetryAfter': self._handle_retry_after,
            'BadRequest': self._handle_bad_request
        })
        
    async def _handle_invalid_token(self, update: Update, error: Exception) -> str:
        self.logger.critical("Bot token is invalid!")
        return "❌ 机器人配置错误，请联系管理员"
        
    async def _handle_unauthorized(self, update: Update, error: Exception) -> str:
        self.logger.error(f"Unauthorized error: {error}")
        return "❌ 权限不足，无法执行该操作"
        
    async def _handle_timeout(self, update: Update, error: Exception) -> str:
        self.logger.warning(f"Request timed out: {error}")
        return "❌ 操作超时，请重试"
        
    async def _handle_network_error(self, update: Update, error: Exception) -> str:
        self.logger.error(f"Network error occurred: {error}")
        return "❌ 网络错误，请稍后重试"
        
    async def _handle_chat_migrated(self, update: Update, error: Exception) -> str:
        self.logger.info(f"Chat migrated to {error.new_chat_id}")
        return "群组ID已更新，请重新设置"

    async def _handle_message_too_long(self, update: Update, error: Exception) -> str:
        self.logger.warning(f"Message too long: {error}")
        return "❌ 消息内容过长，请缩短后重试"

    async def _handle_flood_wait(self, update: Update, error: Exception) -> str:
        wait_time = getattr(error, 'retry_after', 60)
        self.logger.warning(f"Flood wait error: {error}, retry after {wait_time} seconds")
        return f"❌ 操作过于频繁，请等待 {wait_time} 秒后重试"

    async def _handle_retry_after(self, update: Update, error: Exception) -> str:
        retry_after = getattr(error, 'retry_after', 30)
        self.logger.warning(f"Need to retry after {retry_after} seconds")
        return f"❌ 请等待 {retry_after} 秒后重试"

    async def _handle_bad_request(self, update: Update, error: Exception) -> str:
        self.logger.error(f"Bad request error: {error}")
        return "❌ 无效的请求，请检查输入"
        
    async def _handle_telegram_error(self, update: Update, error: Exception) -> str:
        self.logger.error(f"Telegram error occurred: {error}")
        return "❌ 操作失败，请重试"
        
    async def handle_error(self, update: Update, context: CallbackContext) -> None:
        error = context.error
        error_type = type(error).__name__
        
        try:
            handler = self._error_handlers.get(error_type, self._handle_telegram_error)
            error_message = await handler(update, error)
            
            self.logger.error(f"Update {update} caused error {error}", exc_info=context.error)
            
            if update and update.effective_message:
                if update.callback_query:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text(error_message)
                else:
                    await update.effective_message.reply_text(error_message)
                    
        except Exception as e:
            self.logger.error(f"Error handling failed: {e}")
            self.logger.error(traceback.format_exc())

    def register_handler(self, error_type: str, handler: Callable):
        self._error_handlers[error_type] = handler

# 中间件模块
class MessageMiddleware:
    def __init__(self, bot):
        self.bot = bot
        
    async def __call__(self, update, context):
        if not update.effective_message:
            return
            
        try:
            if not await self._check_basic_security(update):
                return
                
            if not await self._check_permissions(update):
                return
                
            await context.application.process_update(update)
            
        except Exception as e:
            logger.error(f"中间件处理错误: {e}")
            
    async def _check_basic_security(self, update: Update) -> bool:
        message = update.effective_message
        
        if message.text and len(message.text) > 4096:
            await message.reply_text("❌ 消息内容过长")
            return False
            
        if message.document and message.document.file_size > 20 * 1024 * 1024:
            await message.reply_text("❌ 文件大小超过限制")
            return False
            
        return True
        
    async def _check_permissions(self, update: Update) -> bool:
        if not update.effective_chat or not update.effective_user:
            return False
            
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        if await self.bot.db.is_user_banned(user_id):
            return False
            
        if not await self.bot.db.get_group(chat_id):
            return False
            
        return True
        
    async def _clean_message(self, update: Update) -> Optional[str]:
        message = update.effective_message
        
        if not message.text:
            return None
            
        cleaned_text = re.sub(r'[^\w\s\-.,?!@#$%^&*()]', '', message.text)
        return cleaned_text
        
    async def _check_rate_limit(self, update: Update) -> bool:
        if not update.effective_user:
            return False
            
        user_id = update.effective_user.id
        recent_messages = await self.bot.db.get_recent_message_count(user_id, seconds=60)
        
        if recent_messages > 30:
            await update.effective_message.reply_text("❌ 消息发送过于频繁，请稍后再试")
            return False
            
        return True

class ErrorHandlingMiddleware:
    def __init__(self, error_handler):
        self.error_handler = error_handler
        
    async def __call__(self, update, context):
        try:
            return await context.application.process_update(update)
        except Exception as e:
            await self.error_handler.handle_error(update, context)
            raise

# 命令帮助模块
class CommandHelper:
    COMMAND_USAGE = {
        'start': {
            'usage': '/start',
            'description': '启动机器人并查看功能列表',
            'example': None,
            'admin_only': False
        },
        'settings': {
            'usage': '/settings',
            'description': '打开设置菜单',
            'example': None,
            'admin_only': True
        },
        'tongji': {
            'usage': '/tongji [页码]',
            'description': '查看今日统计排行',
            'example': '/tongji 2',
            'admin_only': False
        },
        'tongji30': {
            'usage': '/tongji30 [页码]',
            'description': '查看30日统计排行',
            'example': '/tongji30 2',
            'admin_only': False
        },
        'addadmin': {
            'usage': '/addadmin <用户ID>',
            'description': '添加管理员',
            'example': '/addadmin 123456789',
            'admin_only': True
        },
        'deladmin': {
            'usage': '/deladmin <用户ID>',
            'description': '删除管理员',
            'example': '/deladmin 123456789',
            'admin_only': True
        },
        'authgroup': {
            'usage': '/authgroup <群组ID> ...',
            'description': '授权群组',
            'example': '/authgroup -100123456789 keywords stats broadcast',
            'admin_only': True
        },
        'deauthgroup': {
            'usage': '/deauthgroup <群组ID>',
            'description': '取消群组授权',
            'example': '/deauthgroup -100123456789',
            'admin_only': True
        },
        'cancel': {
            'usage': '/cancel',
            'description': '取消当前操作',
            'example': None,
            'admin_only': False
        }
    }
    
    @classmethod
    def get_usage(cls, command: str) -> Optional[dict]:
        return cls.COMMAND_USAGE.get(command)
        
    @classmethod
    def format_usage(cls, command: str) -> str:
        usage = cls.get_usage(command)
        if not usage:
            return "❌ 未知命令"
            
        text = [
            f"📝 命令: {command}",
            f"用法: {usage['usage']}",
            f"说明: {usage['description']}"
        ]
        
        if usage['example']:
            text.append(f"示例: {usage['example']}")
            
        if usage['admin_only']:
            text.append("注意: 仅管理员可用")
            
        return "\n".join(text)

# 主Bot类
class TelegramBot:
    def __init__(self):
        self.db = None
        self.application = None
        self.web_app = None
        self.web_runner = None
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.cleanup_task = None
        self.settings_manager = None
        self.keyword_manager = None
        self.broadcast_manager = None
        self.stats_manager = None
        self.error_handler = None
        
    async def initialize(self):
        """初始化机器人"""
        try:
            from config_validator import validate_config, ConfigValidationError
            import config
            
            try:
                validate_config(config)
            except ConfigValidationError as e:
                logger.error(f"配置验证失败: {e}")
                return False
                
            logger.info("开始初始化机器人")
            
            from config import (
                TELEGRAM_TOKEN, MONGODB_URI, MONGODB_DB, DEFAULT_SUPERADMINS,
                DEFAULT_SETTINGS, BROADCAST_SETTINGS, KEYWORD_SETTINGS
            )
    
            self.db = Database()
            if not await self.db.connect(MONGODB_URI, MONGODB_DB):
                logger.error("数据库连接失败")
                return False
    
            self.error_handler = ErrorHandler(logger)
            self.settings_manager = SettingsManager(self.db)
            await self.settings_manager.start()
            
            self.keyword_manager = KeywordManager(self.db)
            self.broadcast_manager = BroadcastManager(self.db, self)
            self.stats_manager = StatsManager(self.db)
            
            for admin_id in DEFAULT_SUPERADMINS:
                await self.db.add_user({
                    'user_id': admin_id,
                    'role': UserRole.SUPERADMIN.value
                })
                logger.info(f"已设置超级管理员: {admin_id}")
            
            default_groups = [
                {
                    'group_id': -1001234567890,  # 替换为实际群组ID
                    'permissions': [perm.value for perm in GroupPermission]
                }
            ]
            
            for group in default_groups:
                await self.db.add_group({
                    'group_id': group['group_id'],
                    'permissions': group['permissions'],
                    'settings': {'auto_delete': False, 'auto_delete_timeout': config.AUTO_DELETE_SETTINGS['default_timeout']}
                })
                logger.info(f"已设置群组权限: {group['group_id']}")
            
            webhook_domain = os.getenv('WEBHOOK_DOMAIN', 'your-render-app-name.onrender.com')
            self.application = Application.builder().token(TELEGRAM_TOKEN).build()
            
            await self._register_handlers()
            
            self.web_app = web.Application()
            self.web_app.router.add_get('/', self.handle_healthcheck)
            self.web_app.router.add_get('/health', self.handle_healthcheck)
            
            webhook_url = f"https://{webhook_domain}/webhook/{TELEGRAM_TOKEN}"
            webhook_path = f"/webhook/{TELEGRAM_TOKEN}"
            self.web_app.router.add_post(webhook_path, self._handle_webhook)
            
            self.web_runner = web.AppRunner(self.web_app)
            await self.web_runner.setup()
            
            from config import WEB_HOST, WEB_PORT
            site = web.TCPSite(self.web_runner, WEB_HOST, WEB_PORT)
            await site.start()
            logger.info(f"Web服务器已在 {WEB_HOST}:{WEB_PORT} 启动")
            
            await self.application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
            
            self.application.updater = None
            logger.info(f"Webhook已设置为 {webhook_url}")
            
            if not await self.verify_initialization():
                logger.error("初始化验证失败")
                return False
                
            logger.info("机器人初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"机器人初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
            
    async def verify_initialization(self):
        from config import DEFAULT_SUPERADMINS
        
        for admin_id in DEFAULT_SUPERADMINS:
            user = await self.db.get_user(admin_id)
            if not user or user['role'] != UserRole.SUPERADMIN.value:
                logger.error(f"超级管理员 {admin_id} 初始化失败")
                return False
        
        groups = await self.db.find_all_groups()
        if not groups:
            logger.error("没有找到任何已授权的群组")
            return False
        
        logger.info("初始化验证成功")
        logger.info(f"超级管理员: {DEFAULT_SUPERADMINS}")
        logger.info(f"已授权群组: {[g['group_id'] for g in groups]}")
        return True

    @classmethod
    async def main(cls):
        bot = cls()
        if not await bot.initialize():
            logger.error("机器人初始化失败")
            return
        
        await bot.handle_signals()
        if not await bot.start():
            logger.error("机器人启动失败")
            return
        
        while bot.running:
            await asyncio.sleep(1)
            
    async def start(self):
        if not self.application:
            logger.error("机器人未初始化")
            return False
            
        await self.application.initialize()
        await self.application.start()
        self.running = True
        
        await self._start_broadcast_task()
        await self._start_cleanup_task()
        
        logger.info("机器人成功启动")
        return True
    
    async def stop(self):
        self.running = False
        if self.shutdown_event:
            self.shutdown_event.set()

        if self.settings_manager:
            await self.settings_manager.stop()

        if self.cleanup_task:
            self.cleanup_task.cancel()

        if self.web_runner:
            await self.web_runner.cleanup()

        if self.application:
            try:
                if getattr(self.application, 'running', False):
                    await self.application.stop()
                    await self.application.shutdown()
            except Exception as e:
                logger.error(f"停止应用时出错: {e}")

        if self.db:
            try:
                await self.db.close()
            except Exception as e:
                logger.error(f"关闭数据库连接时出错: {e}")
                
    async def shutdown(self):
        await self.stop()

    async def _start_broadcast_task(self):
        while self.running:
            try:
                now = datetime.now(config.TIMEZONE)
                broadcasts = await self.db.db.broadcasts.find({
                    'start_time': {'$lte': now},
                    'end_time': {'$gt': now},
                }).to_list(None)
                
                filtered_broadcasts = []
                for broadcast in broadcasts:
                    if 'last_broadcast' not in broadcast or broadcast['last_broadcast'] <= now - timedelta(seconds=broadcast['interval']):
                        filtered_broadcasts.append(broadcast)
                
                for broadcast in filtered_broadcasts:
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
                    from config import DEFAULT_SETTINGS
                    await self.db.cleanup_old_stats(days=DEFAULT_SETTINGS.get('cleanup_days', 30))
                    await asyncio.sleep(24 * 60 * 60)
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
                    await asyncio.sleep(1 * 60 * 60)
    
        self.cleanup_task = asyncio.create_task(cleanup_routine())
    
    async def handle_signals(self):
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_running_loop().add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.stop())
                )
            logger.info("Signal handlers set up")
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")

    async def handle_healthcheck(self, request):
        return web.Response(text="Healthy", status=200)

    async def _handle_webhook(self, request):
        try:
            if request.content_type != 'application/json':
                logger.warning(f"收到无效的内容类型: {request.content_type}")
                return web.Response(status=415)
        
            update_data = await request.json()
            logger.info(f"收到webhook更新: {update_data}")
        
            update = Update.de_json(update_data, self.application.bot)
            if update:
                await self.application.process_update(update)
                logger.info("成功处理更新")
            else:
                logger.warning("收到无效的更新数据")
        
            return web.Response(status=200)
        except Exception as e:
            logger.error(f"处理webhook错误: {e}", exc_info=True)
            return web.Response(status=500)

    async def is_superadmin(self, user_id: int) -> bool:
        user = await self.db.get_user(user_id)
        return user and user['role'] == UserRole.SUPERADMIN.value
        
    async def is_admin(self, user_id: int) -> bool:
        user = await self.db.get_user(user_id)
        return user and user['role'] in {UserRole.ADMIN.value, UserRole.SUPERADMIN.value}
        
    async def has_permission(self, group_id: int, permission: GroupPermission) -> bool:
        group = await self.db.get_group(group_id)
        return group and permission.value in group.get('permissions', [])

    async def _register_handlers(self):
        message_middleware = MessageMiddleware(self)
        error_middleware = ErrorHandlingMiddleware(self.error_handler)
        
        register_middleware(self.application, [
            message_middleware,
            error_middleware
        ])

        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("tongji", self._handle_rank_command))
        self.application.add_handler(CommandHandler("tongji30", self._handle_rank_command))
        self.application.add_handler(CommandHandler("settings", self._handle_settings))
        self.application.add_handler(CommandHandler("admingroups", self._handle_admin_groups))
        self.application.add_handler(CommandHandler("cancel", self._handle_cancel))
    
        self.application.add_handler(CommandHandler("addsuperadmin", self._handle_add_superadmin))
        self.application.add_handler(CommandHandler("delsuperadmin", self._handle_del_superadmin))
        self.application.add_handler(CommandHandler("addadmin", self._handle_add_admin))
        self.application.add_handler(CommandHandler("deladmin", self._handle_del_admin))
        self.application.add_handler(CommandHandler("authgroup", self._handle_auth_group))
        self.application.add_handler(CommandHandler("deauthgroup", self._handle_deauth_group))
        self.application.add_handler(CommandHandler("checkconfig", self._handle_check_config))

        self.application.add_handler(CallbackQueryHandler(self._handle_settings_callback, pattern=r'^settings_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_keyword_callback, pattern=r'^keyword_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_broadcast_callback, pattern=r'^broadcast_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_keyword_continue_callback, pattern=r'^keyword_continue_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_stats_edit_callback, pattern=r'^stats_edit_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_auto_delete_callback, pattern=r'^auto_delete_'))
    
        self.application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self._handle_message))

    @handle_callback_errors
    async def _handle_keyword_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()

        data = query.data
        parts = data.split('_')
    
        if len(parts) < 3:
            await query.edit_message_text("❌ 无效的操作")
            return

        action = parts[1]
        group_id = int(parts[-1])

        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return

        if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
            await query.edit_message_text("❌ 此群组未启用关键词功能")
            return

        if action == "add":
            keyboard = [
                [InlineKeyboardButton("精确匹配", callback_data=f"keyword_type_exact_{group_id}"),
                 InlineKeyboardButton("正则匹配", callback_data=f"keyword_type_regex_{group_id}")],
                [InlineKeyboardButton("取消", callback_data=f"settings_keywords_{group_id}")]
            ]
            await query.edit_message_text("请选择关键词匹配类型：", reply_markup=InlineKeyboardMarkup(keyboard))

        elif action == "type":
            match_type = parts[2]
            logger.info(f"用户 {update.effective_user.id} 为群组 {group_id} 选择关键词匹配类型: {match_type}")
            
            active_settings = await self.settings_manager.get_active_settings(update.effective_user.id)
            if 'keyword' in active_settings:
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
            
            await self.settings_manager.start_setting(update.effective_user.id, 'keyword', group_id)
            await self.settings_manager.update_setting_state(update.effective_user.id, 'keyword', {'match_type': match_type})

            match_type_text = "精确匹配" if match_type == "exact" else "正则匹配"
            await query.edit_message_text(
                f"您选择了{match_type_text}方式\n\n请发送关键词内容：\n{'(支持正则表达式)' if match_type == 'regex' else ''}\n\n发送 /cancel 取消"
            )

        elif action == "detail":
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的关键词ID")
                return

            keyword_id = parts[2]
            keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
        
            if not keyword:
                await query.edit_message_text("❌ 未找到该关键词")
                return

            pattern = keyword['pattern']
            response_type = keyword['response_type']
            match_type = keyword['type']
            response_preview = "无法预览媒体内容" if response_type != 'text' else (keyword['response'][:100] + "..." if len(keyword['response']) > 100 else keyword['response'])

            response_type_text = {'text': '文本', 'photo': '图片', 'video': '视频', 'document': '文件'}.get(response_type, response_type)
            keyboard = [
                [InlineKeyboardButton("❌ 删除此关键词", callback_data=f"keyword_delete_confirm_{keyword_id}_{group_id}")],
                [InlineKeyboardButton("🔙 返回列表", callback_data=f"settings_keywords_{group_id}")]
            ]

            text = (
                f"📝 关键词详情：\n\n"
                f"🔹 匹配类型：{'正则匹配' if match_type == 'regex' else '精确匹配'}\n"
                f"🔹 关键词：{pattern}\n"
                f"🔹 回复类型：{response_type_text}\n"
                f"🔹 回复内容：{response_preview}\n"
            )
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        elif action == "delete_confirm":
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的关键词ID")
                return

            keyword_id = parts[2]
            keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
            if not keyword:
                await query.edit_message_text("❌ 未找到该关键词")
                return
                
            pattern = keyword['pattern']
            keyboard = [
                [InlineKeyboardButton("✅ 确认删除", callback_data=f"keyword_delete_{keyword_id}_{group_id}"),
                 InlineKeyboardButton("❌ 取消", callback_data=f"keyword_detail_{keyword_id}_{group_id}")]
            ]
            await query.edit_message_text(f"⚠️ 确定要删除关键词「{pattern}」吗？\n此操作不可撤销！", reply_markup=InlineKeyboardMarkup(keyboard))

        elif action == "delete":
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的关键词ID")
                return

            keyword_id = parts[2]
            keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
            pattern = keyword['pattern'] if keyword else "未知关键词"
                
            await self.db.remove_keyword(group_id, keyword_id)
            await query.edit_message_text(f"✅ 已删除关键词「{pattern}」")
            await asyncio.sleep(1)
            await self._show_keyword_settings(query, group_id)

    @handle_callback_errors
    async def _handle_keyword_continue_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()

        group_id = int(update.callback_query.data.split('_')[2])
        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return

        keyboard = [
            [InlineKeyboardButton("精确匹配", callback_data=f"keyword_type_exact_{group_id}"),
             InlineKeyboardButton("正则匹配", callback_data=f"keyword_type_regex_{group_id}")],
            [InlineKeyboardButton("取消", callback_data=f"settings_keywords_{group_id}")]
        ]
        await query.edit_message_text("请选择关键词匹配类型：", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _handle_settings_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()

        data = query.data
        if data == "show_manageable_groups":
            await self._handle_show_manageable_groups(update, context)
            return

        parts = data.split('_')
        if len(parts) < 3:
            await query.edit_message_text("❌ 无效的操作")
            return

        action = parts[1]
        group_id = int(parts[2])

        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return

        if action == "select":
            keyboard = []
            if await self.has_permission(group_id, GroupPermission.STATS):
                keyboard.append([InlineKeyboardButton("📊 统计设置", callback_data=f"settings_stats_{group_id}")])
            if await self.has_permission(group_id, GroupPermission.BROADCAST):
                keyboard.append([InlineKeyboardButton("📢 轮播消息", callback_data=f"settings_broadcast_{group_id}")])
            if await self.has_permission(group_id, GroupPermission.KEYWORDS):
                keyboard.append([InlineKeyboardButton("🔑 关键词设置", callback_data=f"settings_keywords_{group_id}")])
            
            # 添加自动删除设置
            settings = await self.db.get_group_settings(group_id)
            auto_delete_status = '开启' if settings.get('auto_delete', False) else '关闭'
            keyboard.append([InlineKeyboardButton(f"🗑️ 自动删除: {auto_delete_status}", callback_data=f"auto_delete_toggle_{group_id}")])
            
            keyboard.append([InlineKeyboardButton("🔙 返回群组列表", callback_data="show_manageable_groups")])
            await query.edit_message_text("请选择要管理的功能：", reply_markup=InlineKeyboardMarkup(keyboard))

        elif action in ["stats", "broadcast", "keywords"]:
            await self._handle_settings_section(query, context, group_id, action)

    @handle_callback_errors
    async def _handle_broadcast_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()

        data = query.data
        parts = data.split('_')
        if len(parts) < 3:
            await query.edit_message_text("❌ 无效的操作")
            return

        action = parts[1]
        group_id = int(parts[-1])

        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return

        if action == "add":
            await self.settings_manager.start_setting(update.effective_user.id, 'broadcast', group_id)
            await query.edit_message_text(
                "请发送要轮播的内容：\n支持文本、图片、视频或文件\n\n发送 /cancel 取消"
            )

        elif action == "detail":
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的轮播消息ID")
                return
                
            broadcast_id = ObjectId(parts[2])
            broadcast = await self.db.db.broadcasts.find_one({'_id': broadcast_id, 'group_id': group_id})
            if not broadcast:
                await query.edit_message_text("❌ 未找到该轮播消息")
                return
                
            content_preview = str(broadcast['content'])[:50] + "..." if len(str(broadcast['content'])) > 50 else str(broadcast['content'])
            start_time = broadcast['start_time'].astimezone(config.TIMEZONE).strftime('%Y-%m-%d %H:%M')
            end_time = broadcast['end_time'].astimezone(config.TIMEZONE).strftime('%Y-%m-%d %H:%M')
            interval = format_duration(broadcast['interval'])
            text = (
                f"📢 轮播消息详情：\n\n"
                f"🔹 类型：{broadcast['content_type']}\n"
                f"🔹 内容：{content_preview}\n"
                f"🔹 开始时间：{start_time}\n"
                f"🔹 结束时间：{end_time}\n"
                f"🔹 间隔：{interval}"
            )
            keyboard = [
                [InlineKeyboardButton("❌ 删除此轮播消息", callback_data=f"broadcast_delete_{broadcast_id}_{group_id}")],
                [InlineKeyboardButton("🔙 返回列表", callback_data=f"settings_broadcast_{group_id}")]
            ]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        elif action == "delete":
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的轮播消息ID")
                return
                
            broadcast_id = ObjectId(parts[2])
            await self.db.db.broadcasts.delete_one({'_id': broadcast_id, 'group_id': group_id})
            await query.edit_message_text("✅ 已删除轮播消息")
            await asyncio.sleep(1)
            await self._show_broadcast_settings(query, group_id)

    @handle_callback_errors
    async def _handle_stats_edit_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()

        data = query.data
        parts = data.split('_')
        if len(parts) < 4:
            await query.edit_message_text("❌ 无效的操作")
            return
            
        setting_type = parts[2]
        group_id = int(parts[-1])

        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return

        if not await self.has_permission(group_id, GroupPermission.STATS):
            await query.edit_message_text("❌ 此群组未启用统计功能")
            return

        settings = await self.db.get_group_settings(group_id)

        if setting_type == "min_bytes":
            await query.edit_message_text("请输入最小统计字节数：\n• 低于此值的消息将不计入统计\n• 输入 0 表示统计所有消息\n\n发送 /cancel 取消")
            await self.settings_manager.start_setting(update.effective_user.id, 'stats_min_bytes', group_id)

        elif setting_type == "toggle_media":
            settings['count_media'] = not settings.get('count_media', False)
            await self.db.update_group_settings(group_id, settings)
            await self._show_stats_settings(query, group_id, settings)

        elif setting_type == "daily_rank":
            await query.edit_message_text("请输入日排行显示的用户数量：\n• 建议在 5-20 之间\n\n发送 /cancel 取消")
            await self.settings_manager.start_setting(update.effective_user.id, 'stats_daily_rank', group_id)

        elif setting_type == "monthly_rank":
            await query.edit_message_text("请输入月排行显示的用户数量：\n• 建议在 5-20 之间\n\n发送 /cancel 取消")
            await self.settings_manager.start_setting(update.effective_user.id, 'stats_monthly_rank', group_id)

    @handle_callback_errors
    async def _handle_auto_delete_callback(self, update: Update, context):
        """处理自动删除设置回调"""
        query = update.callback_query
        await query.answer()

        data = query.data
        parts = data.split('_')
        if len(parts) < 3:
            await query.edit_message_text("❌ 无效的操作")
            return

        action = parts[2]
        group_id = int(parts[-1])

        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return

        settings = await self.db.get_group_settings(group_id)

        if action == "toggle":
            settings['auto_delete'] = not settings.get('auto_delete', False)
            await self.db.update_group_settings(group_id, settings)
            status = '开启' if settings['auto_delete'] else '关闭'
            keyboard = [
                [InlineKeyboardButton(f"自动删除: {status}", callback_data=f"auto_delete_toggle_{group_id}")],
                [InlineKeyboardButton("设置超时时间", callback_data=f"auto_delete_timeout_{group_id}")],
                [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
            ]
            await query.edit_message_text(f"自动删除已{status}", reply_markup=InlineKeyboardMarkup(keyboard))

        elif action == "timeout":
            current_timeout = settings.get('auto_delete_timeout', config.AUTO_DELETE_SETTINGS['default_timeout'])
            keyboard = [
                [InlineKeyboardButton(f"{'✅' if current_timeout == 300 else ' '} 5分钟", callback_data=f"auto_delete_set_timeout_{group_id}_300")],
                [InlineKeyboardButton(f"{'✅' if current_timeout == 600 else ' '} 10分钟", callback_data=f"auto_delete_set_timeout_{group_id}_600")],
                [InlineKeyboardButton(f"{'✅' if current_timeout == 1800 else ' '} 30分钟", callback_data=f"auto_delete_set_timeout_{group_id}_1800")],
                [InlineKeyboardButton("自定义", callback_data=f"auto_delete_custom_timeout_{group_id}")],
                [InlineKeyboardButton("返回", callback_data=f"auto_delete_toggle_{group_id}")]
            ]
            await query.edit_message_text("请选择自动删除的超时时间：", reply_markup=InlineKeyboardMarkup(keyboard))

        elif action == "set":
            timeout = int(parts[3])
            settings['auto_delete_timeout'] = timeout
            await self.db.update_group_settings(group_id, settings)
            status = '开启' if settings.get('auto_delete', False) else '关闭'
            keyboard = [
                [InlineKeyboardButton(f"自动删除: {status}", callback_data=f"auto_delete_toggle_{group_id}")],
                [InlineKeyboardButton("设置超时时间", callback_data=f"auto_delete_timeout_{group_id}")],
                [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
            ]
            await query.edit_message_text(f"超时时间已设置为 {format_duration(timeout)}", reply_markup=InlineKeyboardMarkup(keyboard))

        elif action == "custom":
            await self.settings_manager.start_setting(update.effective_user.id, 'auto_delete_timeout', group_id)
            await query.edit_message_text("请输入自定义超时时间（单位：秒，60-86400）：\n\n发送 /cancel 取消")

    @check_command_usage
    async def _handle_start(self, update: Update, context):
        if not update.effective_user or not update.message:
            return

        user_id = update.effective_user.id
        is_superadmin = await self.is_superadmin(user_id)
        is_admin = await self.is_admin(user_id)

        welcome_text = (
            f"👋 你好 {update.effective_user.first_name}！\n\n"
            "我是啤酒群酒保，主要功能包括：\n"
            "• 关键词自动回复\n"
            "• 消息统计\n"
            "• 轮播消息\n\n"
            "基础命令：\n"
            "🔧 /settings - 配置机器人\n"
            "📊 /tongji - 查看今日统计\n"
            "📈 /tongji30 - 查看30日统计\n"
            "🚫 /cancel - 取消当前操作\n"
        )

        if is_admin:
            welcome_text += (
                "\n管理员命令：\n"
                "👥 /admingroups - 查看可管理的群组\n"
                "⚙️ /settings - 群组设置管理\n"
            )

        if is_superadmin:
            welcome_text += (
                "\n超级管理员命令：\n"
                "➕ /addsuperadmin <用户ID> - 添加超级管理员\n"
                "➖ /delsuperadmin <用户ID> - 删除超级管理员\n"
                "👤 /addadmin <用户ID> - 添加管理员\n"
                "🚫 /deladmin <用户ID> - 删除管理员\n"
                "✅ /authgroup <群组ID>  ... - 授权群组\n"
                "❌ /deauthgroup <群组ID> - 取消群组授权\n"
                "🔍 /checkconfig - 检查当前配置\n"
            )

        welcome_text += "\n如需帮助，请联系管理员。"
        await update.message.reply_text(welcome_text)

    @check_command_usage
    async def _handle_settings(self, update: Update, context):
        manageable_groups = await self.db.get_manageable_groups(update.effective_user.id)
        if not manageable_groups:
            await update.message.reply_text("❌ 你没有权限管理任何群组")
            return
                
        keyboard = []
        for group in manageable_groups:
            try:
                group_info = await context.bot.get_chat(group['group_id'])
                group_name = group_info.title or f"群组 {group['group_id']}"
            except Exception:
                group_name = f"群组 {group['group_id']}"
                
            keyboard.append([InlineKeyboardButton(group_name, callback_data=f"settings_select_{group['group_id']}")])
            
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("请选择要管理的群组：", reply_markup=reply_markup)

    @check_command_usage
    async def _handle_rank_command(self, update: Update, context):
        if not update.effective_chat or not update.effective_user or not update.message:
            return
            
        command = update.message.text.split('@')[0][1:]
        group_id = update.effective_chat.id
        
        page = 1
        if context.args:
            try:
                page = int(context.args[0])
                if page < 1:
                    raise ValueError
            except ValueError:
                await update.message.reply_text("❌ 无效的页码")
                return

        if command == "tongji":
            stats, total_pages = await self.stats_manager.get_daily_stats(group_id, page)
            title = "📊 今日发言排行"
        else:
            stats, total_pages = await self.stats_manager.get_monthly_stats(group_id, page)
            title = "📊 近30天发言排行"
                
        if not stats:
            await update.message.reply_text("📊 暂无统计数据")
            return
                
        text = f"{title}\n\n"
        for i, stat in enumerate(stats, start=(page-1)*15+1):
            try:
                user = await context.bot.get_chat_member(group_id, stat['_id'])
                name = user.user.full_name or user.user.username or f"用户{stat['_id']}"
            except Exception:
                name = f"用户{stat['_id']}"
                
            text += f"{i}. {name}\n   消息数: {stat['total_messages']}\n\n"
            
        text += f"\n\n第 {page}/{total_pages} 页"
        if total_pages > 1:
            text += f"\n使用 /{command} <页码> 查看其他页"
            
        keyboard = self._create_navigation_keyboard(page, total_pages, f"{'today' if command == 'tongji' else 'monthly'}_{group_id}")
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)

    @check_command_usage
    async def _handle_admin_groups(self, update: Update, context):
        if not await self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ 只有管理员可以使用此命令")
            return
            
        groups = await self.db.get_manageable_groups(update.effective_user.id)
        if not groups:
            await update.message.reply_text("📝 你目前没有可管理的群组")
            return
                
        text = "📝 你可以管理的群组：\n\n"
        for group in groups:
            try:
                group_info = await context.bot.get_chat(group['group_id'])
                group_name = group_info.title
            except Exception:
                group_name = f"群组 {group['group_id']}"
                    
            text += f"• {group_name}\n  ID: {group['group_id']}\n  权限: {', '.join(group.get('permissions', []))}\n\n"
                
        await update.message.reply_text(text)

    @check_command_usage
    async def _handle_add_admin(self, update: Update, context):
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以添加管理员")
            return
            
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/addadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            user = await self.db.get_user(user_id)
            if user and user['role'] in [UserRole.ADMIN.value, UserRole.SUPERADMIN.value]:
                await update.message.reply_text("❌ 该用户已经是管理员")
                return
                
            await self.db.add_user({'user_id': user_id, 'role': UserRole.ADMIN.value})
            await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为管理员")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"添加管理员错误: {e}")
            await update.message.reply_text("❌ 添加管理员时出错")

    @check_command_usage
    async def _handle_del_admin(self, update: Update, context):
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除管理员")
            return
            
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deladmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            user = await self.db.get_user(user_id)
            if not user:
                await update.message.reply_text("❌ 该用户不是管理员")
                return
                
            if user['role'] == UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 不能删除超级管理员")
                return
                
            await self.db.remove_user(user_id)
            await update.message.reply_text(f"✅ 已删除管理员 {user_id}")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"删除管理员错误: {e}")
            await update.message.reply_text("❌ 删除管理员时出错")

    @check_command_usage
    async def _handle_add_superadmin(self, update: Update, context):
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以添加超级管理员")
            return
            
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/addsuperadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            user = await self.db.get_user(user_id)
            if user and user['role'] == UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 该用户已经是超级管理员")
                return
                
            await self.db.add_user({'user_id': user_id, 'role': UserRole.SUPERADMIN.value})
            await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为超级管理员")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"添加超级管理员错误: {e}")
            await update.message.reply_text("❌ 添加超级管理员时出错")

    @check_command_usage
    async def _handle_del_superadmin(self, update: Update, context):
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除超级管理员")
            return
            
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/delsuperadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            if user_id == update.effective_user.id:
                await update.message.reply_text("❌ 不能删除自己的超级管理员权限")
                return
                
            user = await self.db.get_user(user_id)
            if not user or user['role'] != UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 该用户不是超级管理员")
                return
                
            await self.db.remove_user(user_id)
            await update.message.reply_text(f"✅ 已删除超级管理员 {user_id}")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"删除超级管理员错误: {e}")
            await update.message.reply_text("❌ 删除超级管理员时出错")

    @check_command_usage
    async def _handle_check_config(self, update: Update, context):
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以查看配置")
            return
        
        superadmins = await self.db.get_users_by_role(UserRole.SUPERADMIN.value)
        superadmin_ids = [user['user_id'] for user in superadmins]
        groups = await self.db.find_all_groups()
        
        config_text = "🔧 当前配置信息：\n\n👥 超级管理员：\n" + "\n".join(f"• {admin_id}" for admin_id in superadmin_ids)
        config_text += "\n\n📋 已授权群组：\n" + "\n".join(f"• 群组 {group['group_id']}\n  权限: {', '.join(group.get('permissions', []))}" for group in groups)
        await update.message.reply_text(config_text)

    async def _handle_auth_group(self, update: Update, context):
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以授权群组")
            return
            
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：\n/authgroup <群组ID>")
            return
            
        try:
            group_id = int(context.args[0])
            try:
                group_info = await context.bot.get_chat(group_id)
                group_name = group_info.title
            except Exception:
                await update.message.reply_text("❌ 无法获取群组信息，请确保机器人已加入该群组")
                return
                
            all_permissions = [perm.value for perm in GroupPermission]
            await self.db.add_group({
                'group_id': group_id,
                'permissions': all_permissions,
                'settings': {'auto_delete': False, 'auto_delete_timeout': config.AUTO_DELETE_SETTINGS['default_timeout']}
            })
            await update.message.reply_text(f"✅ 已授权群组\n群组：{group_name}\nID：{group_id}\n已启用全部功能")
            
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字")
        except Exception as e:
            logger.error(f"授权群组错误: {e}")
            await update.message.reply_text("❌ 授权群组时出错")

    @check_command_usage
    async def _handle_deauth_group(self, update: Update, context):
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以解除群组授权")
            return
            
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deauthgroup <群组ID>")
            return
            
        try:
            group_id = int(context.args[0])
            group = await self.db.get_group(group_id)
            if not group:
                await update.message.reply_text("❌ 该群组未授权")
                return
                
            await self.db.remove_group(group_id)
            await update.message.reply_text(f"✅ 已解除群组 {group_id} 的所有授权")
            
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字")
        except Exception as e:
            logger.error(f"解除群组授权错误: {e}")
            await update.message.reply_text("❌ 解除群组授权时出错")

    async def _handle_show_manageable_groups(self, update: Update, context):
        query = update.callback_query
        await query.answer()

        manageable_groups = await self.db.get_manageable_groups(update.effective_user.id)
        if not manageable_groups:
            await query.edit_message_text("❌ 你没有权限管理任何群组")
            return
            
        keyboard = []
        for group in manageable_groups:
            try:
                group_info = await context.bot.get_chat(group['group_id'])
                group_name = group_info.title or f"群组 {group['group_id']}"
            except Exception:
                group_name = f"群组 {group['group_id']}"
            
            keyboard.append([InlineKeyboardButton(group_name, callback_data=f"settings_select_{group['group_id']}")])
        
        await query.edit_message_text("请选择要管理的群组：", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _handle_settings_section(self, query, context, group_id: int, section: str):
        if section == "stats":
            settings = await self.db.get_group_settings(group_id)
            await self._show_stats_settings(query, group_id, settings)
            
        elif section == "broadcast":
            await self._show_broadcast_settings(query, group_id)
            
        elif section == "keywords":
            await self._show_keyword_settings(query, group_id)

    async def _show_stats_settings(self, query, group_id: int, settings: dict):
        keyboard = [
            [InlineKeyboardButton(f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", callback_data=f"stats_edit_min_bytes_{group_id}")],
            [InlineKeyboardButton(f"统计多媒体: {'是' if settings.get('count_media', False) else '否'}", callback_data=f"stats_edit_toggle_media_{group_id}")],
            [InlineKeyboardButton(f"日排行显示数量: {settings.get('daily_rank_size', 15)}", callback_data=f"stats_edit_daily_rank_{group_id}")],
            [InlineKeyboardButton(f"月排行显示数量: {settings.get('monthly_rank_size', 15)}", callback_data=f"stats_edit_monthly_rank_{group_id}")],
            [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
        ]
        await query.edit_message_text(f"群组 {group_id} 的统计设置", reply_markup=InlineKeyboardMarkup(keyboard))
        
    async def _show_broadcast_settings(self, query, group_id: int):
        broadcasts = await self.db.get_broadcasts(group_id)
        keyboard = []  
        for bc in broadcasts:
            # 添加默认值处理
            content_type = bc.get('content_type', '未知类型')
            content = bc.get('content', '')
            content_preview = str(content)[:20] + '...' if len(str(content)) > 20 else str(content)   
            keyboard.append([
                InlineKeyboardButton(
                    f"📢 {content_type}: {content_preview}", 
                    callback_data=f"broadcast_detail_{bc['_id']}_{group_id}"
                )
            ])
        keyboard.append([InlineKeyboardButton("➕ 添加轮播消息", callback_data=f"broadcast_add_{group_id}")])
        keyboard.append([InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")])
        await query.edit_message_text(f"群组 {group_id} 的轮播消息设置", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_keyword_settings(self, query, group_id: int, page: int = 1):
        keywords = await self.db.get_keywords(group_id)
        total_pages = (len(keywords) + 9) // 10
        if page < 1:
            page = 1
        if page > total_pages:
            page = total_pages
    
        start_idx = (page - 1) * 10
        end_idx = min(start_idx + 10, len(keywords))
        page_keywords = keywords[start_idx:end_idx] if keywords else []
    
        keyboard = [
            [InlineKeyboardButton(f"🔑 {kw['pattern'][:20] + '...' if len(kw['pattern']) > 20 else kw['pattern']}", 
                                  callback_data=f"keyword_detail_{kw['_id']}_{group_id}")] 
            for kw in page_keywords
        ]
    
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("◀️ 上一页", callback_data=f"keyword_list_page_{page-1}_{group_id}"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("下一页 ▶️", callback_data=f"keyword_list_page_{page+1}_{group_id}"))
            if nav_buttons:
                keyboard.append(nav_buttons)
    
        keyboard.append([InlineKeyboardButton("➕ 添加关键词", callback_data=f"keyword_add_{group_id}")])
        keyboard.append([InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")])
    
        text = f"群组 {group_id} 的关键词设置" + (f"\n第 {page}/{total_pages} 页" if total_pages > 1 else "")
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def _process_stats_setting(self, update: Update, context, stats_state, setting_type):
        if not stats_state:
            await update.message.reply_text("❌ 设置会话已过期，请重新开始")
            return
            
        group_id = stats_state.get('group_id')
        try:
            value = int(update.message.text)
            if value < 0 and setting_type != 'stats_min_bytes':
                raise ValueError("值不能为负")
        except ValueError:
            await update.message.reply_text("❌ 请输入一个有效的数字")
            return
        
        settings = await self.db.get_group_settings(group_id)
        if setting_type == 'stats_min_bytes':
            settings['min_bytes'] = value
            tips = f"最小统计字节数已设置为 {value} 字节"
        elif setting_type == 'stats_daily_rank':
            if value < 1 or value > 50:
                await update.message.reply_text("❌ 显示数量必须在1-50之间")
                return
            settings['daily_rank_size'] = value
            tips = f"日排行显示数量已设置为 {value}"
        elif setting_type == 'stats_monthly_rank':
            if value < 1 or value > 50:
                await update.message.reply_text("❌ 显示数量必须在1-50之间")
                return
            settings['monthly_rank_size'] = value
            tips = f"月排行显示数量已设置为 {value}"
        else:
            await update.message.reply_text("❌ 未知的设置类型")
            return
            
        await self.db.update_group_settings(group_id, settings)
        keyboard = [
            [InlineKeyboardButton(f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", callback_data=f"stats_edit_min_bytes_{group_id}")],
            [InlineKeyboardButton(f"统计多媒体: {'是' if settings.get('count_media', False) else '否'}", callback_data=f"stats_edit_toggle_media_{group_id}")],
            [InlineKeyboardButton(f"日排行显示数量: {settings.get('daily_rank_size', 15)}", callback_data=f"stats_edit_daily_rank_{group_id}")],
            [InlineKeyboardButton(f"月排行显示数量: {settings.get('monthly_rank_size', 15)}", callback_data=f"stats_edit_monthly_rank_{group_id}")],
            [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
        ]
        await update.message.reply_text(f"✅ {tips}", reply_markup=InlineKeyboardMarkup(keyboard))
        await self.settings_manager.clear_setting_state(update.effective_user.id, setting_type)

    async def _process_keyword_adding(self, update: Update, context, setting_state):
        if not setting_state:
            await update.message.reply_text("❌ 设置会话已过期，请重新开始")
            return
            
        step = setting_state.get('step', 1)
        group_id = setting_state.get('group_id')
        data = setting_state.get('data', {})
        match_type = data.get('match_type')
        
        if step == 1:
            pattern = update.message.text.strip()
            new_data = {'pattern': pattern, 'type': match_type}
            await self.settings_manager.update_setting_state(update.effective_user.id, 'keyword', new_data)
            
            state_key = f"setting_{update.effective_user.id}_keyword"
            async with asyncio.Lock():
                if state_key in self.settings_manager._states:
                    self.settings_manager._states[state_key]['step'] = 2
                    self.settings_manager._states[state_key]['timestamp'] = datetime.now(config.TIMEZONE)
            
            await update.message.reply_text("✅ 关键词已设置\n\n请发送此关键词的回复内容（支持文字/图片/视频/文件）：\n\n发送 /cancel 取消设置")
            
        elif step == 2:
            response_type = None
            response_content = None
            
            if update.message.text:
                response_type = 'text'
                response_content = update.message.text
            elif update.message.photo:
                response_type = 'photo'
                response_content = update.message.photo[-1].file_id
            elif update.message.video:
                response_type = 'video'
                response_content = update.message.video.file_id
            elif update.message.document:
                response_type = 'document'
                response_content = update.message.document.file_id
            
            if not response_type or response_content is None:
                await update.message.reply_text("❌ 请发送有效的回复内容（文本/图片/视频/文件）")
                return
                
            pattern = data.get('pattern')
            pattern_type = data.get('type')
            if not pattern or not pattern_type:
                await update.message.reply_text("❌ 添加关键词出错，请重新开始")
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
                return
                
            from config import KEYWORD_SETTINGS
            if response_type == 'text' and len(response_content) > KEYWORD_SETTINGS.get('max_response_length', 1000):
                await update.message.reply_text(f"❌ 回复内容过长，请不要超过 {KEYWORD_SETTINGS.get('max_response_length', 1000)} 个字符")
                return
                
            keywords = await self.db.get_keywords(group_id)
            if len(keywords) >= KEYWORD_SETTINGS.get('max_keywords', 100):
                await update.message.reply_text(f"❌ 关键词数量已达到上限 {KEYWORD_SETTINGS.get('max_keywords', 100)} 个")
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
                return
                
            await self.db.add_keyword({
                'group_id': group_id,
                'pattern': pattern,
                'type': pattern_type,
                'response': response_content,
                'response_type': response_type
            })
            keyboard = [
                [InlineKeyboardButton("➕ 继续添加关键词", callback_data=f"keyword_continue_{group_id}")],
                [InlineKeyboardButton("🔙 返回关键词设置", callback_data=f"settings_keywords_{group_id}")]
            ]
            await update.message.reply_text(f"✅ 关键词 「{pattern}」 添加成功！", reply_markup=InlineKeyboardMarkup(keyboard))
            await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')

    async def _process_broadcast_adding(self, update: Update, context, setting_state):
        if not setting_state:
            await update.message.reply_text("❌ 设置会话已过期，请重新开始")
            return
            
        step = setting_state.get('step', 1)
        group_id = setting_state.get('group_id')
        data = setting_state.get('data', {})
        
        if step == 1:
            content_type = None
            content = None
            if update.message.text:
                content_type = 'text'
                content = update.message.text
            elif update.message.photo:
                content_type = 'photo'
                content = update.message.photo[-1].file_id
            elif update.message.video:
                content_type = 'video'
                content = update.message.video.file_id
            elif update.message.document:
                content_type = 'document'
                content = update.message.document.file_id
                
            if not content_type or not content:
                await update.message.reply_text("❌ 请发送有效的内容（文本/图片/视频/文件）")
                return

            if content_type == 'text' and len(content) > 4096:
                await update.message.reply_text("❌ 文本内容过长")
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')
                return

            await self.settings_manager.update_setting_state(
                update.effective_user.id,
                'broadcast',
                {'content_type': content_type, 'content': content}
            )
            state_key = f"setting_{update.effective_user.id}_broadcast"
            async with asyncio.Lock():
                if state_key in self.settings_manager._states:
                    self.settings_manager._states[state_key]['step'] = 2
                    self.settings_manager._states[state_key]['timestamp'] = datetime.now(config.TIMEZONE)

            await update.message.reply_text(
                "✅ 内容已设置\n\n请设置轮播时间参数：\n格式：开始时间 结束时间 间隔秒数\n例如：2024-02-22 08:00 2024-03-22 20:00 3600\n\n发送 /cancel 取消"
            )

        elif step == 2:
            try:
                parts = update.message.text.split()
                if len(parts) != 5:
                    raise ValueError("参数数量不正确")

                start_time = validate_time_format(f"{parts[0]} {parts[1]}")
                end_time = validate_time_format(f"{parts[2]} {parts[3]}")
                interval = validate_interval(parts[4])

                if not all([start_time, end_time, interval]):
                    raise ValueError("时间格式无效")

                if start_time >= end_time:
                    raise ValueError("结束时间必须晚于开始时间")

                from config import BROADCAST_SETTINGS
                if interval < BROADCAST_SETTINGS.get('min_interval', 60):
                    raise ValueError(f"间隔时间不能小于{BROADCAST_SETTINGS.get('min_interval', 60)}秒")

                broadcasts = await self.db.db.broadcasts.count_documents({'group_id': group_id})
                if broadcasts >= BROADCAST_SETTINGS.get('max_broadcasts', 10):
                    await update.message.reply_text(f"❌ 轮播消息数量已达到上限 {BROADCAST_SETTINGS.get('max_broadcasts', 10)} 条")
                    return

                await self.db.db.broadcasts.insert_one({
                    'group_id': group_id,
                    'content_type': data.get('content_type'),
                    'content': data.get('content'),
                    'start_time': start_time,
                    'end_time': end_time,
                    'interval': interval
                })
                await update.message.reply_text("✅ 轮播消息添加成功！")

            except ValueError as e:
                await update.message.reply_text(f"❌ {str(e)}")
                return
            finally:
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')

    async def _process_auto_delete_timeout(self, update: Update, context, setting_state):
        """处理自定义自动删除超时时间设置"""
        if not setting_state:
            await update.message.reply_text("❌ 设置会话已过期，请重新开始")
            return
            
        group_id = setting_state.get('group_id')
        try:
            timeout = int(update.message.text)
            from config import AUTO_DELETE_SETTINGS
            if timeout < AUTO_DELETE_SETTINGS['min_timeout'] or timeout > AUTO_DELETE_SETTINGS['max_timeout']:
                await update.message.reply_text(f"❌ 超时时间必须在 {AUTO_DELETE_SETTINGS['min_timeout']}-{AUTO_DELETE_SETTINGS['max_timeout']} 秒之间")
                return
                
            settings = await self.db.get_group_settings(group_id)
            settings['auto_delete_timeout'] = timeout
            await self.db.update_group_settings(group_id, settings)
            
            status = '开启' if settings.get('auto_delete', False) else '关闭'
            keyboard = [
                [InlineKeyboardButton(f"自动删除: {status}", callback_data=f"auto_delete_toggle_{group_id}")],
                [InlineKeyboardButton("设置超时时间", callback_data=f"auto_delete_timeout_{group_id}")],
                [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
            ]
            await update.message.reply_text(f"✅ 超时时间已设置为 {format_duration(timeout)}", reply_markup=InlineKeyboardMarkup(keyboard))
            await self.settings_manager.clear_setting_state(update.effective_user.id, 'auto_delete_timeout')
            
        except ValueError:
            await update.message.reply_text("❌ 请输入一个有效的数字")
            return

    @check_command_usage
    async def _handle_cancel(self, update: Update, context):
        """处理 /cancel 命令"""
        user_id = update.effective_user.id
        active_settings = await self.settings_manager.get_active_settings(user_id)
        if not active_settings:
            await update.message.reply_text("❌ 当前没有正在进行的操作")
            return
            
        for setting_type in active_settings:
            await self.settings_manager.clear_setting_state(user_id, setting_type)
        await update.message.reply_text("✅ 已取消所有操作")

    async def _handle_message(self, update: Update, context):
        message = update.effective_message
        if not message or not message.chat:
            return
            
        group_id = message.chat_id
        user_id = message.from_user.id
        
        settings = await self.db.get_group_settings(group_id)
        user = await self.db.get_user(user_id)
        user_role = user['role'] if user else UserRole.USER.value
        
        # 处理关键词
        if message.text and await self.has_permission(group_id, GroupPermission.KEYWORDS):
            reply = await self.keyword_manager.match_keyword(group_id, message.text, message)
            if reply:
                if reply.startswith('__media__'):
                    _, media_type, file_id = reply.split('__', 2)
                    if media_type == 'photo':
                        await message.reply_photo(file_id)
                    elif media_type == 'video':
                        await message.reply_video(file_id)
                    elif media_type == 'document':
                        await message.reply_document(file_id)
                else:
                    await message.reply_text(reply)
        
        # 统计消息
        if await self.has_permission(group_id, GroupPermission.STATS):
            await self.stats_manager.add_message_stat(group_id, user_id, message)
        
        # 自动删除
        if settings.get('auto_delete', False) and not is_auto_delete_exempt(user_role, message.text):
            timeout = settings.get('auto_delete_timeout', config.AUTO_DELETE_SETTINGS['default_timeout'])
            await asyncio.sleep(timeout)
            try:
                await message.delete()
            except Exception as e:
                logger.error(f"删除消息失败: {e}")

    def _create_navigation_keyboard(self, current_page: int, total_pages: int, base_callback: str) -> List[List[InlineKeyboardButton]]:
        keyboard = []
        nav_row = []
        if current_page > 1:
            nav_row.append(InlineKeyboardButton("◀️ 上一页", callback_data=f"{base_callback}_page_{current_page-1}"))
        if current_page < total_pages:
            nav_row.append(InlineKeyboardButton("下一页 ▶️", callback_data=f"{base_callback}_page_{current_page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        return keyboard

if __name__ == "__main__":
    asyncio.run(TelegramBot.main())
