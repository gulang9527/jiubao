import os
import json
import signal
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple, Callable, Union
from enum import Enum
from functools import wraps
from bson import ObjectId
import html
import re
from aiohttp import web, ClientSession
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes, CallbackContext
)
from dotenv import load_dotenv
import pytz

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

# 加载环境变量和配置
load_dotenv()
from config import *

# 设置时区
beijing_tz = pytz.timezone(TIMEZONE)

# 用户角色和群组权限枚举
class UserRole(Enum):
    USER = 'user'
    ADMIN = 'admin'
    SUPERADMIN = 'superadmin'

class GroupPermission(Enum):
    KEYWORDS = 'keywords'
    STATS = 'stats'
    BROADCAST = 'broadcast'

# 装饰器和工具函数
def require_group_permission(permission: GroupPermission):
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
    @wraps(func)
    async def wrapper(self, update, context, *args, **kwargs):
        try:
            return await func(self, update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Callback error in {func.__name__}: {e}")
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text("❌ 操作出错，请重试")
    return wrapper

def error_handler(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(self, update, context, *args, **kwargs)
        except Exception as e:
            await self.error_handler.handle_error(update, context)
            raise
    return wrapper

def check_command_usage(func: Callable) -> Callable:
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
            await update.message.reply_text(f"❌ 命令使用方法不正确\n{CommandHelper.format_usage(command)}")
            return
        return await func(self, update, context, *args, **kwargs)
    return wrapper

def register_middleware(application: Application, middlewares: list) -> None:
    for middleware in middlewares:
        application.post_init = middleware

class Utils:
    @staticmethod
    def validate_time_format(time_str: str) -> Optional[datetime]:
        try:
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M')
            return beijing_tz.localize(dt)
        except ValueError:
            return None

    @staticmethod
    def validate_interval(interval_str: str) -> Optional[int]:
        try:
            interval = int(interval_str)
            return interval if interval > 0 else None
        except ValueError:
            return None

    @staticmethod
    def parse_interval(interval_str: str) -> int:
        """解析 'X小时Y分' 格式为秒数"""
        match = re.match(r"(\d+)小时(\d+)分", interval_str)
        if match:
            hours, minutes = map(int, match.groups())
            return hours * 3600 + minutes * 60
        try:
            return int(interval_str)
        except ValueError:
            return 0

    @staticmethod
    def truncate_name(name: str, max_length: int = 10) -> str:
        """截断名字"""
        return name[:max_length-1] + "…" if len(name) > max_length else name

    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        if size_bytes < 1024:
            return f"{size_bytes}B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.2f}KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.2f}MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.2f}GB"

    @staticmethod
    def validate_regex(pattern: str) -> bool:
        try:
            re.compile(pattern)
            return True
        except re.error:
            return False

    @staticmethod
    def get_media_type(message: Message) -> Optional[str]:
        if message.photo:
            return 'photo'
        elif message.video:
            return 'video'
        elif message.document:
            return 'document'
        elif message.audio:
            return 'audio'
        elif message.voice:
            return 'voice'
        elif message.animation:
            return 'animation'
        elif message.sticker:
            return 'sticker'
        return 'text'

    @staticmethod
    def format_duration(seconds: int) -> str:
        if seconds < 60:
            return f"{seconds}秒"
        elif seconds < 3600:
            return f"{seconds//60}分{seconds%60}秒"
        else:
            return f"{seconds//3600}小时{(seconds%3600)//60}分{seconds%3600%60}秒"

    @staticmethod
    def validate_delete_timeout(message_type: str = None) -> int:
        timeouts = AUTO_DELETE_SETTINGS['timeouts']
        return timeouts.get(message_type, timeouts['default']) if AUTO_DELETE_SETTINGS['enabled'] else 0

    @staticmethod
    def is_auto_delete_exempt(role: str, command: str = None) -> bool:
        if role in AUTO_DELETE_SETTINGS['exempt_roles']:
            return True
        if command and command in AUTO_DELETE_SETTINGS['exempt_command_prefixes']:
            return True
        return False

    @staticmethod
    def get_message_metadata(message: Message) -> Dict:
        metadata = {'type': 'text', 'size': 0, 'duration': 0}
        if not message:
            return metadata
        if message.text:
            metadata['type'] = 'text'
            metadata['size'] = len(message.text)
        elif message.photo:
            metadata['type'] = 'photo'
            metadata['size'] = message.photo[-1].file_size
        elif message.video:
            metadata['type'] = 'video'
            metadata['size'] = message.video.file_size
            metadata['duration'] = message.video.duration
        elif message.document:
            metadata['type'] = 'document'
            metadata['size'] = message.document.file_size
        return metadata

    @staticmethod
    def parse_command_args(message: Message) -> List[str]:
        if not message or not message.text:
            return []
        parts = message.text.split()
        return parts[1:] if len(parts) > 1 else []

    @staticmethod
    def escape_markdown(text: str) -> str:
        escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in escape_chars:
            text = text.replace(char, '\\' + char)
        return text

    @staticmethod
    def verify_environment():
        required_vars = {
            'TELEGRAM_TOKEN': '机器人令牌',
            'MONGODB_URI': 'MongoDB连接URI',
            'MONGODB_DB': 'MongoDB数据库名',
            'WEBHOOK_DOMAIN': 'Webhook域名'
        }
        missing = [f"{var} ({desc})" for var, desc in required_vars.items() if not os.getenv(var)]
        if missing:
            raise ValueError(f"缺少必要的环境变量: {', '.join(missing)}")

# 数据库模块
class Database:
    def __init__(self):
        self.client = None
        self.db = None

    async def connect(self, uri: str, db_name: str) -> bool:
        try:
            from motor.motor_asyncio import AsyncIOMotorClient
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client[db_name]
            await self.db.command('ping')
            logger.info(f"成功连接到数据库 {db_name}")
            return True
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            return False

    async def close(self):
        if self.client:
            self.client.close()
            logger.info("数据库连接已关闭")

    async def get_user(self, user_id: int) -> Optional[Dict]:
        return await self.db.users.find_one({'user_id': user_id})

    async def add_user(self, user_data: Dict) -> bool:
        try:
            user_id = user_data['user_id']
            result = await self.db.users.update_one(
                {'user_id': user_id},
                {'$set': user_data},
                upsert=True
            )
            return result.acknowledged
        except Exception as e:
            logger.error(f"添加用户错误: {e}")
            return False

    async def remove_user(self, user_id: int) -> bool:
        try:
            result = await self.db.users.delete_one({'user_id': user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除用户错误: {e}")
            return False

    async def get_users_by_role(self, role: str) -> List[Dict]:
        return await self.db.users.find({'role': role}).to_list(None)

    async def is_user_banned(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return user and user.get('banned', False)

    async def get_group(self, group_id: int) -> Optional[Dict]:
        return await self.db.groups.find_one({'group_id': group_id})

    async def add_group(self, group_data: Dict) -> bool:
        try:
            group_id = group_data['group_id']
            result = await self.db.groups.update_one(
                {'group_id': group_id},
                {'$set': group_data},
                upsert=True
            )
            return result.acknowledged
        except Exception as e:
            logger.error(f"添加群组错误: {e}")
            return False

    async def remove_group(self, group_id: int) -> bool:
        try:
            result = await self.db.groups.delete_one({'group_id': group_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除群组错误: {e}")
            return False

    async def find_all_groups(self) -> List[Dict]:
        return await self.db.groups.find().to_list(None)

    async def can_manage_group(self, user_id: int, group_id: int) -> bool:
        user = await self.get_user(user_id)
        if not user:
            return False
        if user['role'] == UserRole.SUPERADMIN.value:
            return True
        if user['role'] == UserRole.ADMIN.value:
            group = await self.get_group(group_id)
            return group is not None
        return False

    async def get_manageable_groups(self, user_id: int) -> List[Dict]:
        user = await self.get_user(user_id)
        if not user:
            return []
        if user['role'] == UserRole.SUPERADMIN.value or user['role'] == UserRole.ADMIN.value:
            return await self.find_all_groups()
        return []

    async def get_group_settings(self, group_id: int) -> Dict:
        group = await self.get_group(group_id)
        if not group:
            return {}
        settings = group.get('settings', {})
        return {**DEFAULT_SETTINGS, **settings}

    async def update_group_settings(self, group_id: int, settings: Dict) -> bool:
        try:
            result = await self.db.groups.update_one(
                {'group_id': group_id},
                {'$set': {'settings': settings}}
            )
            return result.acknowledged
        except Exception as e:
            logger.error(f"更新群组设置错误: {e}")
            return False

    async def get_keywords(self, group_id: int) -> List[Dict]:
        return await self.db.keywords.find({'group_id': group_id}).to_list(None)

    async def add_keyword(self, keyword_data: Dict) -> ObjectId:
        result = await self.db.keywords.insert_one(keyword_data)
        return result.inserted_id

    async def remove_keyword(self, group_id: int, keyword_id: str) -> bool:
        try:
            result = await self.db.keywords.delete_one({
                'group_id': group_id,
                '_id': ObjectId(keyword_id)
            })
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除关键词错误: {e}")
            return False

    async def add_message_stat(self, stat_data: Dict) -> bool:
        try:
            stat_data['timestamp'] = datetime.now(beijing_tz)
            result = await self.db.message_stats.update_one(
                {
                    'group_id': stat_data['group_id'],
                    'user_id': stat_data['user_id'],
                    'date': stat_data['date'],
                    'media_type': stat_data['media_type']
                },
                {
                    '$inc': {
                        'total_messages': stat_data['total_messages'],
                        'total_size': stat_data['total_size']
                    },
                    '$set': {'timestamp': stat_data['timestamp']}
                },
                upsert=True
            )
            return result.acknowledged
        except Exception as e:
            logger.error(f"添加消息统计错误: {e}")
            return False

    async def get_recent_message_count(self, user_id: int, seconds: int = 60) -> int:
        try:
            time_threshold = datetime.now(beijing_tz) - timedelta(seconds=seconds)
            count = await self.db.message_stats.count_documents({
                'user_id': user_id,
                'timestamp': {'$gte': time_threshold}
            })
            return count
        except Exception as e:
            logger.error(f"获取最近消息数量错误: {e}")
            return 0

    async def cleanup_old_stats(self, days: int = 30) -> bool:
        try:
            cutoff_date = (datetime.now(beijing_tz) - timedelta(days=days)).strftime('%Y-%m-%d')
            result = await self.db.message_stats.delete_many({
                'date': {'$lt': cutoff_date}
            })
            logger.info(f"已清理 {result.deleted_count} 条旧统计数据")
            return True
        except Exception as e:
            logger.error(f"清理旧统计数据错误: {e}")
            return False

# 设置管理模块
class SettingsManager:
    def __init__(self, db):
        self.db = db
        self._states = {}
        self._locks = {}
        self._state_locks = {}
        self._cleanup_task = None
        self._max_states_per_user = STATE_MANAGEMENT_SETTINGS['max_concurrent_states']

    async def start(self):
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("状态管理器已启动")

    async def stop(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("状态管理器已停止")

    async def _get_state_lock(self, user_id: int):
        if user_id not in self._state_locks:
            self._state_locks[user_id] = asyncio.Lock()
        return self._state_locks[user_id]

    async def _cleanup_loop(self):
        while True:
            try:
                now = datetime.now(beijing_tz)
                expired_keys = []
                async with asyncio.Lock():
                    for key, state in list(self._states.items()):
                        if (now - state['timestamp']).total_seconds() > STATE_MANAGEMENT_SETTINGS['state_timeout']:
                            expired_keys.append(key)
                for key in expired_keys:
                    logger.info(f"清理过期状态: {key}")
                    await self._cleanup_state(key)
                await asyncio.sleep(STATE_MANAGEMENT_SETTINGS['cleanup_interval'])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"状态清理错误: {e}")
                await asyncio.sleep(60)

    async def _cleanup_state(self, key: str):
        if key in self._states:
            del self._states[key]
        if key in self._locks:
            del self._locks[key]
        logger.info(f"状态已清理: {key}")

    async def get_current_page(self, group_id: int, section: str) -> int:
        state_key = f"page_{group_id}_{section}"
        async with asyncio.Lock():
            state = self._states.get(state_key, {})
            return state.get('page', 1)

    async def set_current_page(self, group_id: int, section: str, page: int):
        state_key = f"page_{group_id}_{section}"
        async with asyncio.Lock():
            self._states[state_key] = {
                'page': page,
                'timestamp': datetime.now(beijing_tz)
            }
            logger.info(f"设置页码: {state_key} => {page}")

    async def start_setting(self, user_id: int, setting_type: str, group_id: int):
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
                'timestamp': datetime.now(beijing_tz)
            }
            logger.info(f"创建设置状态: {state_key}, 群组: {group_id}")

    async def get_setting_state(self, user_id: int, setting_type: str) -> Optional[dict]:
        async with asyncio.Lock():
            state_key = f"setting_{user_id}_{setting_type}"
            state = self._states.get(state_key)
            logger.info(f"获取状态: {state_key} => {state}")
            return state

    async def update_setting_state(self, user_id: int, setting_type: str, data: dict, next_step: bool = False):
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
            self._states[state_key]['timestamp'] = datetime.now(beijing_tz)
            logger.info(f"更新状态: {state_key}, 步骤: {self._states[state_key]['step']}, 数据: {self._states[state_key]['data']}")

    async def clear_setting_state(self, user_id: int, setting_type: str):
        state_key = f"setting_{user_id}_{setting_type}"
        state_lock = await self._get_state_lock(user_id)
        async with state_lock:
            if state_key in self._states:
                await self._cleanup_state(state_key)
                logger.info(f"清除设置状态: {state_key}")

    async def get_active_settings(self, user_id: int) -> list:
        async with asyncio.Lock():
            settings = [k.split('_')[2] for k in self._states if k.startswith(f"setting_{user_id}")]
            logger.info(f"用户 {user_id} 的活动设置: {settings}")
            return settings

    async def check_setting_conflict(self, user_id: int, setting_type: str) -> bool:
        async with asyncio.Lock():
            conflicts = [k for k in self._states if k.startswith(f"setting_{user_id}") and setting_type in k]
            has_conflict = bool(conflicts)
            if has_conflict:
                logger.warning(f"检测到设置冲突: 用户 {user_id}, 类型 {setting_type}, 冲突: {conflicts}")
            return has_conflict

# 统计管理模块
class StatsManager:
    def __init__(self, db):
        self.db = db

    async def add_message_stat(self, group_id: int, user_id: int, message: Message):
        media_type = Utils.get_media_type(message)
        message_size = len(message.text or '') if message.text else 0
        if media_type in ['photo', 'video', 'document'] and message.effective_attachment:
            try:
                file_size = getattr(message.effective_attachment, 'file_size', 0) or 0
                message_size += file_size
            except Exception:
                pass
        stat_data = {
            'group_id': group_id,
            'user_id': user_id,
            'date': datetime.now(beijing_tz).strftime('%Y-%m-%d'),
            'total_messages': 1,
            'total_size': message_size,
            'media_type': media_type
        }
        await self.db.add_message_stat(stat_data)

    async def get_daily_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        today = datetime.now(beijing_tz).strftime('%Y-%m-%d')
        limit = DEFAULT_SETTINGS['daily_rank_size']
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
        thirty_days_ago = (datetime.now(beijing_tz) - timedelta(days=30)).strftime('%Y-%m-%d')
        limit = DEFAULT_SETTINGS['monthly_rank_size']
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
        return await self.db.db.broadcasts.find({'group_id': group_id}).to_list(None)

    async def add_broadcast(self, broadcast_data: Dict) -> ObjectId:
        result = await self.db.db.broadcasts.insert_one(broadcast_data)
        return result.inserted_id

    async def remove_broadcast(self, group_id: int, broadcast_id: str) -> bool:
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
        now = datetime.now(beijing_tz)
        broadcasts = await self.db.db.broadcasts.find({
            'start_time': {'$lte': now},
            'end_time': {'$gt': now},
            '$or': [
                {'last_broadcast': {'$exists': False}},
                {'last_broadcast': {'$exists': True}}
            ]
        }).to_list(None)
        return [b for b in broadcasts if 'last_broadcast' not in b or (now - b['last_broadcast']).total_seconds() >= b['interval']]

    async def update_last_broadcast(self, broadcast_id: ObjectId) -> bool:
        try:
            result = await self.db.db.broadcasts.update_one(
                {'_id': broadcast_id},
                {'$set': {'last_broadcast': datetime.now(beijing_tz)}}
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

    def register_built_in_keyword(self, pattern: str, handler: Callable):
        self._built_in_keywords[pattern] = handler

    async def match_keyword(self, group_id: int, text: str, message: Message) -> Optional[str]:
        for pattern, handler in self._built_in_keywords.items():
            if text == pattern:
                return await handler(message)
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            try:
                if kw['type'] == 'regex':
                    if re.search(kw['pattern'], text):
                        return self._format_response(kw)
                else:
                    if text == kw['pattern']:
                        return self._format_response(kw)
            except Exception as e:
                logger.error(f"匹配关键词 {kw['pattern']} 错误: {e}")
                continue
        return None

    def _format_response(self, keyword: dict) -> str:
        if keyword['response_type'] == 'text':
            return keyword['response']
        elif keyword['response_type'] in ['photo', 'video', 'document']:
            return f"__media__{keyword['response_type']}__{keyword['response']}"
        return "❌ 不支持的回复类型"

    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        return await self.db.get_keywords(group_id)

    async def get_keyword_by_id(self, group_id: int, keyword_id: str) -> Optional[Dict[str, Any]]:
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
        'start': {'usage': '/start', 'description': '启动机器人并查看功能列表', 'example': None, 'admin_only': False},
        'settings': {'usage': '/settings', 'description': '打开设置菜单', 'example': None, 'admin_only': True},
        'tongji': {'usage': '/tongji [页码]', 'description': '查看今日统计排行', 'example': '/tongji 2', 'admin_only': False},
        'tongji30': {'usage': '/tongji30 [页码]', 'description': '查看30日统计排行', 'example': '/tongji30 2', 'admin_only': False},
        'addadmin': {'usage': '/addadmin <用户ID>', 'description': '添加管理员', 'example': '/addadmin 123456789', 'admin_only': True},
        'deladmin': {'usage': '/deladmin <用户ID>', 'description': '删除管理员', 'example': '/deladmin 123456789', 'admin_only': True},
        'authgroup': {'usage': '/authgroup <群组ID> ...', 'description': '授权群组', 'example': '/authgroup -100123456789 keywords stats broadcast', 'admin_only': True},
        'deauthgroup': {'usage': '/deauthgroup <群组ID>', 'description': '取消群组授权', 'example': '/deauthgroup -100123456789', 'admin_only': True}
    }

    @classmethod
    def get_usage(cls, command: str) -> Optional[dict]:
        return cls.COMMAND_USAGE.get(command)

    @classmethod
    def format_usage(cls, command: str) -> str:
        usage = cls.get_usage(command)
        if not usage:
            return "❌ 未知命令"
        text = [f"📝 命令: {command}", f"用法: {usage['usage']}", f"说明: {usage['description']}"]
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
        self.message_deletion_manager = None
        self.session = None

    class MessageDeletionManager:
        def __init__(self, bot):
            self.bot = bot
            self.deletion_tasks = {}

        async def schedule_message_deletion(self, message: Message, timeout: int, delete_trigger: bool = False):
            if timeout <= 0 or not AUTO_DELETE_SETTINGS['enabled']:
                return
            task_key = f"delete_message_{message.chat.id}_{message.message_id}"
            async def delete_message_task():
                try:
                    await asyncio.sleep(timeout)
                    if delete_trigger and message.reply_to_message:
                        await message.reply_to_message.delete()
                    await message.delete()
                except Exception as e:
                    logger.warning(f"消息删除错误: {e}")
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

    async def initialize(self):
        try:
            Utils.verify_environment()
            logger.info("开始初始化机器人")

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
            self.message_deletion_manager = self.MessageDeletionManager(self)

            # 注册特殊关键词
            self.keyword_manager.register_built_in_keyword("日排行", self._handle_daily_ranking_keyword)
            self.keyword_manager.register_built_in_keyword("月排行", self._handle_monthly_ranking_keyword)

            for admin_id in DEFAULT_SUPERADMINS:
                await self.db.add_user({'user_id': admin_id, 'role': UserRole.SUPERADMIN.value})
                logger.info(f"已设置超级管理员: {admin_id}")

            for group in DEFAULT_GROUPS:
                await self.db.add_group(group)
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
            site = web.TCPSite(self.web_runner, WEB_HOST, WEB_PORT)
            await site.start()
            logger.info(f"Web服务器已在 {WEB_HOST}:{WEB_PORT} 启动")

            await self.application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
            self.application.updater = None
            logger.info(f"Webhook已设置为 {webhook_url}")

            self.session = ClientSession()
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
        try:
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
            return True
        except Exception as e:
            logger.error(f"验证初始化失败: {e}")
            return False

    @classmethod
    async def main(cls):
        try:
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
        except Exception as e:
            logger.error(f"启动失败: {e}")
            raise

    async def start(self):
        try:
            if not self.application:
                logger.error("机器人未初始化")
                return False
            await self.application.initialize()
            await self.application.start()
            self.running = True
            await self._start_broadcast_task()
            await self._start_cleanup_task()
            await self._start_keep_alive_task()
            logger.info("机器人成功启动")
            return True
        except Exception as e:
            logger.error(f"机器人启动失败: {e}")
            return False

    async def stop(self):
        try:
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
                if getattr(self.application, 'running', False):
                    await self.application.stop()
                    await self.application.shutdown()
            if self.session:
                await self.session.close()
            if self.db:
                await self.db.close()
        except Exception as e:
            logger.error(f"停止机器人时出错: {e}")

    async def _start_broadcast_task(self):
        async def broadcast_routine():
            while self.running:
                try:
                    broadcasts = await self.broadcast_manager.get_pending_broadcasts()
                    for broadcast in broadcasts:
                        try:
                            group = await self.db.get_group(broadcast['group_id'])
                            if not group or not group['feature_switches'].get('broadcast', True):
                                continue
                            if broadcast.get('text'):
                                await self.application.bot.send_message(broadcast['group_id'], broadcast['text'])
                            if broadcast.get('media'):
                                media_type = broadcast.get('media_type', 'photo')
                                media_methods = {
                                    'photo': self.application.bot.send_photo,
                                    'video': self.application.bot.send_video,
                                    'document': self.application.bot.send_document
                                }
                                if media_type in media_methods:
                                    await media_methods[media_type](broadcast['group_id'], broadcast['media'])
                            await self.broadcast_manager.update_last_broadcast(broadcast['_id'])
                            await self.message_deletion_manager.schedule_message_deletion(
                                await self.application.bot.send_message(broadcast['group_id'], f"轮播: {broadcast['name']}"),
                                AUTO_DELETE_SETTINGS['timeouts']['broadcast']
                            )
                        except Exception as e:
                            logger.error(f"发送轮播消息 {broadcast.get('name', '未知')} 时出错: {e}")
                    await asyncio.sleep(BROADCAST_SETTINGS['check_interval'])
                except Exception as e:
                    logger.error(f"轮播任务出错: {e}")
                    await asyncio.sleep(60)
        asyncio.create_task(broadcast_routine())

    async def _start_cleanup_task(self):
        async def cleanup_routine():
            while self.running:
                try:
                    await self.db.cleanup_old_stats(days=DEFAULT_SETTINGS.get('cleanup_days', 30))
                    await asyncio.sleep(24 * 60 * 60)
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
                    await asyncio.sleep(1 * 60 * 60)
        self.cleanup_task = asyncio.create_task(cleanup_routine())

    async def _start_keep_alive_task(self):
        async def keep_alive_routine():
            while self.running:
                try:
                    await self.session.get(f"http://{WEB_HOST}:{WEB_PORT}/health")
                    logger.info("发送防休眠请求")
                    await asyncio.sleep(KEEP_ALIVE_INTERVAL)
                except Exception as e:
                    logger.error(f"防休眠任务出错: {e}")
                    await asyncio.sleep(60)
        asyncio.create_task(keep_alive_routine())

    async def handle_signals(self):
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_running_loop().add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.stop())
                )
            logger.info("信号处理器已设置")
        except NotImplementedError:
            logger.warning("此平台不支持信号处理器")

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
        return group and permission.value in group.get('permissions', []) and group.get('feature_switches', {}).get(permission.value, True)

    async def _register_handlers(self):
        message_middleware = MessageMiddleware(self)
        error_middleware = ErrorHandlingMiddleware(self.error_handler)
        register_middleware(self.application, [message_middleware, error_middleware])

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
        self.application.add_handler(CommandHandler("checkconfig", self._handle_check_config))

        self.application.add_handler(CallbackQueryHandler(self._handle_settings_callback, pattern=r'^settings_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_keyword_callback, pattern=r'^keyword_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_broadcast_callback, pattern=r'^broadcast_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_keyword_continue_callback, pattern=r'^keyword_continue_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_stats_edit_callback, pattern=r'^stats_edit_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_feature_switch_callback, pattern=r'^feature_switch_'))

        self.application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self._handle_message))

    @handle_callback_errors
    async def _handle_keyword_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        try:
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
                await self.settings_manager.start_setting(update.effective_user.id, 'keyword', group_id)
                await self.settings_manager.update_setting_state(update.effective_user.id, 'keyword', {'match_type': match_type})
                match_type_text = "精确匹配" if match_type == "exact" else "正则匹配"
                await query.edit_message_text(
                    f"您选择了{match_type_text}方式\n\n请发送关键词内容：\n{'(支持正则表达式)' if match_type == 'regex' else ''}\n\n发送 /cancel 取消"
                )

            elif action == "detail":
                keyword_id = parts[2]
                keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                if not keyword:
                    await query.edit_message_text("❌ 未找到该关键词")
                    return
                pattern = keyword['pattern']
                response_type = keyword['response_type']
                match_type = keyword['type']
                response_preview = "无法预览媒体内容" if response_type != 'text' else (
                    keyword['response'][:100] + "..." if len(keyword['response']) > 100 else keyword['response']
                )
                response_type_text = {'text': '文本', 'photo': '图片', 'video': '视频', 'document': '文件'}.get(response_type, response_type)
                keyboard = [
                    [InlineKeyboardButton("❌ 删除此关键词", callback_data=f"keyword_delete_{keyword_id}_{group_id}")],
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

            elif action == "delete":
                keyword_id = parts[2]
                keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                if not keyword:
                    await query.edit_message_text("❌ 未找到该关键词")
                    return
                await self.db.remove_keyword(group_id, keyword_id)
                await query.edit_message_text(f"✅ 已删除关键词「{keyword['pattern']}」")
                await asyncio.sleep(1)
                await self._show_keyword_settings(query, group_id)

            elif action == "list_page":
                page = int(parts[2])
                await self.settings_manager.set_current_page(group_id, "keywords", page)
                await self._show_keyword_settings(query, group_id, page)

        except Exception as e:
            logger.error(f"处理关键词回调错误: {e}")
            await query.edit_message_text("❌ 处理关键词设置时出错，请重试")

    @handle_callback_errors
    async def _handle_keyword_continue_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        try:
            group_id = int(query.data.split('_')[2])
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return
            keyboard = [
                [InlineKeyboardButton("精确匹配", callback_data=f"keyword_type_exact_{group_id}"),
                 InlineKeyboardButton("正则匹配", callback_data=f"keyword_type_regex_{group_id}")],
                [InlineKeyboardButton("取消", callback_data=f"settings_keywords_{group_id}")]
            ]
            await query.edit_message_text("请选择关键词匹配类型：", reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"处理关键词继续添加回调错误: {e}")
            await query.edit_message_text("❌ 处理操作时出错，请重试")

    @handle_callback_errors
    async def _handle_settings_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        try:
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
                group = await self.db.get_group(group_id)
                switches = group.get('feature_switches', {})
                if 'stats' in group.get('permissions', []):
                    keyboard.append([InlineKeyboardButton("📊 统计设置", callback_data=f"settings_stats_{group_id}")])
                    keyboard.append([InlineKeyboardButton(f"统计功能: {'开' if switches.get('stats', True) else '关'}", callback_data=f"feature_switch_stats_{group_id}")])
                if 'broadcast' in group.get('permissions', []):
                    keyboard.append([InlineKeyboardButton("📢 轮播消息", callback_data=f"settings_broadcast_{group_id}")])
                    keyboard.append([InlineKeyboardButton(f"轮播功能: {'开' if switches.get('broadcast', True) else '关'}", callback_data=f"feature_switch_broadcast_{group_id}")])
                if 'keywords' in group.get('permissions', []):
                    keyboard.append([InlineKeyboardButton("🔑 关键词设置", callback_data=f"settings_keywords_{group_id}")])
                    keyboard.append([InlineKeyboardButton(f"关键词功能: {'开' if switches.get('keywords', True) else '关'}", callback_data=f"feature_switch_keywords_{group_id}")])
                keyboard.append([InlineKeyboardButton("🔙 返回群组列表", callback_data="show_manageable_groups")])
                await query.edit_message_text(f"群组 {group_id} 的设置菜单\n请选择要管理的功能：", reply_markup=InlineKeyboardMarkup(keyboard))

            elif action in ["stats", "broadcast", "keywords"]:
                await self._handle_settings_section(query, context, group_id, action)

        except Exception as e:
            logger.error(f"处理设置回调错误: {e}")
            await query.edit_message_text("❌ 处理设置操作时出错")

    @handle_callback_errors
    async def _handle_broadcast_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        try:
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
                    "请发送要轮播的内容：\n支持文本和多媒体（图片/视频/文件）\n\n发送 /cancel 取消"
                )

            elif action == "detail":
                broadcast_id = ObjectId(parts[2])
                broadcast = await self.db.db.broadcasts.find_one({'_id': broadcast_id, 'group_id': group_id})
                if not broadcast:
                    await query.edit_message_text("❌ 未找到该轮播消息")
                    return
                content_preview = (broadcast.get('text', '')[:50] + "..." if len(broadcast.get('text', '')) > 50 else broadcast.get('text', '无文本内容'))
                if broadcast.get('media'):
                    content_preview += "\n[多媒体内容]"
                start_time = broadcast['start_time'].astimezone(beijing_tz).strftime('%Y-%m-%d %H:%M')
                end_time = broadcast['end_time'].astimezone(beijing_tz).strftime('%Y-%m-%d %H:%M')
                interval = Utils.format_duration(broadcast['interval'])
                text = (
                    f"📢 轮播消息详情：\n\n"
                    f"🔹 名称：{broadcast.get('name', '未命名')}\n"
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
                broadcast_id = ObjectId(parts[2])
                await self.db.db.broadcasts.delete_one({'_id': broadcast_id, 'group_id': group_id})
                await query.edit_message_text("✅ 已删除轮播消息")
                await asyncio.sleep(1)
                await self._show_broadcast_settings(query, group_id)

        except Exception as e:
            logger.error(f"处理轮播消息回调错误: {e}")
            await query.edit_message_text("❌ 处理轮播消息设置时出错，请重试")

    @handle_callback_errors
    async def _handle_stats_edit_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        try:
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
            else:
                await query.edit_message_text(f"❌ 未知的设置类型: {setting_type}")
        except Exception as e:
            logger.error(f"处理统计设置编辑回调错误: {e}")
            await query.edit_message_text("❌ 处理设置时出错，请重试")

    @handle_callback_errors
    async def _handle_feature_switch_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        try:
            parts = query.data.split('_')
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的操作")
                return
            feature = parts[2]
            group_id = int(parts[3])
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return
            group = await self.db.get_group(group_id)
            switches = group.get('feature_switches', {})
            switches[feature] = not switches.get(feature, True)
            await self.db.add_group({**group, 'feature_switches': switches})
            await self._handle_settings_callback(update, context)
        except Exception as e:
            logger.error(f"处理功能开关回调错误: {e}")
            await query.edit_message_text("❌ 处理功能开关时出错，请重试")

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
                "✅ /authgroup <群组ID> ... - 授权群组\n"
                "❌ /deauthgroup <群组ID> - 取消群组授权\n"
                "🔍 /checkconfig - 检查当前配置\n"
            )
        welcome_text += "\n如需帮助，请联系管理员。"
        await update.message.reply_text(welcome_text)

    @check_command_usage
    async def _handle_settings(self, update: Update, context):
        try:
            manageable_groups = await self.db.get_manageable_groups(update.effective_user.id)
            if not manageable_groups:
                await update.message.reply_text("❌ 你没有权限管理任何群组")
                return
            keyboard = [
                [InlineKeyboardButton(
                    (await context.bot.get_chat(group['group_id'])).title or f"群组 {group['group_id']}",
                    callback_data=f"settings_select_{group['group_id']}"
                )] for group in manageable_groups
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("请选择要管理的群组：", reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"设置命令处理错误: {e}")
            await update.message.reply_text("❌ 处理设置命令时出错")

    @check_command_usage
    async def _handle_rank_command(self, update: Update, context):
        if not update.effective_chat or not update.effective_user or not update.message:
            return
        try:
            command = update.message.text.split('@')[0][1:]
            group_id = update.effective_chat.id
            page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
            if page < 1:
                await update.message.reply_text("❌ 无效的页码")
                return

            stats, total_pages = (
                await self.stats_manager.get_daily_stats(group_id, page) if command == "tongji"
                else await self.stats_manager.get_monthly_stats(group_id, page)
            )
            title = "📊 今日发言排行" if command == "tongji" else "📊 近30天发言排行"
            if not stats:
                await update.message.reply_text("📊 暂无统计数据")
                return

            text = f"{title}\n\n"
            for i, stat in enumerate(stats, start=(page-1)*15+1):
                try:
                    user = await context.bot.get_chat_member(group_id, stat['_id'])
                    name = Utils.truncate_name(user.user.full_name or user.user.username or f"用户{stat['_id']}")
                except Exception:
                    name = Utils.truncate_name(f"用户{stat['_id']}")
                text += f"{i}. {name} - {stat['total_messages']}条\n"
            text += f"\n第 {page}/{total_pages} 页"
            if total_pages > 1:
                text += f"\n使用 /{command} <页码> 查看其他页"

            keyboard = self._create_navigation_keyboard(page, total_pages, f"{'today' if command == 'tongji' else 'monthly'}_{group_id}")
            sent_message = await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)
            await self.message_deletion_manager.schedule_message_deletion(sent_message, AUTO_DELETE_SETTINGS['timeouts']['ranking'])
        except Exception as e:
            logger.error(f"处理排行命令错误: {e}")
            await update.message.reply_text("❌ 获取排行榜时出错")

    @check_command_usage
    async def _handle_admin_groups(self, update: Update, context):
        if not update.effective_user or not update.message or not await self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ 只有管理员可以使用此命令")
            return
        try:
            groups = await self.db.get_manageable_groups(update.effective_user.id)
            if not groups:
                await update.message.reply_text("📝 你目前没有可管理的群组")
                return
            text = "📝 你可以管理的群组：\n\n"
            for group in groups:
                group_name = (await context.bot.get_chat(group['group_id'])).title or f"群组 {group['group_id']}"
                text += f"• {group_name}\n  ID: {group['group_id']}\n  权限: {', '.join(group.get('permissions', []))}\n\n"
            await update.message.reply_text(text)
        except Exception as e:
            logger.error(f"列出管理员群组错误: {e}")
            await update.message.reply_text("❌ 获取群组列表时出错")

    @check_command_usage
    async def _handle_add_admin(self, update: Update, context):
        if not update.effective_user or not update.message or not await self.is_superadmin(update.effective_user.id):
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
        if not update.effective_user or not update.message or not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除管理员")
            return
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deladmin <用户ID>")
            return
        try:
            user_id = int(context.args[0])
            user = await self.db.get_user(user_id)
            if not user or user['role'] == UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 该用户不是管理员或为超级管理员")
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
        if not update.effective_user or not update.message or not await self.is_superadmin(update.effective_user.id):
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
        if not update.effective_user or not update.message or not await self.is_superadmin(update.effective_user.id):
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
        if not update.effective_user or not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以查看配置")
            return
        try:
            superadmins = await self.db.get_users_by_role(UserRole.SUPERADMIN.value)
            groups = await self.db.find_all_groups()
            config_text = "🔧 当前配置信息：\n\n👥 超级管理员：\n" + "\n".join(f"• {user['user_id']}" for user in superadmins)
            config_text += "\n\n📋 已授权群组：\n" + "\n".join(f"• 群组 {group['group_id']}\n  权限: {', '.join(group.get('permissions', []))}" for group in groups)
            await update.message.reply_text(config_text)
        except Exception as e:
            logger.error(f"检查配置出错: {e}")
            await update.message.reply_text("❌ 获取配置信息时出错")

    async def _handle_auth_group(self, update: Update, context):
        if not update.effective_user or not update.message or not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以授权群组")
            return
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：\n/authgroup <群组ID>")
            return
        try:
            group_id = int(context.args[0])
            group_info = await context.bot.get_chat(group_id)
            group_name = group_info.title
            all_permissions = [perm.value for perm in GroupPermission]
            await self.db.add_group({
                'group_id': group_id,
                'permissions': all_permissions,
                'feature_switches': {'keywords': True, 'stats': True, 'broadcast': True}
            })
            await update.message.reply_text(f"✅ 已授权群组\n群组：{group_name}\nID：{group_id}\n已启用全部功能")
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字")
        except Exception as e:
            logger.error(f"授权群组错误: {e}")
            await update.message.reply_text("❌ 授权群组时出错")

    @check_command_usage
    async def _handle_deauth_group(self, update: Update, context):
        if not update.effective_user or not update.message or not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以解除群组授权")
            return
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deauthgroup <群组ID>")
            return
        try:
            group_id = int(context.args[0])
            if not await self.db.get_group(group_id):
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
        try:
            manageable_groups = await self.db.get_manageable_groups(update.effective_user.id)
            if not manageable_groups:
                await query.edit_message_text("❌ 你没有权限管理任何群组")
                return
            keyboard = [
                [InlineKeyboardButton(
                    (await context.bot.get_chat(group['group_id'])).title or f"群组 {group['group_id']}",
                    callback_data=f"settings_select_{group['group_id']}"
                )] for group in manageable_groups
            ]
            await query.edit_message_text("请选择要管理的群组：", reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"显示可管理群组错误: {e}")
            await query.edit_message_text("❌ 获取群组列表时出错")

    async def _handle_settings_section(self, query, context, group_id: int, section: str):
        try:
            if section == "stats":
                settings = await self.db.get_group_settings(group_id)
                await self._show_stats_settings(query, group_id, settings)
            elif section == "broadcast":
                await self._show_broadcast_settings(query, group_id)
            elif section == "keywords":
                await self._show_keyword_settings(query, group_id)
        except Exception as e:
            logger.error(f"处理设置分区显示错误: {e}")
            await query.edit_message_text("❌ 显示设置分区时出错")

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
        broadcasts = await self.db.db.broadcasts.find({'group_id': group_id}).to_list(None)
        keyboard = [
            [InlineKeyboardButton(
                f"📢 {b.get('name', '未命名')}: {(b.get('text', '')[:20] + '...' if len(b.get('text', '')) > 20 else b.get('text', '无文本'))}{' [多媒体]' if b.get('media') else ''}",
                callback_data=f"broadcast_detail_{b['_id']}_{group_id}"
            )] for b in broadcasts
        ]
        keyboard.extend([
            [InlineKeyboardButton("➕ 添加轮播消息", callback_data=f"broadcast_add_{group_id}")],
            [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
        ])
        await query.edit_message_text(f"群组 {group_id} 的轮播消息设置", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_keyword_settings(self, query, group_id: int, page: int = 1):
        keywords = await self.db.get_keywords(group_id)
        total_pages = (len(keywords) + 9) // 10
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * 10
        end_idx = min(start_idx + 10, len(keywords))
        page_keywords = keywords[start_idx:end_idx]
        keyboard = [
            [InlineKeyboardButton(
                f"🔑 {kw['pattern'][:20] + '...' if len(kw['pattern']) > 20 else kw['pattern']}",
                callback_data=f"keyword_detail_{kw['_id']}_{group_id}"
            )] for kw in page_keywords
        ]
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("◀️ 上一页", callback_data=f"keyword_list_page_{page-1}_{group_id}"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("下一页 ▶️", callback_data=f"keyword_list_page_{page+1}_{group_id}"))
            if nav_buttons:
                keyboard.append(nav_buttons)
        keyboard.extend([
            [InlineKeyboardButton("➕ 添加关键词", callback_data=f"keyword_add_{group_id}")],
            [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
        ])
        text = f"群组 {group_id} 的关键词设置" + (f"\n第 {page}/{total_pages} 页" if total_pages > 1 else "")
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def _process_stats_setting(self, update: Update, context, stats_state, setting_type):
        try:
            if not stats_state:
                await update.message.reply_text("❌ 设置会话已过期，请重新开始")
                return
            group_id = stats_state.get('group_id')
            value = int(update.message.text)
            if value < 0 and setting_type != 'stats_min_bytes':
                await update.message.reply_text("❌ 值不能为负")
                return
            settings = await self.db.get_group_settings(group_id)
            if setting_type == 'stats_min_bytes':
                settings['min_bytes'] = value
                tips = f"最小统计字节数已设置为 {value} 字节"
            elif setting_type == 'stats_daily_rank':
                if not 5 <= value <= 20:
                    await update.message.reply_text("❌ 显示数量必须在5-20之间")
                    return
                settings['daily_rank_size'] = value
                tips = f"日排行显示数量已设置为 {value}"
            elif setting_type == 'stats_monthly_rank':
                if not 5 <= value <= 20:
                    await update.message.reply_text("❌ 显示数量必须在5-20之间")
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
        except Exception as e:
            logger.error(f"处理统计设置错误: {e}")
            await update.message.reply_text("❌ 更新设置时出错")
            await self.settings_manager.clear_setting_state(update.effective_user.id, setting_type)

    async def _process_keyword_adding(self, update: Update, context, setting_state):
        try:
            if not setting_state:
                await update.message.reply_text("❌ 设置会话已过期，请重新开始")
                return
            step = setting_state.get('step', 1)
            group_id = setting_state.get('group_id')
            data = setting_state.get('data', {})
            match_type = data.get('match_type')

            if step == 1:
                pattern = update.message.text.strip()
                if match_type == 'regex' and not Utils.validate_regex(pattern):
                    await update.message.reply_text("❌ 无效的正则表达式")
                    return
                await self.settings_manager.update_setting_state(update.effective_user.id, 'keyword', {'pattern': pattern, 'type': match_type}, next_step=True)
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
                if not response_type or not response_content:
                    await update.message.reply_text("❌ 请发送有效的回复内容（文本/图片/视频/文件）")
                    return
                if response_type == 'text' and len(response_content) > KEYWORD_SETTINGS['max_response_length']:
                    await update.message.reply_text(f"❌ 回复内容过长，请不要超过 {KEYWORD_SETTINGS['max_response_length']} 个字符")
                    return
                keywords = await self.db.get_keywords(group_id)
                if len(keywords) >= KEYWORD_SETTINGS['max_keywords']:
                    await update.message.reply_text(f"❌ 关键词数量已达到上限 {KEYWORD_SETTINGS['max_keywords']} 个")
                    await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
                    return
                await self.db.add_keyword({
                    'group_id': group_id,
                    'pattern': data.get('pattern'),
                    'type': data.get('type'),
                    'response': response_content,
                    'response_type': response_type,
                    'delete_trigger': False  # 默认不删除触发消息，可在设置中调整
                })
                keyboard = [
                    [InlineKeyboardButton("➕ 继续添加关键词", callback_data=f"keyword_continue_{group_id}")],
                    [InlineKeyboardButton("🔙 返回关键词设置", callback_data=f"settings_keywords_{group_id}")]
                ]
                await update.message.reply_text(f"✅ 关键词 「{data.get('pattern')}」 添加成功！", reply_markup=InlineKeyboardMarkup(keyboard))
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')

        except Exception as e:
            logger.error(f"处理关键词添加流程出错: {e}")
            await update.message.reply_text("❌ 添加关键词时出错，请重试")
            await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')

    async def _process_broadcast_adding(self, update: Update, context, setting_state):
        try:
            if not setting_state:
                await update.message.reply_text("❌ 设置会话已过期，请重新开始")
                return
            step = setting_state.get('step', 1)
            group_id = setting_state.get('group_id')
            data = setting_state.get('data', {})

            if step == 1:
                text_content = update.message.text if update.message.text else None
                media_type = None
                media_content = None
                if update.message.photo:
                    media_type = 'photo'
                    media_content = update.message.photo[-1].file_id
                elif update.message.video:
                    media_type = 'video'
                    media_content = update.message.video.file_id
                elif update.message.document:
                    media_type = 'document'
                    media_content = update.message.document.file_id
                if not text_content and not media_content:
                    await update.message.reply_text("❌ 请发送有效的内容（文本/图片/视频/文件）")
                    return
                if text_content and len(text_content) > 4096:
                    await update.message.reply_text("❌ 文本内容过长")
                    return
                await self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'broadcast',
                    {'text': text_content, 'media': media_content, 'media_type': media_type}
                )
                state_key = f"setting_{update.effective_user.id}_broadcast"
                async with asyncio.Lock():
                    if state_key in self.settings_manager._states:
                        self.settings_manager._states[state_key]['step'] = 2
                        self.settings_manager._states[state_key]['timestamp'] = datetime.now(beijing_tz)
                await update.message.reply_text(
                    "✅ 内容已设置\n\n请设置轮播参数：\n格式：名称 开始时间 结束时间 间隔\n例如：欢迎消息 2024-02-22 08:00 2024-03-22 20:00 2小时30分\n\n发送 /cancel 取消"
                )

            elif step == 2:
                parts = update.message.text.split(maxsplit=4)
                if len(parts) != 5:
                    await update.message.reply_text("❌ 参数数量不正确")
                    return
                name, start_date, start_time, end_date, end_time, interval_str = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
                start_time = Utils.validate_time_format(f"{start_date} {start_time}")
                end_time = Utils.validate_time_format(f"{end_date} {end_time}")
                interval = Utils.parse_interval(interval_str)
                if not all([start_time, end_time, interval]):
                    await update.message.reply_text("❌ 时间格式或间隔无效")
                    return
                if start_time >= end_time:
                    await update.message.reply_text("❌ 结束时间必须晚于开始时间")
                    return
                if interval < BROADCAST_SETTINGS['min_interval']:
                    await update.message.reply_text(f"❌ 间隔时间不能小于{BROADCAST_SETTINGS['min_interval']}秒")
                    return
                broadcasts = await self.db.db.broadcasts.count_documents({'group_id': group_id})
                if broadcasts >= BROADCAST_SETTINGS['max_broadcasts']:
                    await update.message.reply_text(f"❌ 轮播消息数量已达到上限 {BROADCAST_SETTINGS['max_broadcasts']} 条")
                    return
                await self.db.db.broadcasts.insert_one({
                    'group_id': group_id,
                    'name': name,
                    'text': data.get('text'),
                    'media': data.get('media'),
                    'media_type': data.get('media_type'),
                    'start_time': start_time,
                    'end_time': end_time,
                    'interval': interval
                })
                await update.message.reply_text("✅ 轮播消息添加成功！")
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')

        except Exception as e:
            logger.error(f"处理轮播消息添加错误: {e}")
            await update.message.reply_text("❌ 添加轮播消息时出错")
            await self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')

    def _create_navigation_keyboard(self, current_page: int, total_pages: int, base_callback: str) -> List[List[InlineKeyboardButton]]:
        keyboard = []
        nav_row = []
        if current_page > 1:
            nav_row.append(InlineKeyboardButton("◀️ 上一页", callback_data=f"{base_callback}_{current_page-1}"))
        if current_page < total_pages:
            nav_row.append(InlineKeyboardButton("下一页 ▶️", callback_data=f"{base_callback}_{current_page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        return keyboard

    async def check_message_security(self, update: Update) -> bool:
        message = update.effective_message
        if message.text and len(message.text) > 4096:
            await message.reply_text("❌ 消息内容过长")
            return False
        if message.document and message.document.file_size > 20 * 1024 * 1024:
            await message.reply_text("❌ 文件大小超过限制")
            return False
        return True

    async def check_user_permissions(self, update: Update) -> bool:
        if not update.effective_chat or not update.effective_user:
            return False
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        if await self.db.is_user_banned(user_id):
            return False
        if not await self.db.get_group(chat_id):
            return False
        return True

    async def handle_keyword_response(self, chat_id: int, response: str, context, original_message: Optional[Message] = None) -> Optional[Message]:
        sent_message = None
        if response.startswith('__media__'):
            _, media_type, file_id = response.split('__')
            media_methods = {
                'photo': context.bot.send_photo,
                'video': context.bot.send_video,
                'document': context.bot.send_document
            }
            if media_type in media_methods:
                sent_message = await media_methods[media_type](chat_id, file_id)
        else:
            sent_message = await context.bot.send_message(chat_id, response)
        if sent_message:
            group = await self.db.get_group(chat_id)
            delete_trigger = group.get('settings', {}).get('delete_trigger', False)
            await self.message_deletion_manager.schedule_message_deletion(
                sent_message, 
                AUTO_DELETE_SETTINGS['timeouts']['keyword'],
                delete_trigger=delete_trigger and original_message is not None
            )
        return sent_message

    async def _handle_daily_ranking_keyword(self, message: Message) -> str:
        stats, _ = await self.stats_manager.get_daily_stats(message.chat_id, 1)
        if not stats:
            return "📊 今日暂无统计数据"
        text = "📊 今日发言排行\n\n"
        for i, stat in enumerate(stats[:10], 1):
            try:
                user = await self.application.bot.get_chat_member(message.chat_id, stat['_id'])
                name = Utils.truncate_name(user.user.full_name or user.user.username or f"用户{stat['_id']}")
            except Exception:
                name = Utils.truncate_name(f"用户{stat['_id']}")
            text += f"{i}. {name} - {stat['total_messages']}条\n"
        return text

    async def _handle_monthly_ranking_keyword(self, message: Message) -> str:
        stats, _ = await self.stats_manager.get_monthly_stats(message.chat_id, 1)
        if not stats:
            return "📊 近30天暂无统计数据"
        text = "📊 近30天发言排行\n\n"
        for i, stat in enumerate(stats[:10], 1):
            try:
                user = await self.application.bot.get_chat_member(message.chat_id, stat['_id'])
                name = Utils.truncate_name(user.user.full_name or user.user.username or f"用户{stat['_id']}")
            except Exception:
                name = Utils.truncate_name(f"用户{stat['_id']}")
            text += f"{i}. {name} - {stat['total_messages']}条\n"
        return text

    async def _handle_message(self, update: Update, context):
        if not update.effective_message or not update.effective_user:
            return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        message = update.message
        try:
            setting_state = await self.settings_manager.get_setting_state(user_id, 'keyword')
            if setting_state and setting_state['group_id'] == chat_id:
                await self._process_keyword_adding(update, context, setting_state)
                return
            broadcast_state = await self.settings_manager.get_setting_state(user_id, 'broadcast')
            if broadcast_state and broadcast_state['group_id'] == chat_id:
                await self._process_broadcast_adding(update, context, broadcast_state)
                return
            for setting_type in ['stats_min_bytes', 'stats_daily_rank', 'stats_monthly_rank']:
                stats_state = await self.settings_manager.get_setting_state(user_id, setting_type)
                if stats_state and stats_state['group_id'] == chat_id:
                    await self._process_stats_setting(update, context, stats_state, setting_type)
                    return
            if not await self.check_message_security(update) or not await self.check_user_permissions(update):
                return
            user = await self.db.get_user(user_id)
            user_role = user['role'] if user else 'user'
            if message.text and message.text.lower() == '/cancel':
                active_settings = await self.settings_manager.get_active_settings(user_id)
                if active_settings:
                    for setting_type in active_settings:
                        await self.settings_manager.clear_setting_state(user_id, setting_type)
                    await message.reply_text("✅ 已取消设置操作")
                else:
                    await message.reply_text("❓ 当前没有进行中的设置操作")
                return
            command = message.text.split()[0] if message.text else None
            if not Utils.is_auto_delete_exempt(user_role, command):
                metadata = Utils.get_message_metadata(message)
                timeout = Utils.validate_delete_timeout(metadata['type'])
                await self.message_deletion_manager.schedule_message_deletion(message, timeout)
            if await self.has_permission(chat_id, GroupPermission.KEYWORDS) and message.text:
                response = await self.keyword_manager.match_keyword(chat_id, message.text, message)
                if response:
                    await self.handle_keyword_response(chat_id, response, context, message)
            if await self.has_permission(chat_id, GroupPermission.STATS):
                await self.stats_manager.add_message_stat(chat_id, user_id, message)
        except Exception as e:
            logger.error(f"处理消息错误: {e}")
            logger.error(traceback.format_exc())

if __name__ == '__main__':
    try:
        asyncio.run(TelegramBot.main())
    except KeyboardInterrupt:
        logger.info("机器人被用户停止")
    except Exception as e:
        logger.error(f"机器人停止，错误原因: {e}")
        raise
