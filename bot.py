# 注意: 需要创建 config.py 文件，并定义以下变量:
# TELEGRAM_TOKEN - Telegram 机器人的 API 令牌
# MONGODB_URI - MongoDB 数据库的连接 URI
# MONGODB_DB - MongoDB 数据库名称
# DEFAULT_SUPERADMINS - 默认超级管理员的用户 ID 列表
# DEFAULT_SETTINGS - 默认机器人设置
# BROADCAST_SETTINGS - 轮播消息设置
# KEYWORD_SETTINGS - 关键词设置
# AUTO_DELETE_SETTINGS - 自动删除消息设置
# WEB_HOST - Web 服务器主机
# WEB_PORT - Web 服务器端口
import os
import json
import signal
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from bson import ObjectId
from typing import Optional, Callable, Any
from telegram import Update
from telegram.ext import ContextTypes

from aiohttp import web
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters
)

def register_middleware(application: Application, middlewares: list) -> None:
    """注册中间件"""
    for middleware in middlewares:
        application.post_init = middleware
        
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
import functools
from typing import Callable
from telegram import Update
from telegram.ext import CallbackContext

def require_group_permission(permission):
    """权限验证装饰器"""
    def decorator(func):
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

def verify_environment():
    """验证环境变量"""
    required_vars = {
        'TELEGRAM_TOKEN': '机器人令牌',
        'MONGODB_URI': 'MongoDB连接URI',
        'MONGODB_DB': 'MongoDB数据库名',
        'WEBHOOK_DOMAIN': 'Webhook域名'
    }
    
    missing = []
    for var, desc in required_vars.items():
        if not os.getenv(var):
            missing.append(f"{var} ({desc})")
    
    if missing:
        raise ValueError(f"缺少必要的环境变量: {', '.join(missing)}")
        
# 加载环境变量
load_dotenv()

class SettingsManager:
    def __init__(self, db):
        self.db = db
        self._states = {}
        self._locks = {}
        self._state_locks = {}
        self._cleanup_task = None
        self._max_states_per_user = 5  # 每个用户最大并发状态数
        
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
                now = datetime.now()
                expired_keys = []
                async with asyncio.Lock():  # 使用锁保护状态清理
                    for key, state in self._states.items():
                        if (now - state['timestamp']).total_seconds() > 300:  # 5分钟超时
                            expired_keys.append(key)
                    
                    for key in expired_keys:
                        logger.info(f"清理过期状态: {key}")
                        await self._cleanup_state(key)
                        
                await asyncio.sleep(60)  # 每分钟检查一次
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
        async with asyncio.Lock():  # 使用锁保护状态读取
            state = self._states.get(state_key, {})
            return state.get('page', 1)
        
    async def set_current_page(self, group_id: int, section: str, page: int):
        """设置当前页码"""
        state_key = f"page_{group_id}_{section}"
        async with asyncio.Lock():  # 使用锁保护状态写入
            self._states[state_key] = {
                'page': page,
                'timestamp': datetime.now()
            }
            logger.info(f"设置页码: {state_key} => {page}")
            
    async def start_setting(self, user_id: int, setting_type: str, group_id: int):
        """开始设置过程"""
        state_lock = await self._get_state_lock(user_id)
        async with state_lock:
            # 检查用户当前状态数量
            user_states = sum(1 for k in self._states if k.startswith(f"setting_{user_id}"))
            if user_states >= self._max_states_per_user:
                raise ValueError(f"用户同时进行的设置操作不能超过 {self._max_states_per_user} 个")
            
            # 清除可能存在的旧状态
            old_state_key = f"setting_{user_id}_{setting_type}"
            if old_state_key in self._states:
                del self._states[old_state_key]
                logger.info(f"清除旧状态: {old_state_key}")
            
            # 创建新状态
            state_key = f"setting_{user_id}_{setting_type}"
            self._states[state_key] = {
                'group_id': group_id,
                'step': 1,  # 总是从步骤1开始
                'data': {},
                'timestamp': datetime.now()
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
        """更新设置状态
        
        参数:
            user_id: 用户ID
            setting_type: 设置类型
            data: 更新的数据
            next_step: 是否进入下一步
        """
        state_key = f"setting_{user_id}_{setting_type}"
        state_lock = await self._get_state_lock(user_id)
        
        async with state_lock:
            if state_key not in self._states:
                logger.warning(f"更新不存在的状态: {state_key}")
                return
                
            # 更新数据
            self._states[state_key]['data'].update(data)
            
            # 如果需要，进入下一步
            if next_step:
                self._states[state_key]['step'] += 1
                logger.info(f"状态 {state_key} 进入下一步: {self._states[state_key]['step']}")
            
            # 更新时间戳
            self._states[state_key]['timestamp'] = datetime.now()
            
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
        """获取近30日统计"""
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

from typing import Optional, Callable, Any
from telegram.ext import CallbackContext
import functools

class ErrorHandler:
    """统一错误处理器"""
    def __init__(self, logger):
        self.logger = logger
        self._error_handlers = {}
        self._setup_default_handlers()
        
    def _setup_default_handlers(self):
        """设置默认错误处理器"""
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
        """处理无效令牌错误"""
        self.logger.critical("Bot token is invalid!")
        return "❌ 机器人配置错误，请联系管理员"
        
    async def _handle_unauthorized(self, update: Update, error: Exception) -> str:
        """处理未授权错误"""
        self.logger.error(f"Unauthorized error: {error}")
        return "❌ 权限不足，无法执行该操作"
        
    async def _handle_timeout(self, update: Update, error: Exception) -> str:
        """处理超时错误"""
        self.logger.warning(f"Request timed out: {error}")
        return "❌ 操作超时，请重试"
        
    async def _handle_network_error(self, update: Update, error: Exception) -> str:
        """处理网络错误"""
        self.logger.error(f"Network error occurred: {error}")
        return "❌ 网络错误，请稍后重试"
        
    async def _handle_chat_migrated(self, update: Update, error: Exception) -> str:
        """处理群组迁移错误"""
        self.logger.info(f"Chat migrated to {error.new_chat_id}")
        return "群组ID已更新，请重新设置"

    async def _handle_message_too_long(self, update: Update, error: Exception) -> str:
        """处理消息过长错误"""
        self.logger.warning(f"Message too long: {error}")
        return "❌ 消息内容过长，请缩短后重试"

    async def _handle_flood_wait(self, update: Update, error: Exception) -> str:
        """处理洪水等待错误"""
        wait_time = getattr(error, 'retry_after', 60)
        self.logger.warning(f"Flood wait error: {error}, retry after {wait_time} seconds")
        return f"❌ 操作过于频繁，请等待 {wait_time} 秒后重试"

    async def _handle_retry_after(self, update: Update, error: Exception) -> str:
        """处理重试等待错误"""
        retry_after = getattr(error, 'retry_after', 30)
        self.logger.warning(f"Need to retry after {retry_after} seconds")
        return f"❌ 请等待 {retry_after} 秒后重试"

    async def _handle_bad_request(self, update: Update, error: Exception) -> str:
        """处理错误请求"""
        self.logger.error(f"Bad request error: {error}")
        return "❌ 无效的请求，请检查输入"
        
    async def _handle_telegram_error(self, update: Update, error: Exception) -> str:
        """处理一般Telegram错误"""
        self.logger.error(f"Telegram error occurred: {error}")
        return "❌ 操作失败，请重试"
        
    async def handle_error(self, update: Update, context: CallbackContext) -> None:
        """统一错误处理入口"""
        error = context.error
        error_type = type(error).__name__
        
        try:
            # 获取对应的错误处理器
            handler = self._error_handlers.get(
                error_type, 
                self._handle_telegram_error
            )
            
            # 处理错误并获取消息
            error_message = await handler(update, error)
            
            # 记录错误
            self.logger.error(
                f"Update {update} caused error {error}",
                exc_info=context.error
            )
            
            # 发送错误消息
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
        """注册自定义错误处理器"""
        self._error_handlers[error_type] = handler

class MessageMiddleware:
    """消息处理中间件"""
    def __init__(self, bot):
        self.bot = bot
        
    async def __call__(self, update, context):
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
            logger.error(f"中间件处理错误: {e}")
            
    async def _check_basic_security(self, update: Update) -> bool:
        """基本安全检查"""
        message = update.effective_message
        
        # 检查消息大小
        if message.text and len(message.text) > 4096:  # Telegram消息长度限制
            await message.reply_text("❌ 消息内容过长")
            return False
            
        # 检查文件大小
        if message.document and message.document.file_size > 20 * 1024 * 1024:  # 20MB
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
        if await self.bot.db.is_user_banned(user_id):
            return False
            
        # 检查群组是否已授权
        if not await self.bot.db.get_group(chat_id):
            return False
            
        return True
        
    async def _clean_message(self, update: Update) -> Optional[str]:
        """消息清理和验证"""
        message = update.effective_message
        
        if not message.text:
            return None
            
        # 清理HTML标签
        cleaned_text = html.escape(message.text)
        
        # 清理危险字符
        cleaned_text = re.sub(r'[^\w\s\-.,?!@#$%^&*()]', '', cleaned_text)
        
        return cleaned_text
        
    async def _check_rate_limit(self, update: Update) -> bool:
        """速率限制检查"""
        if not update.effective_user:
            return False
            
        user_id = update.effective_user.id
        
        # 获取用户最近的消息数量
        recent_messages = await self.bot.db.get_recent_message_count(
            user_id,
            seconds=60  # 检查最近60秒
        )
        
        # 如果超过限制，拒绝处理
        if recent_messages > 30:  # 每分钟最多30条消息
            await update.effective_message.reply_text(
                "❌ 消息发送过于频繁，请稍后再试"
            )
            return False
            
        return True

from telegram.ext import Application

class ErrorHandlingMiddleware:
    """错误处理中间件"""
    def __init__(self, error_handler):
        self.error_handler = error_handler
        
    async def __call__(self, update, context):
        try:
            return await context.application.process_update(update)
        except Exception as e:
            await self.error_handler.handle_error(update, context)
            raise

def error_handler(func: Callable) -> Callable:
    """错误处理装饰器"""
    @functools.wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(self, update, context, *args, **kwargs)
        except Exception as e:
            await self.error_handler.handle_error(update, context)
            raise
    return wrapper

class CommandHelper:
    """命令帮助工具类"""
    
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
            'usage': '/authgroup <群组ID> [权限1] [权限2] ...',
            'description': '授权群组',
            'example': '/authgroup -100123456789 keywords stats broadcast',
            'admin_only': True
        },
        'deauthgroup': {
            'usage': '/deauthgroup <群组ID>',
            'description': '取消群组授权',
            'example': '/deauthgroup -100123456789',
            'admin_only': True
        }
    }
    
    @classmethod
    def get_usage(cls, command: str) -> Optional[dict]:
        """获取命令使用说明"""
        return cls.COMMAND_USAGE.get(command)
        
    @classmethod
    def format_usage(cls, command: str) -> str:
        """格式化命令使用说明"""
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
        
    @classmethod
    async def check_usage(cls, update: Update, command: str, args: list) -> bool:
        """检查命令使用是否正确"""
        usage = cls.get_usage(command)
        if not usage:
            return True
            
        # 检查管理员权限
        if usage['admin_only'] and not await self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ 该命令仅管理员可用")
            return False
            
        # 检查参数
        if '<' in usage['usage'] and not args:
            await update.message.reply_text(
                f"❌ 命令使用方法不正确\n{cls.format_usage(command)}"
            )
            return False
            
        return True
        
def check_command_usage(func: Callable) -> Callable:
    """命令使用检查装饰器"""
    @functools.wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_message:
            return
            
        message = update.effective_message
        command = message.text.split()[0].lstrip('/').split('@')[0]
        
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            return
            
        # 检查命令使用是否正确
        usage = CommandHelper.get_usage(command)
        if not usage:
            return True
            
        # 检查管理员权限
        if usage['admin_only'] and not await self.is_admin(user_id):
            await update.message.reply_text("❌ 该命令仅管理员可用")
            return False
            
        # 检查参数
        if '<' in usage['usage'] and not context.args:
            await update.message.reply_text(
                f"❌ 命令使用方法不正确\n{CommandHelper.format_usage(command)}"
            )
            return False
            
        return await func(self, update, context, *args, **kwargs)
    return wrapper
    
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
        self.message_deletion_manager = None
        self.error_handler = ErrorHandler(logger)
           
    class MessageDeletionManager:
        """管理消息删除的类"""
        def __init__(self, bot):
            self.bot = bot
            self.deletion_tasks = {}

        async def schedule_message_deletion(
            self, 
            message: Message, 
            timeout: int, 
            delete_original: bool = False
        ):
            """调度消息删除"""
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
            """取消特定消息的删除任务"""
            task_key = f"delete_message_{message.chat.id}_{message.message_id}"
            if task_key in self.deletion_tasks:
                task = self.deletion_tasks[task_key]
                task.cancel()
                del self.deletion_tasks[task_key]

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

            # 初始化数据库
            self.db = Database()
            if not await self.db.connect(MONGODB_URI, MONGODB_DB):
                logger.error("数据库连接失败")
                return False

            # 初始化管理器
            self.settings_manager = SettingsManager(self.db)
            await self.settings_manager.start()  # 启动设置管理器
    
            self.keyword_manager = KeywordManager(self.db)
            self.broadcast_manager = BroadcastManager(self.db, self)
            self.stats_manager = StatsManager(self.db)
            self.message_deletion_manager = self.MessageDeletionManager(self)
    
            # 强制更新所有默认超级管理员
            for admin_id in DEFAULT_SUPERADMINS:
                await self.db.add_user({
                    'user_id': admin_id,
                    'role': UserRole.SUPERADMIN.value
                })
                logger.info(f"已设置超级管理员: {admin_id}")
    
            # 创建Telegram Bot应用
            self.application = (
                Application.builder()
                .token(TELEGRAM_TOKEN)
                .build()
            )
    
            # 注册处理器
            await self._register_handlers()
    
            # 重要：确保application正确初始化
            await self.application.initialize()
        
            # 创建 web 应用并添加路由
            self.web_app = web.Application()
            self.web_app.router.add_get('/', self.handle_healthcheck)
            self.web_app.router.add_get('/health', self.handle_healthcheck)

            # 设置webhook路径并添加路由
            webhook_domain = os.getenv('WEBHOOK_DOMAIN', 'your-render-app-name.onrender.com')
            webhook_url = f"https://{webhook_domain}/webhook/{TELEGRAM_TOKEN}"
            webhook_path = f"/webhook/{TELEGRAM_TOKEN}"
            self.web_app.router.add_post(webhook_path, self._handle_webhook)

            # 设置web服务器
            self.web_runner = web.AppRunner(self.web_app)
            await self.web_runner.setup()

            site = web.TCPSite(self.web_runner, WEB_HOST, WEB_PORT)
            await site.start()
            logger.info(f"Web服务器已在 {WEB_HOST}:{WEB_PORT} 启动")

            # 配置webhook
            await self.application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
        
            logger.info(f"Webhook已设置为 {webhook_url}")
    
            # 验证初始化
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
        """验证初始化是否成功"""
        # 验证超级管理员
        for admin_id in DEFAULT_SUPERADMINS:
            user = await self.db.get_user(admin_id)
            if not user or user['role'] != UserRole.SUPERADMIN.value:
                logger.error(f"超级管理员 {admin_id} 初始化失败")
                return False
    
        # 验证群组权限
        groups = await self.db.find_all_groups()
        if not groups:
            logger.error("没有找到任何已授权的群组")
            return False
    
        logger.info("初始化验证成功")
        logger.info(f"超级管理员: {DEFAULT_SUPERADMINS}")
        logger.info(f"已授权群组: {[g['group_id'] for g in groups]}")
        return True

    async def main(cls):
        """主函数"""
        try:
            # 验证环境变量
            verify_environment()
        
            # 创建机器人实例
            bot = cls()
        
            # 初始化
            if not await bot.initialize():
                logger.error("机器人初始化失败")
                return
        
            # 设置信号处理
            await bot.handle_signals()
        
            # 启动机器人
            if not await bot.start():
                logger.error("机器人启动失败")
                return
        
            # 等待关闭
            while bot.running:
                await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"启动失败: {e}")
            raise
            
    async def start(self):
        """启动机器人"""
        try:
            if not self.application:
                logger.error("机器人未初始化。初始化失败。")
                return False
        
            await self.application.start()
            self.running = True
        
            # 启动轮播消息和清理任务
            await self._start_broadcast_task()
            await self._start_cleanup_task()
        
            logger.info("机器人成功启动")
            return True
    
        except Exception as e:
            logger.error(f"机器人启动失败: {e}")
            logger.error(traceback.format_exc())
            return False

    async def stop(self):
        """停止机器人"""
        try:
            self.running = False
            if self.shutdown_event:
                self.shutdown_event.set()

            # 停止设置管理器
            if self.settings_manager:
                await self.settings_manager.stop()

            # 停止清理任务
            if self.cleanup_task:
                self.cleanup_task.cancel()

            # 停止web服务器
            if self.web_runner:
                await self.web_runner.cleanup()

            # 停止应用
            if self.application:
                try:
                    if getattr(self.application, 'running', False):
                        await self.application.stop()
                        await self.application.shutdown()
                except Exception as e:
                    logger.error(f"停止应用时出错: {e}")

            # 关闭数据库连接
            if self.db:
                try:
                    await self.db.close()
                except Exception as e:
                    logger.error(f"关闭数据库连接时出错: {e}")
                
        except Exception as e:
            logger.error(f"停止机器人时出错: {e}")
     
    async def shutdown(self):
        """完全关闭机器人"""
        await self.stop()

    async def _start_broadcast_task(self):
        """启动轮播消息任务"""
        while self.running:
            try:
                # 获取所有需要发送的轮播消息
                now = datetime.now()
                broadcasts = await self.db.db.broadcasts.find({
                    'start_time': {'$lte': now},
                    'end_time': {'$gt': now},
                    '$or': [
                        {'last_broadcast': {'$exists': False}},
                        # 使用聚合管道或其他查询方法来处理间隔时间
                        {'last_broadcast': {'$lt': now}}  # 简化条件，稍后在代码中过滤
                    ]
                }).to_list(None)

                # 然后在获取到 broadcasts 后进行过滤
                filtered_broadcasts = []
                for broadcast in broadcasts:
                    if 'last_broadcast' not in broadcast or broadcast['last_broadcast'] <= now - timedelta(seconds=broadcast['interval']):
                        filtered_broadcasts.append(broadcast)

                broadcasts = filtered_broadcasts

                for broadcast in broadcasts:
                    try:
                        # 发送轮播消息
                        if broadcast['content_type'] == 'text':
                            await self.application.bot.send_message(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'photo':
                            await self.application.bot.send_photo(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'video':
                            await self.application.bot.send_video(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'document':
                            await self.application.bot.send_document(broadcast['group_id'], broadcast['content'])

                        # 更新最后发送时间
                        await self.db.db.broadcasts.update_one(
                            {'_id': broadcast['_id']},
                            {'$set': {'last_broadcast': now}}
                        )
                    except Exception as e:
                        logger.error(f"发送轮播消息时出错: {e}")

                # 等待一分钟后再次检查
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"轮播任务出错: {e}")
                await asyncio.sleep(60)  # 如果出错，等待1分钟后重试

    async def _start_cleanup_task(self):
        """启动数据清理任务"""
        async def cleanup_routine():
            while self.running:
                try:
                    await self.db.cleanup_old_stats(
                        days=DEFAULT_SETTINGS.get('cleanup_days', 30)
                    )
                    await asyncio.sleep(24 * 60 * 60)  # 每24小时运行一次
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
                    await asyncio.sleep(1 * 60 * 60)  # 如果出错，等待1小时后重试
    
        self.cleanup_task = asyncio.create_task(cleanup_routine())
    
    async def handle_signals(self):
        """设置信号处理器"""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_running_loop().add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.stop())
                )
            logger.info("Signal handlers set up")
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")

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

    async def handle_healthcheck(self, request):
        """处理健康检查请求"""
        return web.Response(text="Healthy", status=200)

    async def _handle_webhook(self, request):
        """处理Telegram webhook请求"""
        try:
            if request.content_type != 'application/json':
                logger.warning(f"收到无效的内容类型: {request.content_type}")
                return web.Response(status=415)
    
            update_data = await request.json()
            logger.info(f"收到webhook更新: {update_data}")
    
            update = Update.de_json(update_data, self.application.bot)
            if update:
                # 确保应用已初始化
                if not getattr(self.application, '_initialized', False):
                    logger.error("应用未初始化，尝试重新初始化")
                    await self.application.initialize()
                
                await self.application.process_update(update)
                logger.info("成功处理更新")
            else:
                logger.warning("收到无效的更新数据")
    
            return web.Response(status=200)
        except RuntimeError as e:
            if "not initialized" in str(e):
                logger.error(f"应用未初始化: {e}")
                # 尝试重新初始化和启动
                try:
                    await self.application.initialize()
                    await self.application.start()
                    logger.info("应用已重新初始化和启动")
                    # 重试处理更新
                    if update:
                        await self.application.process_update(update)
                except Exception as re_init_error:
                    logger.error(f"重新初始化失败: {re_init_error}")
            else:
                logger.error(f"处理webhook错误: {e}", exc_info=True)
            return web.Response(status=500)
        except Exception as e:
            logger.error(f"处理webhook错误: {e}", exc_info=True)
            return web.Response(status=500)

    async def _register_handlers(self):
        """注册各种事件处理器"""
        # 注册中间件
        message_middleware = MessageMiddleware(self)
        error_middleware = ErrorHandlingMiddleware(self.error_handler)
        
        register_middleware(self.application, [
            message_middleware,
            error_middleware
        ])

        # 注册命令处理器
        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("tongji", self._handle_rank_command))
        self.application.add_handler(CommandHandler("tongji30", self._handle_rank_command))
        self.application.add_handler(CommandHandler("settings", self._handle_settings))
        self.application.add_handler(CommandHandler("admingroups", self._handle_admin_groups))
    
        # 注册管理员命令
        self.application.add_handler(CommandHandler("addsuperadmin", self._handle_add_superadmin))
        self.application.add_handler(CommandHandler("delsuperadmin", self._handle_del_superadmin))
        self.application.add_handler(CommandHandler("addadmin", self._handle_add_admin))
        self.application.add_handler(CommandHandler("deladmin", self._handle_del_admin))
        self.application.add_handler(CommandHandler("authgroup", self._handle_auth_group))
        self.application.add_handler(CommandHandler("deauthgroup", self._handle_deauth_group))
        self.application.add_handler(CommandHandler("checkconfig", self._handle_check_config))

        # 注册回调查询处理器
        self.application.add_handler(CallbackQueryHandler(self._handle_settings_callback, pattern=r'^settings_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_keyword_callback, pattern=r'^keyword_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_broadcast_callback, pattern=r'^broadcast_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_keyword_continue_callback, pattern=r'^keyword_continue_'))
        self.application.add_handler(CallbackQueryHandler(self._handle_stats_edit_callback, pattern=r'^stats_edit_'))
    
        # 注册通用消息处理器
        self.application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self._handle_message))

    @handle_callback_errors
    async def _handle_keyword_callback(self, update: Update, context):
        """处理关键词回调"""
        query = update.callback_query
        await query.answer()

        try:
            data = query.data
            logger.info(f"处理关键词回调: 用户ID={update.effective_user.id}, 回调数据={data}")
        
            # 标准化参数解析
            parts = data.split('_')
        
            # 基本验证 - 确保至少有基本结构 (keyword_action_...)
            if len(parts) < 3:
                logger.warning(f"无效的回调数据格式: {data} (部分不足)")
                await query.edit_message_text("❌ 无效的操作")
                return

            # 提取操作类型
            action = parts[1]
            logger.info(f"关键词操作类型: {action}")
        
            # 统一解析群组ID (在所有回调数据中保持一致位置)
            try:
                # 对于所有操作，群组ID都是第三个参数
                group_id = int(parts[2])
                logger.info(f"群组ID: {group_id}")
            except (ValueError, IndexError) as e:
                logger.error(f"无法解析群组ID: {e}, 回调数据={data}")
                await query.edit_message_text("❌ 无效的群组ID")
                return
            
            # 权限验证 - 对所有操作都进行验证
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                logger.warning(f"权限不足: 用户ID={update.effective_user.id}, 群组ID={group_id}")
                await query.edit_message_text("❌ 无权限管理此群组")
                return

            if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
                logger.warning(f"群组未启用关键词功能: 群组ID={group_id}")
                await query.edit_message_text("❌ 此群组未启用关键词功能")
                return

            # 根据不同的操作类型处理
            if action == "detail":
                # 格式: keyword_detail_group_id_keyword_id
                if len(parts) < 4:
                    logger.warning(f"关键词详情缺少参数: {data}")
                    await query.edit_message_text("❌ 无效的关键词ID")
                    return
                
                keyword_id = parts[3]
                logger.info(f"查看关键词详情: 关键词ID={keyword_id}")
            
                # 获取关键词信息
                keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
    
                if not keyword:
                    logger.warning(f"未找到关键词: ID={keyword_id}, 群组ID={group_id}")
                    await query.edit_message_text("❌ 未找到该关键词")
                    return

                pattern = keyword['pattern']
                response_type = keyword['response_type']
                match_type = keyword['type']
                logger.info(f"关键词信息: 模式={pattern}, 响应类型={response_type}, 匹配类型={match_type}")

                # 构建响应内容预览
                response_preview = "无法预览媒体内容"
                if response_type == 'text':
                    response_text = keyword['response']
                    # 限制预览长度
                    if len(response_text) > 100:
                        response_preview = response_text[:97] + "..."
                    else:
                        response_preview = response_text

                # 构建回复类型的文本描述
                response_type_text = {
                    'text': '文本',
                    'photo': '图片',
                    'video': '视频',
                    'document': '文件'
                }.get(response_type, response_type)

                # 构建详情界面的键盘 - 注意保持群组ID在关键词ID之前的一致性
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "❌ 删除此关键词", 
                            callback_data=f"keyword_delete_confirm_{group_id}_{keyword_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "🔙 返回列表", 
                            callback_data=f"settings_keywords_{group_id}"
                        )
                    ]
                ]

                # 构建详情文本
                text = (
                    f"📝 关键词详情：\n\n"
                    f"🔹 匹配类型：{'正则匹配' if match_type == 'regex' else '精确匹配'}\n"
                    f"🔹 关键词：{pattern}\n"
                    f"🔹 回复类型：{response_type_text}\n"
                )
        
                if response_type == 'text':
                    text += f"🔹 回复内容：{response_preview}\n"

                await query.edit_message_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                logger.info(f"已显示关键词详情: ID={keyword_id}")
            
            elif action == "add":
                # 让用户选择匹配类型
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "精确匹配", 
                            callback_data=f"keyword_type_exact_{group_id}"
                        ),
                        InlineKeyboardButton(
                            "正则匹配", 
                            callback_data=f"keyword_type_regex_{group_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "取消", 
                            callback_data=f"settings_keywords_{group_id}"
                        )
                    ]
                ]
                await query.edit_message_text(
                    "请选择关键词匹配类型：",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                logger.info(f"已显示关键词匹配类型选择: 群组ID={group_id}")

            elif action == "type":
                if len(parts) < 4:
                    logger.warning(f"关键词类型选择缺少参数: {data}")
                    await query.edit_message_text("❌ 无效的匹配类型")
                    return
                
                match_type = parts[3]  # exact/regex
                if match_type not in ['exact', 'regex']:
                    logger.warning(f"无效的匹配类型: {match_type}")
                    await query.edit_message_text("❌ 无效的匹配类型")
                    return
        
                # 记录详细日志
                logger.info(f"用户 {update.effective_user.id} 为群组 {group_id} 选择关键词匹配类型: {match_type}")
        
                # 检查是否已有正在进行的关键词设置
                active_settings = await self.settings_manager.get_active_settings(update.effective_user.id)
                if 'keyword' in active_settings:
                    # 清除之前的状态
                    await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
                    logger.info(f"清除用户 {update.effective_user.id} 之前的关键词设置状态")
        
                # 开始设置状态
                await self.settings_manager.start_setting(
                    update.effective_user.id,
                    'keyword',
                    group_id
                )
        
                # 保存匹配类型到状态
                await self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'keyword',
                    {'match_type': match_type}
                )
                logger.info(f"已更新设置状态: 用户ID={update.effective_user.id}, 匹配类型={match_type}")

                # 提示输入关键词
                match_type_text = "精确匹配" if match_type == "exact" else "正则匹配"
                await query.edit_message_text(
                    f"您选择了{match_type_text}方式\n\n"
                    f"请发送关键词内容：\n"
                    f"{'(支持正则表达式)' if match_type == 'regex' else ''}\n\n"
                    "发送 /cancel 取消"
                )
                logger.info(f"已提示用户输入关键词: 用户ID={update.effective_user.id}")

            elif action == "delete_confirm":
                if len(parts) < 4:
                    logger.warning(f"关键词删除确认缺少参数: {data}")
                    await query.edit_message_text("❌ 无效的关键词ID")
                    return
                
                keyword_id = parts[3]
                logger.info(f"确认删除关键词: ID={keyword_id}, 群组ID={group_id}")
        
                # 获取关键词信息用于显示
                keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                if not keyword:
                    logger.warning(f"未找到要删除的关键词: ID={keyword_id}")
                    await query.edit_message_text("❌ 未找到该关键词")
                    return
            
                pattern = keyword['pattern']
                logger.info(f"关键词信息: 模式={pattern}")
        
                # 构建确认删除的键盘 - 保持群组ID在关键词ID之前
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "✅ 确认删除", 
                            callback_data=f"keyword_delete_{group_id}_{keyword_id}"
                        ),
                        InlineKeyboardButton(
                            "❌ 取消", 
                            callback_data=f"keyword_detail_{group_id}_{keyword_id}"
                        )
                    ]
                ]
        
                await query.edit_message_text(
                    f"⚠️ 确定要删除关键词「{pattern}」吗？\n"
                    "此操作不可撤销！",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                logger.info(f"已显示删除确认: 关键词={pattern}")

            elif action == "delete":
                if len(parts) < 4:
                    logger.warning(f"关键词删除缺少参数: {data}")
                    await query.edit_message_text("❌ 无效的关键词ID")
                    return
                
                keyword_id = parts[3]
                logger.info(f"删除关键词: ID={keyword_id}, 群组ID={group_id}")
        
                try:
                    # 获取关键词信息用于显示
                    keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                    if not keyword:
                        logger.warning(f"未找到要删除的关键词: ID={keyword_id}")
                        await query.edit_message_text("❌ 未找到该关键词")
                        return
                    
                    pattern = keyword['pattern']
                    logger.info(f"关键词信息: 模式={pattern}")
            
                    # 执行删除
                    await self.db.remove_keyword(group_id, keyword_id)
                    logger.info(f"已删除关键词: ID={keyword_id}, 模式={pattern}")
            
                    # 显示删除成功消息
                    await query.edit_message_text(f"✅ 已删除关键词「{pattern}」")
            
                    # 短暂延迟后返回关键词列表
                    await asyncio.sleep(1)
                    await self._show_keyword_settings(query, group_id)
            
                except Exception as e:
                    logger.error(f"删除关键词时出错: {e}")
                    await query.edit_message_text("❌ 删除关键词时出错，请重试")

            elif action == "edit":
                # 目前不支持编辑，如需添加可以在此实现
                logger.info(f"尝试编辑关键词 (不支持): 群组ID={group_id}")
                await query.edit_message_text(
                    "⚠️ 目前不支持编辑关键词\n"
                    "如需修改，请删除后重新添加",
                    reply_markup=InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton(
                                "🔙 返回", 
                                callback_data=f"settings_keywords_{group_id}"
                            )
                        ]
                    ])
                )

            elif action == "list_page":
                # 分页显示关键词列表
                try:
                    if len(parts) < 4:
                        logger.warning(f"关键词分页缺少参数: {data}")
                        await query.edit_message_text("❌ 无效的页码")
                        return
                    
                    page = int(parts[3])
                    logger.info(f"显示关键词列表页: 页码={page}, 群组ID={group_id}")
                
                    await self.settings_manager.set_current_page(group_id, "keywords", page)
                    await self._show_keyword_settings(query, group_id, page)
                except ValueError as e:
                    logger.error(f"无效的页码: {e}")
                    await query.edit_message_text("❌ 无效的页码")
            else:
                logger.warning(f"未知的关键词操作: {action}")
                await query.edit_message_text(
                    f"❌ 未知的操作: {action}",
                    reply_markup=InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton(
                                "🔙 返回", 
                                callback_data=f"settings_keywords_{group_id}"
                            )
                        ]
                    ])
                )

        except Exception as e:
            logger.error(f"处理关键词回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理关键词设置时出错，请重试")

    @handle_callback_errors
    async def _handle_keyword_continue_callback(self, update: Update, context):
        """处理关键词添加后的继续操作回调"""
        query = update.callback_query
        await query.answer()

        try:
            data = query.data
            logger.info(f"处理关键词继续添加回调: 用户ID={update.effective_user.id}, 回调数据={data}")
        
            parts = data.split('_')
        
            # 基本验证 - 确保至少有基本结构
            if len(parts) < 3:
                logger.warning(f"无效的回调数据格式: {data} (部分不足)")
                await query.edit_message_text("❌ 无效的操作")
                return
    
            # 提取群组ID
            try:
                group_id = int(parts[2])
                logger.info(f"群组ID: {group_id}")
            except (ValueError, IndexError) as e:
                logger.error(f"无法解析群组ID: {e}, 回调数据={data}")
                await query.edit_message_text("❌ 无效的群组ID")
                return

            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                logger.warning(f"权限不足: 用户ID={update.effective_user.id}, 群组ID={group_id}")
                await query.edit_message_text("❌ 无权限管理此群组")
                return

            # 验证关键词功能是否启用
            if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
                logger.warning(f"群组未启用关键词功能: 群组ID={group_id}")
                await query.edit_message_text("❌ 此群组未启用关键词功能")
                return

            # 直接跳转到关键词添加的匹配类型选择
            keyboard = [
                [
                    InlineKeyboardButton(
                        "精确匹配", 
                        callback_data=f"keyword_type_exact_{group_id}"
                    ),
                    InlineKeyboardButton(
                        "正则匹配", 
                        callback_data=f"keyword_type_regex_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "取消", 
                        callback_data=f"settings_keywords_{group_id}"
                    )
                ]
            ]
            await query.edit_message_text(
                "请选择关键词匹配类型：",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            logger.info(f"已显示关键词匹配类型选择: 群组ID={group_id}")

        except Exception as e:
            logger.error(f"处理关键词继续添加回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理操作时出错，请重试")
        
    @check_command_usage
    async def _handle_start(self, update: Update, context):
        """处理 start 命令"""
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
            admin_commands = (
                "\n管理员命令：\n"
                "👥 /admingroups - 查看可管理的群组\n"
                "⚙️ /settings - 群组设置管理\n"
            )
            welcome_text += admin_commands

        if is_superadmin:
            superadmin_commands = (
                "\n超级管理员命令：\n"
                "➕ /addsuperadmin <用户ID> - 添加超级管理员\n"
                "➖ /delsuperadmin <用户ID> - 删除超级管理员\n"
                "👤 /addadmin <用户ID> - 添加管理员\n"
                "🚫 /deladmin <用户ID> - 删除管理员\n"
                "✅ /authgroup <群组ID> <权限1> [权限2] ... - 授权群组\n"
                "❌ /deauthgroup <群组ID> - 取消群组授权\n"
                "🔍 /checkconfig - 检查当前配置\n"
            )
            welcome_text += superadmin_commands

        welcome_text += "\n如需帮助，请联系管理员。"
    
        await update.message.reply_text(welcome_text)

    @check_command_usage
    async def _handle_settings(self, update: Update, context):
        """处理设置命令"""
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
            logger.error(f"设置命令处理错误: {e}")
            await update.message.reply_text("❌ 处理设置命令时出错")

    async def _handle_show_manageable_groups(self, update: Update, context):
        """处理显示可管理的群组列表"""
        query = update.callback_query
        await query.answer()

        try:
            # 获取用户可管理的群组
            manageable_groups = await self.db.get_manageable_groups(update.effective_user.id)
        
            if not manageable_groups:
                await query.edit_message_text("❌ 你没有权限管理任何群组")
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
        
            await query.edit_message_text(
                "请选择要管理的群组：", 
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        except Exception as e:
            logger.error(f"显示可管理群组错误: {e}")
            await query.edit_message_text("❌ 获取群组列表时出错")

    async def _handle_settings_callback(self, update: Update, context):
        """
        处理设置菜单的回调查询
    
        该函数处理用户在设置菜单中的各种操作，包括：
        - 选择要管理的群组
        - 管理统计设置
        - 管理轮播消息
        - 管理关键词
    
        参数:
            update (Update): Telegram更新对象
            context (CallbackContext): 回调上下文
        
        回调数据格式:
            settings_<action>_<group_id>
        
        可用动作:
            - select: 选择群组
            - stats: 统计设置
            - broadcast: 轮播设置
            - keywords: 关键词设置
        
        权限要求:
            - 用户必须是群组管理员或超级管理员
            - 群组必须启用相应功能
        
        错误处理:
            - 无效的回调数据
            - 权限不足
            - 群组未启用功能
            - 数据库操作错误
        
        状态管理:
            - 保存当前设置状态
            - 在完成或取消时清理状态
        
        异常:
            - ValueError: 无效的回调数据
            - PermissionError: 权限不足
            - DatabaseError: 数据库操作失败
        """
        query = update.callback_query
        await query.answer()

        try:
            data = query.data
            # 处理特殊的返回群组列表的回调
            if data == "show_manageable_groups":
                await self._handle_show_manageable_groups(update, context)
                return

            parts = data.split('_')
        
            # 确保有足够的参数
            if len(parts) < 3:
                await query.edit_message_text("❌ 无效的操作")
                return

            action = parts[1]
        
            try:
                group_id = int(parts[2])
            except ValueError:
                await query.edit_message_text("❌ 无效的群组ID")
                return

            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return

            if action == "select":
                # 显示设置菜单
                keyboard = []
            
                # 检查各功能权限并添加对应按钮
                if await self.has_permission(group_id, GroupPermission.STATS):
                    keyboard.append([
                        InlineKeyboardButton(
                            "📊 统计设置", 
                            callback_data=f"settings_stats_{group_id}"
                        )
                    ])
                
                if await self.has_permission(group_id, GroupPermission.BROADCAST):
                    keyboard.append([
                        InlineKeyboardButton(
                             "📢 轮播消息", 
                            callback_data=f"settings_broadcast_{group_id}"
                        )
                    ])
                
                if await self.has_permission(group_id, GroupPermission.KEYWORDS):
                    keyboard.append([
                        InlineKeyboardButton(
                            "🔑 关键词设置", 
                            callback_data=f"settings_keywords_{group_id}"
                        )
                    ])

                # 添加返回按钮
                keyboard.append([
                    InlineKeyboardButton(
                        "🔙 返回群组列表", 
                        callback_data="show_manageable_groups"
                    )
                ])

                await query.edit_message_text(
                    f"群组 {group_id} 的设置菜单\n"
                    "请选择要管理的功能：",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif action in ["stats", "broadcast", "keywords"]:
                # 处理具体设置分区
                await self._handle_settings_section(query, context, group_id, action)

        except Exception as e:
            logger.error(f"处理设置回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理设置操作时出错")

    async def _handle_settings_section(self, query, context, group_id: int, section: str):
        """处理设置分区显示"""
        try:
            if section == "stats":
                # 获取当前群组的统计设置
                settings = await self.db.get_group_settings(group_id)
                await self._show_stats_settings(query, group_id, settings)
            
            elif section == "broadcast":
                await self._show_broadcast_settings(query, group_id)
            
            elif section == "keywords":
                await self._show_keyword_settings(query, group_id)
            
        except Exception as e:
            logger.error(f"处理设置分区显示错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 显示设置分区时出错")

    async def _show_stats_settings(self, query, group_id: int, settings: dict):
        """显示统计设置页面"""
        keyboard = [
            [
                InlineKeyboardButton(
                    f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", 
                    callback_data=f"stats_edit_min_bytes_{group_id}"  # 直接使用字符串格式
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

        await query.edit_message_text(
            f"群组 {group_id} 的统计设置",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    async def _show_broadcast_settings(self, query, group_id: int):
        """显示轮播消息设置页面"""
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

    async def _show_keyword_settings(self, query, group_id: int, page: int = 1):
        """显示关键词设置页面
    
        """
        try:
            logger.info(f"显示群组 {group_id} 的关键词设置，页码: {page}")
        
            # 获取群组的关键词列表
            keywords = await self.db.get_keywords(group_id)
        
            # 分页处理
            page_size = 10  # 每页显示的关键词数量
            total_keywords = len(keywords)
            total_pages = (total_keywords + page_size - 1) // page_size
        
            # 确保页码有效
            if page < 1:
                page = 1
            elif page > total_pages and total_pages > 0:
                page = total_pages
            
            # 切片当前页的关键词
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            current_page_keywords = keywords[start_idx:end_idx] if keywords else []
        
            logger.info(f"关键词总数: {total_keywords}, 总页数: {total_pages}, 当前页: {page}")
    
            keyboard = []
        
            # 添加关键词按钮
            for kw in current_page_keywords:
                keyword_text = kw['pattern']
                # 限制显示长度
                if len(keyword_text) > 20:
                    keyword_text = keyword_text[:17] + '...'
                
                # 生成回调数据，格式：keyword_detail_group_id_keyword_id
                callback_data = f"keyword_detail_{group_id}_{kw['_id']}"
            
                keyboard.append([
                    InlineKeyboardButton(
                        f"🔑 {keyword_text}", 
                        callback_data=callback_data
                    )
                ])
            
            # 添加分页导航
            if total_pages > 1:
                nav_row = []
            
                if page > 1:
                    nav_row.append(
                        InlineKeyboardButton(
                            "◀️ 上一页", 
                            callback_data=f"keyword_list_page_{group_id}_{page-1}"
                        )
                    )
                
                if page < total_pages:
                    nav_row.append(
                        InlineKeyboardButton(
                            "下一页 ▶️", 
                            callback_data=f"keyword_list_page_{group_id}_{page+1}"
                        )
                    )
                
                if nav_row:
                    keyboard.append(nav_row)
    
            # 添加功能按钮
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
        
            # 组装页面标题
            title = f"群组 {group_id} 的关键词设置"
            if total_pages > 1:
                title += f" (第 {page}/{total_pages} 页)"
    
            await query.edit_message_text(
                title,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            logger.info(f"已显示关键词设置页面: 群组ID={group_id}, 页码={page}")
        
        except Exception as e:
            logger.error(f"显示关键词设置出错: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 加载关键词设置时出错，请重试")

    @check_command_usage
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
            logger.error(f"处理排行命令错误: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 获取排行榜时出错")

    @handle_callback_errors
    async def _handle_broadcast_callback(self, update: Update, context):
        """
        处理轮播消息的回调查询
    
        该函数处理轮播消息的添加、编辑和删除操作。
    
        参数:
            update (Update): Telegram更新对象
            context (CallbackContext): 回调上下文
        
        回调数据格式:
            broadcast_<action>_<group_id>[_<broadcast_id>]
        
        可用动作:
            - add: 添加新轮播消息
            - edit: 编辑现有轮播消息
            - delete: 删除轮播消息
            - type: 选择消息类型
        
        支持的消息类型:
            - text: 文本消息
            - photo: 图片
            - video: 视频
            - document: 文件
        
        权限要求:
            - 用户必须是群组管理员
            - 群组必须启用轮播功能
        
        配置限制:
            - 最大轮播消息数量
            - 最小轮播间隔
            - 消息类型限制
        """
        
        query = update.callback_query
        await query.answer()

        try:
            data = query.data
            parts = data.split('_')

            # 健壮性检查
            if len(parts) < 3:
                await query.edit_message_text("❌ 无效的操作")
                return

            action = parts[1]
            group_id = int(parts[-1])

            # 验证权限
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
                        InlineKeyboardButton("图片", callback_data=f"broadcast_type_photo_{group_id}")
                    ],
                    [
                        InlineKeyboardButton("视频", callback_data=f"broadcast_type_video_{group_id}"),
                        InlineKeyboardButton("文件", callback_data=f"broadcast_type_document_{group_id}")
                    ],
                    [
                        InlineKeyboardButton("取消", callback_data=f"settings_broadcast_{group_id}")
                    ]
                ]

                await query.edit_message_text(
                    "请选择轮播消息类型：",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif action == "type":
                content_type = parts[2]
                await self.settings_manager.start_setting(
                    update.effective_user.id,
                    'broadcast',
                    group_id
                )
                await self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'broadcast',
                    {'content_type': content_type}
                )

                type_prompts = {
                    'text': '文本内容',
                    'photo': '图片',
                    'video': '视频',
                    'document': '文件'
                }

                await query.edit_message_text(
                    f"请发送要轮播的{type_prompts.get(content_type, '内容')}：\n\n"
                    f"发送 /cancel 取消"
                )

            elif action == "delete":
                broadcast_id = ObjectId(parts[2])
                await self.db.db.broadcasts.delete_one({
                    '_id': broadcast_id,
                    'group_id': group_id
                })
                await self._show_broadcast_settings(query, group_id)

        except Exception as e:
            logger.error(f"处理轮播消息回调错误: {e}")
            await query.edit_message_text("❌ 处理轮播消息设置时出错，请重试")

    async def _handle_stats_edit_callback(self, update: Update, context):
        """处理统计设置编辑回调"""
        query = update.callback_query
        await query.answer()

        try:
            data = query.data
            # 记录收到的数据，帮助调试
            logger.info(f"收到统计设置回调数据: {data}")
        
            # 检查数据格式
            if "_" in data:
                parts = data.split('_')
                # 对于类似stats_edit_min_bytes_123456的格式
                if len(parts) >= 4 and parts[0] == "stats" and parts[1] == "edit":
                    setting_type = parts[2]
                
                    # 尝试从最后一部分获取群组ID
                    try:
                        group_id = int(parts[-1])
                    except ValueError:
                        # 如果最后一部分不是数字，可能是类似min_bytes的格式
                        logger.error(f"无法解析群组ID: {parts}")
                        await query.edit_message_text("❌ 回调数据格式错误")
                        return
                else:
                    logger.error(f"回调数据部分不足或格式错误: {parts}")
                    await query.edit_message_text("❌ 无效的操作")
                    return
            else:
                parts = data.split('|')
                # 对于类似stats_edit|min_bytes|123456的格式
                if len(parts) >= 3 and parts[0] == "stats_edit":
                    setting_type = parts[1]
                
                    try:
                        group_id = int(parts[2])
                    except ValueError:
                        logger.error(f"无法解析群组ID: {parts}")
                        await query.edit_message_text("❌ 回调数据格式错误")
                        return
                else:
                    logger.error(f"回调数据部分不足或格式错误: {parts}")
                    await query.edit_message_text("❌ 无效的操作")
                    return

            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return

            if not await self.has_permission(group_id, GroupPermission.STATS):
                await query.edit_message_text("❌ 此群组未启用统计功能")
                return

            # 获取当前设置
            settings = await self.db.get_group_settings(group_id)

            # 处理不同类型的设置
            if setting_type == "min_bytes":
                # 开始输入最小字节数的流程
                await query.edit_message_text(
                    "请输入最小统计字节数：\n"
                    "• 低于此值的消息将不计入统计\n"
                    "• 输入 0 表示统计所有消息\n\n"
                    "发送 /cancel 取消"
                )
                # 开始设置流程
                await self.settings_manager.start_setting(
                    update.effective_user.id,
                    'stats_min_bytes',
                    group_id
                )

            elif setting_type == "toggle_media":
                # 切换是否统计多媒体
                current_value = settings.get('count_media', False)
                settings['count_media'] = not current_value
                await self.db.update_group_settings(group_id, settings)

                # 刷新统计设置页面
                await self._show_stats_settings(query, group_id, settings)

            elif setting_type == "daily_rank":
                await query.edit_message_text(
                    "请输入日排行显示的用户数量：\n"
                    "• 建议在 5-20 之间\n\n"
                    "发送 /cancel 取消"
                )
                # 开始设置流程
                await self.settings_manager.start_setting(
                    update.effective_user.id,
                    'stats_daily_rank',
                    group_id
                )

            elif setting_type == "monthly_rank":
                await query.edit_message_text(
                    "请输入月排行显示的用户数量：\n"
                    "• 建议在 5-20 之间\n\n"
                    "发送 /cancel 取消"
                )
                # 开始设置流程
                await self.settings_manager.start_setting(
                    update.effective_user.id,
                    'stats_monthly_rank',
                    group_id
                )
            else:
                await query.edit_message_text(f"❌ 未知的设置类型: {setting_type}")

        except Exception as e:
            logger.error(f"处理统计设置编辑回调错误: {e}")
            logger.error(traceback.format_exc())
        
            # 尝试返回统计设置页面
            try:
                await query.edit_message_text("❌ 处理设置时出错，请重试")
            except Exception:
                pass

    async def _process_stats_setting(self, update: Update, context, stats_state, setting_type):
        """处理统计设置编辑"""
        try:
            logger.info(f"处理统计设置: {setting_type}, 状态: {stats_state}")
        
            if not stats_state:
                await update.message.reply_text("❌ 设置会话已过期，请重新开始")
                return
            
            group_id = stats_state.get('group_id')
        
            # 获取用户输入的值
            try:
                value = int(update.message.text)
                if value < 0 and setting_type != 'stats_min_bytes':  # min_bytes可以为0
                    raise ValueError("值不能为负")
            except ValueError:
                await update.message.reply_text("❌ 请输入一个有效的数字")
                return
        
            # 根据设置类型更新相应的值
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
            
            # 更新设置到数据库
            await self.db.update_group_settings(group_id, settings)
        
            # 显示更新后的统计设置页面
            keyboard = [
                [
                    InlineKeyboardButton(
                        f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", 
                        callback_data=f"stats_edit|min_bytes|{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"统计多媒体: {'是' if settings.get('count_media', False) else '否'}", 
                        callback_data=f"stats_edit|toggle_media|{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"日排行显示数量: {settings.get('daily_rank_size', 15)}", 
                        callback_data=f"stats_edit|daily_rank|{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"月排行显示数量: {settings.get('monthly_rank_size', 15)}", 
                        callback_data=f"stats_edit|monthly_rank|{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "返回设置菜单", 
                        callback_data=f"settings_select_{group_id}"
                    )
                ]
            ]
        
            await update.message.reply_text(
                f"✅ {tips}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
            # 清除设置状态
            await self.settings_manager.clear_setting_state(update.effective_user.id, setting_type)
        
        except Exception as e:
            logger.error(f"处理统计设置错误: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 更新设置时出错")
            try:
                await self.settings_manager.clear_setting_state(update.effective_user.id, setting_type)
            except Exception:
                pass

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

    async def _handle_message(self, update: Update, context):
        """处理消息"""
        # 安全检查：确保消息和用户有效
        if not update.effective_message or not update.effective_user:
            return
    
        # 获取必要的信息
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        message = update.message

        try:
            # 检查是否正在进行关键词添加流程
            setting_state = await self.settings_manager.get_setting_state(
                update.effective_user.id, 
                'keyword'
            )
            if setting_state and setting_state['group_id'] == chat_id:
                await self._process_keyword_adding(update, context, setting_state)
                return
            
            # 检查是否正在进行轮播消息添加流程
            broadcast_state = await self.settings_manager.get_setting_state(user_id, 'broadcast')
            if broadcast_state and broadcast_state['group_id'] == chat_id:
                await self._process_broadcast_adding(update, context, broadcast_state)
                return
            
            # 检查是否正在进行统计设置编辑
            for setting_type in ['stats_min_bytes', 'stats_daily_rank', 'stats_monthly_rank']:
                stats_state = await self.settings_manager.get_setting_state(user_id, setting_type)
                if stats_state and stats_state['group_id'] == chat_id:
                    await self._process_stats_setting(update, context, stats_state, setting_type)
                    return
                    
            # 检查消息安全性
            if not await self.check_message_security(update):
                return
        
            # 检查用户权限
            if not await self.check_user_permissions(update):
                return

            # 获取用户角色
            user = await self.db.get_user(user_id)
            user_role = user['role'] if user else 'user'

            # 检查是否有正在进行的设置操作
            setting_states = {
                'keyword': await self.settings_manager.get_setting_state(user_id, 'keyword'),
                'broadcast': await self.settings_manager.get_setting_state(user_id, 'broadcast'),
                'stats_min_bytes': await self.settings_manager.get_setting_state(user_id, 'stats_min_bytes'),
                'stats_daily_rank': await self.settings_manager.get_setting_state(user_id, 'stats_daily_rank'),
                'stats_monthly_rank': await self.settings_manager.get_setting_state(user_id, 'stats_monthly_rank')
            }

            # 获取用户的活动设置状态
            active_states = {k: v for k, v in setting_states.items() if v and v['group_id'] == chat_id}

            # 处理取消操作
            if message.text and message.text.lower() == '/cancel':
                for setting_type, state in active_states.items():
                    await self.settings_manager.clear_setting_state(user_id, setting_type)
                    await message.reply_text(f"✅ 已取消 {setting_type} 的设置操作")
                    return

            # 处理设置状态下的消息
            for setting_type, state in active_states.items():
                logger.info(f"检测到设置状态：{setting_type}, 状态：{state}")
            
                if setting_type == 'keyword':
                    await self._process_keyword_adding(update, context, state)
                    return
            
                elif setting_type == 'broadcast':
                    await self._process_broadcast_adding(update, context, state)
                    return
            
                elif setting_type in ['stats_min_bytes', 'stats_daily_rank', 'stats_monthly_rank']:
                    await self._process_stats_setting(update, context, state, setting_type)
                    return

            # 检查是否免除自动删除
            command = message.text.split()[0] if message.text else None
            if not is_auto_delete_exempt(user_role, command):
                # 获取消息元数据
                metadata = get_message_metadata(message)
                # 计算删除超时时间
                timeout = validate_delete_timeout(
                    message_type=metadata['type']
                )
        
                # 调度消息删除
                await self.message_deletion_manager.schedule_message_deletion(
                    message, 
                    timeout
                )
                
            # 处理关键词匹配
            if await self.has_permission(chat_id, GroupPermission.KEYWORDS):
                if message.text:
                    # 尝试匹配关键词
                    response = await self.keyword_manager.match_keyword(
                        chat_id,
                        message.text,
                        message
                    )
                    if response:
                        await self.handle_keyword_response(
                            chat_id, 
                            response, 
                            context, 
                            message
                        )
        
            # 处理消息统计
            if await self.has_permission(chat_id, GroupPermission.STATS):
                await self.stats_manager.add_message_stat(chat_id, user_id, message)
            
        except Exception as e:
            logger.error(f"处理消息错误: {e}")
            logger.error(traceback.format_exc())

    @check_command_usage
    async def _handle_admin_groups(self, update: Update, context):
        """处理管理员群组管理命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是管理员
        if not await self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ 只有管理员可以使用此命令")
            return
            
        try:
            # 获取可管理的群组
            groups = await self.db.get_manageable_groups(update.effective_user.id)
            
            if not groups:
                await update.message.reply_text("📝 你目前没有可管理的群组")
                return
                
            # 生成群组列表
            text = "📝 你可以管理的群组：\n\n"
            for group in groups:
                try:
                    group_info = await context.bot.get_chat(group['group_id'])
                    group_name = group_info.title
                except Exception:
                    group_name = f"群组 {group['group_id']}"
                    
                text += f"• {group_name}\n"
                text += f"  ID: {group['group_id']}\n"
                text += f"  权限: {', '.join(group.get('permissions', []))}\n\n"
                
            await update.message.reply_text(text)
            
        except Exception as e:
            logger.error(f"列出管理员群组错误: {e}")
            await update.message.reply_text("❌ 获取群组列表时出错")

    @check_command_usage
    async def _handle_add_admin(self, update: Update, context):
        """处理添加管理员命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以添加管理员")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/addadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            
            # 检查用户是否已经是管理员
            user = await self.db.get_user(user_id)
            if user and user['role'] in [UserRole.ADMIN.value, UserRole.SUPERADMIN.value]:
                await update.message.reply_text("❌ 该用户已经是管理员")
                return
                
            # 添加管理员
            await self.db.add_user({
                'user_id': user_id,
                'role': UserRole.ADMIN.value
            })
            
            await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为管理员")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"添加管理员错误: {e}")
            await update.message.reply_text("❌ 添加管理员时出错")

    async def _handle_del_admin(self, update: Update, context):
        """处理删除管理员命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除管理员")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deladmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            
            # 检查不能删除超级管理员
            user = await self.db.get_user(user_id)
            if not user:
                await update.message.reply_text("❌ 该用户不是管理员")
                return
                
            if user['role'] == UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 不能删除超级管理员")
                return
                
            # 删除管理员
            await self.db.remove_user(user_id)
            
            await update.message.reply_text(f"✅ 已删除管理员 {user_id}")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"删除管理员错误: {e}")
            await update.message.reply_text("❌ 删除管理员时出错")

    @check_command_usage
    async def _handle_add_superadmin(self, update: Update, context):
        """处理添加超级管理员命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以添加超级管理员")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/addsuperadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            
            # 检查用户是否已经是超级管理员
            user = await self.db.get_user(user_id)
            if user and user['role'] == UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 该用户已经是超级管理员")
                return
                
            # 添加超级管理员
            await self.db.add_user({
                'user_id': user_id,
                'role': UserRole.SUPERADMIN.value
            })
            
            await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为超级管理员")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"添加超级管理员错误: {e}")
            await update.message.reply_text("❌ 添加超级管理员时出错")

    @check_command_usage
    async def _handle_del_superadmin(self, update: Update, context):
        """处理删除超级管理员命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除超级管理员")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/delsuperadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            
            # 不能删除自己
            if user_id == update.effective_user.id:
                await update.message.reply_text("❌ 不能删除自己的超级管理员权限")
                return
            
            # 检查用户是否是超级管理员
            user = await self.db.get_user(user_id)
            if not user or user['role'] != UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 该用户不是超级管理员")
                return
                
            # 删除超级管理员
            await self.db.remove_user(user_id)
            
            await update.message.reply_text(f"✅ 已删除超级管理员 {user_id}")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"删除超级管理员错误: {e}")
            await update.message.reply_text("❌ 删除超级管理员时出错")

    @check_command_usage
    async def _handle_check_config(self, update: Update, context):
        """处理检查配置命令"""
        if not update.effective_user:
            return
        
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以查看配置")
            return
        
        try:
            # 获取超级管理员列表
            superadmins = await self.db.get_users_by_role(UserRole.SUPERADMIN.value)
            superadmin_ids = [user['user_id'] for user in superadmins]
        
            # 获取群组列表
            groups = await self.db.find_all_groups()
        
            # 构建配置信息
            config_text = "🔧 当前配置信息：\n\n"
            config_text += "👥 超级管理员：\n"
            for admin_id in superadmin_ids:
                config_text += f"• {admin_id}\n"
            
            config_text += "\n📋 已授权群组：\n"
            for group in groups:
                config_text += f"• 群组 {group['group_id']}\n"
                config_text += f"  权限: {', '.join(group.get('permissions', []))}\n"
        
            await update.message.reply_text(config_text)
        
        except Exception as e:
            logger.error(f"检查配置出错: {e}")
            await update.message.reply_text("❌ 获取配置信息时出错")

    @check_command_usage
    async def _handle_auth_group(self, update: Update, context):
        """处理授权群组命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以授权群组")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text(
                "❌ 请使用正确的格式：\n"
                "/authgroup <群组ID>"
            )
            return
            
        try:
            group_id = int(context.args[0])
            
            # 获取群组信息
            try:
                group_info = await context.bot.get_chat(group_id)
                group_name = group_info.title
            except Exception:
                await update.message.reply_text("❌ 无法获取群组信息，请确保机器人已加入该群组")
                return
            
            # 设置全部权限
            all_permissions = ['keywords', 'stats', 'broadcast']
        
            # 更新群组权限
            await self.db.add_group({
                'group_id': group_id,
                'permissions': all_permissions
            })
            
            await update.message.reply_text(
                f"✅ 已授权群组\n"
                f"群组：{group_name}\n"
                f"ID：{group_id}\n"
                f"已启用全部功能"
            )
            
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字")
        except Exception as e:
            logger.error(f"授权群组错误: {e}")
            await update.message.reply_text("❌ 授权群组时出错")

    @check_command_usage
    async def _handle_deauth_group(self, update: Update, context):
        """处理解除群组授权命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以解除群组授权")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deauthgroup <群组ID>")
            return
            
        try:
            group_id = int(context.args[0])
            
            # 检查群组是否已授权
            group = await self.db.get_group(group_id)
            if not group:
                await update.message.reply_text("❌ 该群组未授权")
                return
            
            # 删除群组
            await self.db.remove_group(group_id)
            
            await update.message.reply_text(f"✅ 已解除群组 {group_id} 的所有授权")
            
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字")
        except Exception as e:
            logger.error(f"解除群组授权错误: {e}")
            await update.message.reply_text("❌ 解除群组授权时出错")

    @handle_callback_errors
    async def _handle_keyword_response_type_callback(self, update: Update, context):
        """处理关键词响应类型的回调"""
        query = update.callback_query
        await query.answer()
    
        try:
            data = query.data
            parts = data.split('_')
            response_type = parts[2]  # 获取响应类型
            group_id = int(parts[3])  # 获取群组ID
        
            # 获取当前设置状态
            setting_state = self.settings_manager.get_setting_state(
                update.effective_user.id,
                'keyword'
            )
        
            if not setting_state:
                await query.edit_message_text("❌ 设置会话已过期，请重新开始")
                return
            
            # 更新设置状态
            setting_state['data']['response_type'] = response_type
        
            # 根据响应类型提示用户
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
        
            # 更新设置状态到下一步
            self.settings_manager.update_setting_state(
                update.effective_user.id,
                'keyword',
                {'response_type': response_type}
            )
        
        except Exception as e:
            logger.error(f"处理关键词响应类型回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理响应类型选择时出错")

    async def _handle_broadcast_message_type(self, update: Update, context, content_type: str):
        """处理广播消息类型选择"""
        try:
            query = update.callback_query
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
            
            # 更新设置状态
            await self.settings_manager.update_setting_state(
                update.effective_user.id,
                'broadcast',
                {'content_type': content_type}
            )
            
        except Exception as e:
            logger.error(f"处理广播消息类型选择错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理消息类型选择时出错")
        
    async def handle_keyword_response(
            self, 
            chat_id: int, 
            response: str, 
            context, 
            original_message: Optional[Message] = None
        ) -> Optional[Message]:
            """处理关键词响应，并可能进行自动删除
        
            :param chat_id: 聊天ID
            :param response: 响应内容
            :param context: 机器人上下文
            :param original_message: 原始消息
            :return: 发送的消息
            """
            sent_message = None
        
            if response.startswith('__media__'):
                # 处理媒体响应
                _, media_type, file_id = response.split('__')
            
                # 根据媒体类型发送消息
                media_methods = {
                    'photo': context.bot.send_photo,
                    'video': context.bot.send_video,
                    'document': context.bot.send_document
                }
            
                if media_type in media_methods:
                    sent_message = await media_methods[media_type](chat_id, file_id)
            else:
                # 处理文本响应
                sent_message = await context.bot.send_message(chat_id, response)
        
            # 如果成功发送消息，进行自动删除
            if sent_message:
                # 获取原始消息的元数据（如果有）
                metadata = get_message_metadata(original_message) if original_message else {}
            
                # 计算删除超时时间
                timeout = validate_delete_timeout(
                    message_type=metadata.get('type')
                )
            
                # 调度消息删除
                await self.message_deletion_manager.schedule_message_deletion(
                    sent_message, 
                    timeout
                )
        
            return sent_message

    async def _process_keyword_adding(self, update: Update, context, setting_state):
        """处理关键词添加流程"""
        try:
            # 记录详细日志
            logger.info(f"处理关键词添加: {setting_state}")
        
            if not setting_state:
                logger.error("设置状态为空")
                await update.message.reply_text("❌ 设置会话已过期，请重新开始")
                return
            
            step = setting_state.get('step', 1)
            group_id = setting_state.get('group_id')
            data = setting_state.get('data', {})
            match_type = data.get('match_type')
        
            logger.info(f"步骤: {step}, 群组: {group_id}, 匹配类型: {match_type}")
        
            # 接收关键词
            if step == 1:
                pattern = update.message.text.strip()
                logger.info(f"收到关键词: '{pattern}'")
            
                # 更新状态
                new_data = {'pattern': pattern, 'type': match_type}
                await self.settings_manager.update_setting_state(update.effective_user.id, 'keyword', new_data)
            
                # 手动更新步骤到2
                state_key = f"setting_{update.effective_user.id}_keyword"
                async with asyncio.Lock():
                    if state_key in self.settings_manager._states:
                        self.settings_manager._states[state_key]['step'] = 2
                        self.settings_manager._states[state_key]['timestamp'] = datetime.now()
                        logger.info(f"手动更新步骤到2: {self.settings_manager._states[state_key]}")
            
                # 提示用户输入回复内容
                await update.message.reply_text(
                    "✅ 关键词已设置\n\n"
                    "请发送此关键词的回复内容（支持文字/图片/视频/文件）：\n\n"
                    "发送 /cancel 取消设置"
                )
            
            elif step == 2:  # 添加回复内容
                logger.info("处理回复内容")
            
                # 检测回复类型和内容
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
            
                logger.info(f"回复类型: {response_type}")
            
                if not response_type or response_content is None:
                    await update.message.reply_text("❌ 请发送有效的回复内容（文本/图片/视频/文件）")
                    return
                
                # 从状态中获取之前设置的数据
                pattern = data.get('pattern')
                pattern_type = data.get('type')
            
                if not pattern or not pattern_type:
                    logger.error(f"缺少关键词设置数据: {data}")
                    await update.message.reply_text("❌ 添加关键词出错，请重新开始")
                    await self.settings_manager.clear_setting_state(
                        update.effective_user.id, 
                        'keyword'
                    )
                    return
                
                # 验证回复内容长度
                from config import KEYWORD_SETTINGS
                if response_type == 'text' and len(response_content) > KEYWORD_SETTINGS.get('max_response_length', 1000):
                    await update.message.reply_text(
                        f"❌ 回复内容过长，请不要超过 {KEYWORD_SETTINGS.get('max_response_length', 1000)} 个字符"
                    )
                    return
                
                # 检查关键词数量限制
                keywords = await self.db.get_keywords(group_id)
                if len(keywords) >= KEYWORD_SETTINGS.get('max_keywords', 100):
                    await update.message.reply_text(
                        f"❌ 关键词数量已达到上限 {KEYWORD_SETTINGS.get('max_keywords', 100)} 个"
                    )
                    await self.settings_manager.clear_setting_state(
                        update.effective_user.id, 
                        'keyword'
                    )
                    return
                
                # 添加关键词
                try:
                    await self.db.add_keyword({
                        'group_id': group_id,
                        'pattern': pattern,
                        'type': pattern_type,
                        'response': response_content,
                        'response_type': response_type
                    })
                
                    logger.info(f"已添加关键词: {pattern}, 类型: {pattern_type}, 响应类型: {response_type}")
                
                    # 询问是否继续添加
                    keyboard = [
                        [
                            InlineKeyboardButton(
                                "➕ 继续添加关键词", 
                                callback_data=f"keyword_continue_{group_id}"
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                "🔙 返回关键词设置", 
                                callback_data=f"settings_keywords_{group_id}"
                            )
                        ]
                    ]
                
                    await update.message.reply_text(
                        f"✅ 关键词 「{pattern}」 添加成功！",
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                
                    # 清除设置状态
                    await self.settings_manager.clear_setting_state(
                        update.effective_user.id, 
                        'keyword'
                    )
                
                except Exception as e:
                    logger.error(f"添加关键词失败: {e}")
                    logger.error(traceback.format_exc())
                    await update.message.reply_text("❌ 保存关键词时出错，请重试")
                    await self.settings_manager.clear_setting_state(
                        update.effective_user.id, 
                        'keyword'
                    )
                
            else:
                logger.warning(f"未知的设置步骤: {step}")
                await update.message.reply_text("❌ 设置过程出错，请重新开始")
                await self.settings_manager.clear_setting_state(
                    update.effective_user.id, 
                    'keyword'
                )
            
        except Exception as e:
            logger.error(f"处理关键词添加流程出错: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 添加关键词时出错，请重试")
            try:
                await self.settings_manager.clear_setting_state(
                    update.effective_user.id, 
                    'keyword'
                )
            except Exception:
                pass

    async def _process_broadcast_adding(self, update: Update, context, setting_state):
        """处理轮播消息添加流程修复"""
        try:
            logger.info(f"处理轮播消息添加，状态: {setting_state}")
        
            if not setting_state:
                await update.message.reply_text("❌ 设置会话已过期，请重新开始")
                return
            
            step = setting_state.get('step', 1)
            group_id = setting_state.get('group_id')
            data = setting_state.get('data', {})
            content_type = data.get('content_type')
        
            logger.info(f"步骤: {step}, 群组: {group_id}, 内容类型: {content_type}")

            if step == 1:  # 获取消息内容
                # 获取消息内容
                content = None
                if content_type == 'text':
                    content = update.message.text
                elif content_type == 'photo':
                    content = update.message.photo[-1].file_id if update.message.photo else None
                elif content_type == 'video':
                    content = update.message.video.file_id if update.message.video else None
                elif content_type == 'document':
                    content = update.message.document.file_id if update.message.document else None

                if not content:
                    await update.message.reply_text(f"❌ 请发送正确的{content_type}内容")
                    return

                # 检查内容限制
                if content_type == 'text' and len(content) > 4096:  # Telegram消息长度限制
                    await update.message.reply_text("❌ 文本内容过长")
                    await self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')
                    return

                # 更新状态
                await self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'broadcast',
                    {'content': content}
                )
            
                # 手动更新步骤到2
                state_key = f"setting_{update.effective_user.id}_broadcast"
                async with asyncio.Lock():
                    if state_key in self.settings_manager._states:
                        self.settings_manager._states[state_key]['step'] = 2
                        self.settings_manager._states[state_key]['timestamp'] = datetime.now()
                        logger.info(f"手动更新轮播设置步骤到2: {self.settings_manager._states[state_key]}")

                await update.message.reply_text(
                    "✅ 内容已设置\n\n"
                    "请设置轮播时间参数：\n"
                    "格式：开始时间 结束时间 间隔(秒)\n"
                    "例如：2024-02-22 08:00 2024-03-22 20:00 3600\n\n"
                    "发送 /cancel 取消"
                )

            elif step == 2:  # 设置时间参数
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
                    if interval < BROADCAST_SETTINGS.get('min_interval', 300):
                        raise ValueError(f"间隔时间不能小于{format_duration(BROADCAST_SETTINGS.get('min_interval', 300))}")

                    # 检查轮播消息数量限制
                    broadcasts = await self.db.db.broadcasts.count_documents({'group_id': group_id})
                    if broadcasts >= BROADCAST_SETTINGS.get('max_broadcasts', 10):
                        await update.message.reply_text(
                            f"❌ 轮播消息数量已达到上限 {BROADCAST_SETTINGS.get('max_broadcasts', 10)} 条"
                        )
                        return

                    # 添加轮播消息
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

        except Exception as e:
            logger.error(f"处理轮播消息添加错误: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 添加轮播消息时出错")
            try:
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')
            except Exception:
                pass

    async def check_message_security(self, update: Update) -> bool:
        """检查消息安全性"""
        if not update.effective_message:
            return False
        
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

    async def check_user_permissions(self, update: Update) -> bool:
        """检查用户权限"""
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

def async_main():
    """异步主入口点"""
    try:
        asyncio.run(TelegramBot.main(TelegramBot))
    except KeyboardInterrupt:
        logger.info("机器人被用户停止")
    except Exception as e:
        logger.error(f"机器人停止，错误原因: {e}")
        logger.error(traceback.format_exc())
        raise

if __name__ == '__main__':
    async_main()
