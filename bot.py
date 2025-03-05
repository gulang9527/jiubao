import os
import json
import signal
import asyncio
import logging
import traceback
import config
from telegram.error import BadRequest
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
def error_handler(func: Callable) -> Callable:
    """统一的错误处理装饰器"""
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(self, update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            
            # 处理错误
            if hasattr(self, 'error_handler'):
                await self.error_handler.handle_error(update, context)
            
            # 显示友好的错误消息
            if update and update.effective_message:
                try:
                    await update.effective_message.reply_text(
                        "❌ 操作过程中出现错误，请稍后重试或联系管理员。"
                    )
                except Exception as msg_error:
                    logger.error(f"发送错误消息失败: {msg_error}")
    return wrapper

def require_admin(func: Callable) -> Callable:
    """要求管理员权限的装饰器"""
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_user:
            return
            
        user_id = update.effective_user.id
        if not await self.is_admin(user_id):
            await update.message.reply_text("❌ 该命令仅管理员可用")
            return
            
        return await func(self, update, context, *args, **kwargs)
    return wrapper

def require_superadmin(func: Callable) -> Callable:
    """要求超级管理员权限的装饰器"""
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_user:
            return
            
        user_id = update.effective_user.id
        if not await self.is_superadmin(user_id):
            await update.message.reply_text("❌ 该命令仅超级管理员可用")
            return
            
        return await func(self, update, context, *args, **kwargs)
    return wrapper

def require_group_permission(permission: GroupPermission):
    """要求群组有特定权限的装饰器"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
            if not update.effective_chat:
                return
                
            group_id = update.effective_chat.id
            if not await self.has_permission(group_id, permission):
                await update.message.reply_text(f"❌ 此群组未启用{permission.value}功能")
                return
                
            return await func(self, update, context, *args, **kwargs)
        return wrapper
    return decorator

def check_command_usage(func: Callable) -> Callable:
    """检查命令使用格式的装饰器"""
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        if not update.effective_message:
            return
        message = update.effective_message
        command = message.text.split()[0].lstrip('/').split('@')[0]
        
        # 获取命令使用说明
        usage = CommandHelper.get_usage(command)
        if not usage:
            return await func(self, update, context, *args, **kwargs)
            
        # 检查权限
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            return
            
        if usage['admin_only'] and not await self.is_admin(user_id):
            await update.message.reply_text("❌ 该命令仅管理员可用")
            return
            
        # 检查参数
        if '<' in usage['usage'] and not context.args:
            await update.message.reply_text(f"❌ 命令使用方法不正确\n{CommandHelper.format_usage(command)}")
            return
            
        return await func(self, update, context, *args, **kwargs)
    return wrapper

def handle_callback_errors(func: Callable) -> Callable:
    """回调错误处理装饰器"""
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return await func(self, update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"回调错误 {func.__name__}: {e}", exc_info=True)
            if update.callback_query:
                try:
                    await update.callback_query.answer()
                    await update.callback_query.edit_message_text("❌ 操作出错，请重试")
                except Exception as answer_error:
                    logger.error(f"无法回应回调查询: {answer_error}")
    return wrapper

# 设置管理模块
class SettingsManager:
    def __init__(self, db):
        self.db = db
        self._states = {}
        self._locks = {}
        self._state_locks = {}
        self._cleanup_task = None
        self._max_states_per_user = 5
        
    async def start(self):
        """启动状态管理器和清理任务"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("状态管理器已启动")
        
    async def stop(self):
        """停止状态管理器和清理任务"""
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
        """定期清理过期状态的循环"""
        while True:
            try:
                now = datetime.now(config.TIMEZONE)
                expired_keys = []
                async with asyncio.Lock():
                    for key, state in self._states.items():
                        if (now - state['timestamp']).total_seconds() > 300:  # 5分钟过期
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
        """开始设置会话"""
        state_lock = await self._get_state_lock(user_id)
        async with state_lock:
            # 清理用户现有的设置状态
            user_states = [k for k in self._states if k.startswith(f"setting_{user_id}")]
            for state_key in user_states:
                await self._cleanup_state(state_key)
                logger.info(f"清除用户现有状态: {state_key}")
                
            # 检查用户状态数量限制
            user_states_count = sum(1 for k in self._states if k.startswith(f"setting_{user_id}"))
            if user_states_count >= self._max_states_per_user:
                raise ValueError(f"用户同时进行的设置操作不能超过 {self._max_states_per_user} 个")
                
            # 创建新的设置状态
            state_key = f"setting_{user_id}_{setting_type}"
            self._states[state_key] = {
                'group_id': group_id,  # 修复：使用传入的group_id参数
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
            if state:
                # 更新时间戳
                state['timestamp'] = datetime.now(config.TIMEZONE)
            logger.info(f"获取状态: {state_key} => {state is not None}")
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
            logger.info(f"更新状态: {state_key}, 步骤: {self._states[state_key]['step']}")
            
    async def clear_setting_state(self, user_id: int, setting_type: str):
        """清除设置状态"""
        state_key = f"setting_{user_id}_{setting_type}"
        state_lock = await self._get_state_lock(user_id)
        async with state_lock:
            await self._cleanup_state(state_key)
            logger.info(f"清除设置状态: {state_key}")

    async def get_active_settings(self, user_id: int) -> list:
        """获取用户活动的设置类型列表"""
        async with asyncio.Lock():
            settings = [
                k.split('_')[2] 
                for k in self._states 
                if k.startswith(f"setting_{user_id}")
            ]
            logger.info(f"用户 {user_id} 的活动设置: {settings}")
            return settings

    async def check_setting_conflict(self, user_id: int, setting_type: str) -> bool:
        """检查设置冲突"""
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

    async def process_setting(self, user_id: int, setting_type: str, message: Message, process_func: Callable):
        """处理用户设置消息"""
        state = await self.get_setting_state(user_id, setting_type)
        if not state:
            return False
        
        try:
            await process_func(state, message)
            return True
        except Exception as e:
            logger.error(f"处理设置 {setting_type} 时出错: {e}", exc_info=True)
            await message.reply_text(f"❌ 设置过程出错，请重试或使用 /cancel 取消")
            return True

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
            'group_id': group_id,  # 修复：使用传入的group_id参数
            'user_id': user_id,
            'date': datetime.now(config.TIMEZONE).strftime('%Y-%m-%d'),
            'total_messages': 1,
            'total_size': message_size,
            'media_type': media_type
        }
        await self.db.add_message_stat(stat_data)

    async def get_daily_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取每日统计数据"""
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
        """获取每月统计数据"""
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
        """获取群组的广播消息列表"""
        return await self.db.get_broadcasts(group_id)
        
    async def add_broadcast(self, broadcast_data: Dict) -> ObjectId:
        """添加广播消息"""
        if 'content_type' not in broadcast_data:
            raise ValueError("Missing 'content_type' in broadcast data")
        if broadcast_data['content_type'] not in config.ALLOWED_MEDIA_TYPES:
            raise ValueError(f"Invalid content_type: {broadcast_data['content_type']}")
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
        """更新最后广播时间"""
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
        """注册内置关键词处理函数"""
        self._built_in_keywords[pattern] = handler
        
    async def match_keyword(self, group_id: int, text: str, message: Message) -> Optional[str]:
        """匹配消息中的关键词"""
        logger.info(f"开始匹配关键词 - 群组: {group_id}, 文本: {text[:20]}...")
        
        # 匹配内置关键词
        for pattern, handler in self._built_in_keywords.items():
            if text == pattern:
                return await handler(message)
                
        # 匹配自定义关键词
        keywords = await self.get_keywords(group_id)
        logger.info(f"群组 {group_id} 有 {len(keywords)} 个关键词")
        
        for kw in keywords:
            logger.info(f"尝试匹配关键词: {kw['pattern']}, 类型: {kw['type']}")
            try:
                if kw['type'] == 'regex':
                    pattern = re.compile(kw['pattern'])
                    if pattern.search(text):
                        return self._format_response(kw)
                else:
                    if text == kw['pattern']:
                        return self._format_response(kw)
            except Exception as e:
                logger.error(f"匹配关键词 {kw['pattern']} 时出错: {e}")
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
        """通过ID获取特定关键词"""
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
        """设置默认错误处理函数"""
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
        """处理无效Token错误"""
        self.logger.critical("Bot token is invalid!")
        return "❌ 机器人配置错误，请联系管理员"
        
    async def _handle_unauthorized(self, update: Update, error: Exception) -> str:
        """处理无权限错误"""
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
        """处理群组ID迁移错误"""
        self.logger.info(f"Chat migrated to {error.new_chat_id}")
        return "群组ID已更新，请重新设置"

    async def _handle_message_too_long(self, update: Update, error: Exception) -> str:
        """处理消息过长错误"""
        self.logger.warning(f"Message too long: {error}")
        return "❌ 消息内容过长，请缩短后重试"

    async def _handle_flood_wait(self, update: Update, error: Exception) -> str:
        """处理消息频率限制错误"""
        wait_time = getattr(error, 'retry_after', 60)
        self.logger.warning(f"Flood wait error: {error}, retry after {wait_time} seconds")
        return f"❌ 操作过于频繁，请等待 {wait_time} 秒后重试"

    async def _handle_retry_after(self, update: Update, error: Exception) -> str:
        """处理需要重试错误"""
        retry_after = getattr(error, 'retry_after', 30)
        self.logger.warning(f"Need to retry after {retry_after} seconds")
        return f"❌ 请等待 {retry_after} 秒后重试"

    async def _handle_bad_request(self, update: Update, error: Exception) -> str:
        """处理无效请求错误"""
        self.logger.error(f"Bad request error: {error}")
        return "❌ 无效的请求，请检查输入"
        
    async def _handle_telegram_error(self, update: Update, error: Exception) -> str:
        """处理Telegram API错误"""
        self.logger.error(f"Telegram error occurred: {error}")
        return "❌ 操作失败，请重试"
        
    async def handle_error(self, update: Update, context: CallbackContext) -> None:
        """处理错误的主函数"""
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
        """注册自定义错误处理函数"""
        self._error_handlers[error_type] = handler

# 中间件模块
class MessageMiddleware:
    def __init__(self, bot):
        self.bot = bot
        
    async def __call__(self, update, context):
        """中间件主函数"""
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
        """检查基本安全限制"""
        message = update.effective_message
        if message.text and len(message.text) > 4096:
            await message.reply_text("❌ 消息内容过长")
            return False
        if message.document and message.document.file_size > 20 * 1024 * 1024:
            await message.reply_text("❌ 文件大小超过限制")
            return False
        return True
        
    async def _check_permissions(self, update: Update) -> bool:
        """检查权限"""
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
        """清理消息内容"""
        message = update.effective_message
        if not message.text:
            return None
        cleaned_text = re.sub(r'[^\w\s\-.,?!@#$%^&*()]', '', message.text)
        return cleaned_text

class ErrorHandlingMiddleware:
    def __init__(self, error_handler):
        self.error_handler = error_handler
        
    async def __call__(self, update, context):
        """错误处理中间件"""
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
        'deauthgroup': {'usage': '/deauthgroup <群组ID>', 'description': '取消群组授权', 'example': '/deauthgroup -100123456789', 'admin_only': True},
        'cancel': {'usage': '/cancel', 'description': '取消当前操作', 'example': None, 'admin_only': False}
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
        text = [f"📝 命令: {command}", f"用法: {usage['usage']}", f"说明: {usage['description']}"]
        if usage['example']:
            text.append(f"示例: {usage['example']}")
        if usage['admin_only']:
            text.append("注意: 仅管理员可用")
        return "\n".join(text)

# 回调数据处理器
class CallbackDataHandler:
    def __init__(self):
        self.handlers = {}
        
    def register(self, prefix: str, handler: Callable):
        """注册回调处理函数"""
        self.handlers[prefix] = handler
        
    async def handle(self, update: Update, context: CallbackContext) -> bool:
        """处理回调查询"""
        query = update.callback_query
        if not query:
            return False
            
        data = query.data
        if not data:
            return False
            
        parts = data.split('_')
        if not parts:
            return False
            
        prefix = parts[0]
        handler = self.handlers.get(prefix)
        
        if handler:
            try:
                await handler(update, context, parts)
                return True
            except Exception as e:
                logger.error(f"处理回调 {prefix} 出错: {e}", exc_info=True)
                await query.answer("处理出错，请重试")
                return False
                
        return False

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
        self.callback_handler = None
        
    async def initialize(self):
        """初始化机器人"""
        try:
            # 验证配置
            from config_validator import validate_config, ConfigValidationError
            import config
            try:
                validate_config(config)
            except ConfigValidationError as e:
                logger.error(f"配置验证失败: {e}")
                return False
                
            logger.info("开始初始化机器人")
            
            # 加载配置
            from config import (
                TELEGRAM_TOKEN, MONGODB_URI, MONGODB_DB, DEFAULT_SUPERADMINS,
                DEFAULT_SETTINGS, BROADCAST_SETTINGS, KEYWORD_SETTINGS
            )
            
            # 连接数据库
            try:
                self.db = Database()
                if not await self.db.connect(MONGODB_URI, MONGODB_DB):
                    logger.error("数据库连接失败")
                    return False
            except Exception as e:
                logger.error(f"数据库连接错误: {e}", exc_info=True)
                return False
                
            # 初始化各个管理器
            self.error_handler = ErrorHandler(logger)
            self.settings_manager = SettingsManager(self.db)
            await self.settings_manager.start()
            self.keyword_manager = KeywordManager(self.db)
            self.broadcast_manager = BroadcastManager(self.db, self)
            self.stats_manager = StatsManager(self.db)
            self.callback_handler = CallbackDataHandler()
            
            # 设置超级管理员
            for admin_id in DEFAULT_SUPERADMINS:
                await self.db.add_user({'user_id': admin_id, 'role': UserRole.SUPERADMIN.value})
                logger.info(f"已设置超级管理员: {admin_id}")
                
            # 设置默认群组
            default_groups = [{
                'group_id': -1001234567890,
                'permissions': [perm.value for perm in GroupPermission],
                'feature_switches': {'keywords': True, 'stats': True, 'broadcast': True}
            }]
            
            for group in default_groups:
                await self.db.add_group({
                    'group_id': group['group_id'],
                    'permissions': group['permissions'],
                    'settings': {'auto_delete': False, 'auto_delete_timeout': config.AUTO_DELETE_SETTINGS['default_timeout']},
                    'feature_switches': group['feature_switches']
                })
                logger.info(f"已设置群组权限: {group['group_id']}")
                
            # 设置Web服务和Webhook
            webhook_domain = os.getenv('WEBHOOK_DOMAIN', 'your-render-app-name.onrender.com')
            self.application = Application.builder().token(TELEGRAM_TOKEN).build()
            
            # 注册处理函数
            await self._register_handlers()
            
            # 设置Web应用
            self.web_app = web.Application()
            self.web_app.router.add_get('/', self.handle_healthcheck)
            self.web_app.router.add_get('/health', self.handle_healthcheck)
            
            # 设置Webhook
            webhook_url = f"https://{webhook_domain}/webhook/{TELEGRAM_TOKEN}"
            webhook_path = f"/webhook/{TELEGRAM_TOKEN}"
            self.web_app.router.add_post(webhook_path, self._handle_webhook)
            
            # 启动Web服务器
            self.web_runner = web.AppRunner(self.web_app)
            await self.web_runner.setup()
            from config import WEB_HOST, WEB_PORT
            site = web.TCPSite(self.web_runner, WEB_HOST, WEB_PORT)
            await site.start()
            logger.info(f"Web服务器已在 {WEB_HOST}:{WEB_PORT} 启动")
            
            # 设置Webhook
            await self.application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
            self.application.updater = None
            logger.info(f"Webhook已设置为 {webhook_url}")
            
            # 验证初始化
            if not await self.verify_initialization():
                logger.error("初始化验证失败")
                return False
                
            logger.info("机器人初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"机器人初始化失败: {e}", exc_info=True)
            return False
            
    async def verify_initialization(self):
        """验证初始化是否成功"""
        from config import DEFAULT_SUPERADMINS
        
        # 验证超级管理员
        for admin_id in DEFAULT_SUPERADMINS:
            user = await self.db.get_user(admin_id)
            if not user or user['role'] != UserRole.SUPERADMIN.value:
                logger.error(f"超级管理员 {admin_id} 初始化失败")
                return False
                
        # 验证群组
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
        """主入口方法"""
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
        """启动机器人"""
        if not self.application:
            logger.error("机器人未初始化")
            return False
            
        # 启动应用
        await self.application.initialize()
        await self.application.start()
        self.running = True
        
        # 启动任务
        await self._start_broadcast_task()
        await self._start_cleanup_task()
        logger.info("机器人成功启动")
        return True
    
    async def stop(self):
        """停止机器人"""
        self.running = False
        
        # 设置关闭信号
        if self.shutdown_event:
            self.shutdown_event.set()
            
        # 停止设置管理器
        if self.settings_manager:
            await self.settings_manager.stop()
            
        # 取消清理任务
        if self.cleanup_task:
            self.cleanup_task.cancel()
            
        # 清理Web服务器
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
                
    async def shutdown(self):
        """关闭机器人"""
        await self.stop()

    async def _start_broadcast_task(self):
        """启动广播任务"""
        while self.running:
            try:
                now = datetime.now(config.TIMEZONE)
                
                # 获取应该发送的广播
                broadcasts = await self.db.db.broadcasts.find({
                    'start_time': {'$lte': now},
                    'end_time': {'$gt': now},
                }).to_list(None)
                
                # 过滤出需要发送的广播
                filtered_broadcasts = []
                for broadcast in broadcasts:
                    if 'last_broadcast' not in broadcast or broadcast['last_broadcast'] <= now - timedelta(seconds=broadcast['interval']):
                        filtered_broadcasts.append(broadcast)
                        
                # 发送广播
                for broadcast in filtered_broadcasts:
                    group_id = broadcast['group_id']
                    
                    # 检查权限
                    if not await self.has_permission(group_id, GroupPermission.BROADCAST):
                        continue
                        
                    try:
                        # 根据内容类型发送不同的消息
                        content_type = broadcast.get('content_type', 'text')
                        if content_type == 'text':
                            msg = await self.application.bot.send_message(group_id, broadcast['content'])
                        elif content_type == 'photo':
                            msg = await self.application.bot.send_photo(group_id, broadcast['content'])
                        elif content_type == 'video':
                            msg = await self.application.bot.send_video(group_id, broadcast['content'])
                        elif content_type == 'document':
                            msg = await self.application.bot.send_document(group_id, broadcast['content'])
                        else:
                            logger.error(f"不支持的内容类型: {content_type}")
                            continue
                            
                        # 处理自动删除
                        settings = await self.db.get_group_settings(group_id)
                        if settings.get('auto_delete', False):
                            timeout = validate_delete_timeout(message_type='broadcast')
                            asyncio.create_task(self._schedule_delete(msg, timeout))
                            
                        # 更新最后广播时间
                        await self.db.db.broadcasts.update_one(
                            {'_id': broadcast['_id']},
                            {'$set': {'last_broadcast': now}}
                        )
                    except Exception as e:
                        logger.error(f"发送轮播消息时出错: {e}")
                        
                # 等待一分钟再检查
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"轮播任务出错: {e}")
                await asyncio.sleep(60)

    async def _start_cleanup_task(self):
        """启动清理任务"""
        async def cleanup_routine():
            while self.running:
                try:
                    from config import DEFAULT_SETTINGS
                    # 清理旧的统计数据
                    await self.db.cleanup_old_stats(days=DEFAULT_SETTINGS.get('cleanup_days', 30))
                    # 每天运行一次
                    await asyncio.sleep(24 * 60 * 60)
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
                    # 出错时一小时后重试
                    await asyncio.sleep(1 * 60 * 60)
                    
        self.cleanup_task = asyncio.create_task(cleanup_routine())
    
    async def handle_signals(self):
        """处理系统信号"""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_running_loop().add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.stop())
                )
            logger.info("信号处理器设置完成")
        except NotImplementedError:
            logger.warning("此平台不支持信号处理器")

    async def handle_healthcheck(self, request):
        """健康检查处理函数"""
        return web.Response(text="Healthy", status=200)

    async def _handle_webhook(self, request):
        """处理Webhook请求"""
        try:
            # 验证内容类型
            if request.content_type != 'application/json':
                logger.warning(f"收到无效的内容类型: {request.content_type}")
                return web.Response(status=415)
                
            # 解析更新数据
            update_data = await request.json()
            logger.info(f"收到webhook更新: {update_data}")
            
            # 创建更新对象
            update = Update.de_json(update_data, self.application.bot)
            if update:
                # 处理更新
                await self.application.process_update(update)
                logger.info("成功处理更新")
            else:
                logger.warning("收到无效的更新数据")
                
            return web.Response(status=200)
            
        except Exception as e:
            logger.error(f"处理webhook错误: {e}", exc_info=True)
            return web.Response(status=500)

    async def is_superadmin(self, user_id: int) -> bool:
        """检查用户是否为超级管理员"""
        user = await self.db.get_user(user_id)
        return user and user['role'] == UserRole.SUPERADMIN.value
        
    async def is_admin(self, user_id: int) -> bool:
        """检查用户是否为管理员"""
        user = await self.db.get_user(user_id)
        return user and user['role'] in {UserRole.ADMIN.value, UserRole.SUPERADMIN.value}
        
    async def has_permission(self, group_id: int, permission: GroupPermission) -> bool:
        """检查群组是否有特定权限"""
        group = await self.db.get_group(group_id)
        if group:
            switches = group.get('feature_switches', {'keywords': True, 'stats': True, 'broadcast': True})
            return permission.value in group.get('permissions', []) and switches.get(permission.value, True)
        return False

    def _register_command_handler(self, command: str, handler: Callable, admin_only: bool = False):
        """注册命令处理器"""
        # 添加命令使用检查
        async def wrapper(update, context):
        
            # 如果需要，添加管理员检查
            if admin_only and not await self.is_admin(update.effective_user.id):
                await update.message.reply_text("❌ 该命令仅管理员可用")
                return
            return await handler(self, update, context)
            
        # 注册处理器
        self.application.add_handler(CommandHandler(command, wrapper))
        
    async def _register_handlers(self):
        """注册所有处理器"""
        # 注册中间件
        message_middleware = MessageMiddleware(self)
        error_middleware = ErrorHandlingMiddleware(self.error_handler)
        # 注册中间件
        self.application.post_init = [message_middleware, error_middleware]
    
        # 创建包装函数以确保正确传递self参数
        async def create_handler(handler):
            async def wrapped_handler(update, context):
                return await handler(self, update, context)
            return wrapped_handler
    
        # 直接注册命令处理器
        self.application.add_handler(CommandHandler("start", await create_handler(self._handle_start)))
        self.application.add_handler(CommandHandler("tongji", await create_handler(self._handle_rank_command)))
        self.application.add_handler(CommandHandler("tongji30", await create_handler(self._handle_rank_command)))
        self.application.add_handler(CommandHandler("settings", await create_handler(self._handle_settings)))
        self.application.add_handler(CommandHandler("admingroups", await create_handler(self._handle_admin_groups)))
        self.application.add_handler(CommandHandler("cancel", await create_handler(self._handle_cancel)))
        self.application.add_handler(CommandHandler("addsuperadmin", await create_handler(self._handle_add_superadmin)))
        self.application.add_handler(CommandHandler("delsuperadmin", await create_handler(self._handle_del_superadmin)))
        self.application.add_handler(CommandHandler("addadmin", await create_handler(self._handle_add_admin)))
        self.application.add_handler(CommandHandler("deladmin", await create_handler(self._handle_del_admin)))
        self.application.add_handler(CommandHandler("authgroup", await create_handler(self._handle_auth_group)))
        self.application.add_handler(CommandHandler("deauthgroup", await create_handler(self._handle_deauth_group)))
        self.application.add_handler(CommandHandler("checkconfig", await create_handler(self._handle_check_config)))
    
        # 注册回调查询处理器
        self.application.add_handler(CallbackQueryHandler(await create_handler(self._handle_settings_callback), pattern=r'^settings_'))
        self.application.add_handler(CallbackQueryHandler(await create_handler(self._handle_keyword_callback), pattern=r'^keyword_'))
        self.application.add_handler(CallbackQueryHandler(await create_handler(self._handle_broadcast_callback), pattern=r'^broadcast_'))
        self.application.add_handler(CallbackQueryHandler(await create_handler(self._handle_keyword_continue_callback), pattern=r'^keyword_continue_'))
        self.application.add_handler(CallbackQueryHandler(await create_handler(self._handle_stats_edit_callback), pattern=r'^stats_edit_'))
        self.application.add_handler(CallbackQueryHandler(await create_handler(self._handle_auto_delete_callback), pattern=r'^auto_delete_'))
        self.application.add_handler(CallbackQueryHandler(await create_handler(self._handle_switch_toggle_callback), pattern=r'^switch_toggle_'))
    
        # 注册消息处理器
        self.application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, await create_handler(self._handle_message)))
    
        # 注册错误处理器
        self.application.add_error_handler(self.error_handler.handle_error)

    @handle_callback_errors
    async def _handle_keyword_callback(self, update: Update, context):
        """处理关键词回调"""
        query = update.callback_query
        await query.answer()
        data = query.data
        parts = data.split('_')
        
        # 验证回调数据格式
        if len(parts) < 3:
            await query.edit_message_text("❌ 无效的操作")
            return
            
        action = parts[1]
        group_id = int(parts[-1])
        user_id = update.effective_user.id
        
        # 检查用户权限
        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return
            
        # 检查群组权限
        if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
            await query.edit_message_text("❌ 此群组未启用关键词功能")
            return
            
        # 处理不同的操作
        if action == "add":
            # 添加关键词 - 选择匹配类型
            keyboard = [
                [InlineKeyboardButton("精确匹配", callback_data=f"keyword_type_exact_{group_id}"),
                 InlineKeyboardButton("正则匹配", callback_data=f"keyword_type_regex_{group_id}")],
                [InlineKeyboardButton("取消", callback_data=f"settings_keywords_{group_id}")]
            ]
            await query.edit_message_text("请选择关键词匹配类型：", reply_markup=InlineKeyboardMarkup(keyboard))
            
        elif action == "type":
            # 选择关键词类型后的处理
            match_type = parts[2]
            logger.info(f"用户 {update.effective_user.id} 为群组 {group_id} 选择关键词匹配类型: {match_type}")
            
            # 清理已有的设置状态
            active_settings = await self.settings_manager.get_active_settings(update.effective_user.id)
            if 'keyword' in active_settings:
                await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
                
            # 创建新的设置状态
            await self.settings_manager.start_setting(update.effective_user.id, 'keyword', group_id)
            await self.settings_manager.update_setting_state(update.effective_user.id, 'keyword', {'match_type': match_type})
            
            # 提示用户输入关键词
            match_type_text = "精确匹配" if match_type == "exact" else "正则匹配"
            await query.edit_message_text(
                f"您选择了{match_type_text}方式\n\n请发送关键词内容：\n{'(支持正则表达式)' if match_type == 'regex' else ''}\n\n发送 /cancel 取消"
            )
            
        elif action == "detail":
            # 查看关键词详情
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的关键词ID")
                return
                
            keyword_id = parts[2]
            keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
            
            if not keyword:
                await query.edit_message_text("❌ 未找到该关键词")
                return
                
            # 获取关键词信息
            pattern = keyword['pattern']
            response_type = keyword['response_type']
            match_type = keyword['type']
            
            # 准备预览信息
            response_preview = "无法预览媒体内容" if response_type != 'text' else (
                keyword['response'][:100] + "..." if len(keyword['response']) > 100 else keyword['response']
            )
            response_type_text = {'text': '文本', 'photo': '图片', 'video': '视频', 'document': '文件'}.get(response_type, response_type)
            
            # 构建键盘
            keyboard = [
                [InlineKeyboardButton("❌ 删除此关键词", callback_data=f"keyword_delete_confirm_{keyword_id}_{group_id}")],
                [InlineKeyboardButton("🔙 返回列表", callback_data=f"settings_keywords_{group_id}")]
            ]
            
            # 显示详情
            text = (
                f"📝 关键词详情：\n\n"
                f"🔹 匹配类型：{'正则匹配' if match_type == 'regex' else '精确匹配'}\n"
                f"🔹 关键词：{pattern}\n"
                f"🔹 回复类型：{response_type_text}\n"
                f"🔹 回复内容：{response_preview}\n"
            )
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            
        elif action == "delete_confirm":
            # 确认删除关键词
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的关键词ID")
                return
                
            keyword_id = parts[2]
            keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
            
            if not keyword:
                await query.edit_message_text("❌ 未找到该关键词")
                return
                
            pattern = keyword['pattern']
            
            # 构建确认键盘
            keyboard = [
                [InlineKeyboardButton("✅ 确认删除", callback_data=f"keyword_delete_{keyword_id}_{group_id}"),
                 InlineKeyboardButton("❌ 取消", callback_data=f"keyword_detail_{keyword_id}_{group_id}")]
            ]
            
            # 显示确认消息
            await query.edit_message_text(
                f"⚠️ 确定要删除关键词「{pattern}」吗？\n此操作不可撤销！", 
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        elif action == "delete":
            # 执行删除关键词
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的关键词ID")
                return
                
            keyword_id = parts[2]
            keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
            pattern = keyword['pattern'] if keyword else "未知关键词"
            
            # 删除关键词
            await self.db.remove_keyword(group_id, keyword_id)
            
            # 更新关键词列表显示
            await self._show_keyword_settings(query, group_id, 1)
            
        elif action == "list_page":
            # 显示关键词列表的特定页码
            page = int(parts[2])
            await self._show_keyword_settings(query, group_id, page)

    @handle_callback_errors
    async def _handle_keyword_continue_callback(self, update: Update, context):
        """处理继续添加关键词的回调"""
        query = update.callback_query
        await query.answer()
        
        # 解析群组ID
        group_id = int(update.callback_query.data.split('_')[2])
        
        # 检查权限
        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return
            
        # 显示匹配类型选择
        keyboard = [
            [InlineKeyboardButton("精确匹配", callback_data=f"keyword_type_exact_{group_id}"),
             InlineKeyboardButton("正则匹配", callback_data=f"keyword_type_regex_{group_id}")],
            [InlineKeyboardButton("取消", callback_data=f"settings_keywords_{group_id}")]
        ]
        await query.edit_message_text("请选择关键词匹配类型：", reply_markup=InlineKeyboardMarkup(keyboard))

    @handle_callback_errors
    async def _handle_settings_callback(self, update, context):
        """处理设置菜单的回调"""
        query = update.callback_query
        logger.info(f"收到回调查询: {query.id} at {query.message.date}")
        try:
            # 立即响应回调查询
            await query.answer()    
            data = query.data
            logger.info(f"处理回调数据: {data}")
            
            # 处理返回群组列表的情况
            if data == "show_manageable_groups":
                try:
                    await self._show_manageable_groups(query, context)
                    return
                except Exception as e:
                    logger.error(f"获取可管理群组失败: {e}", exc_info=True)
                    await query.edit_message_text("❌ 获取群组列表失败，请重试")
                    return
                    
            # 解析回调数据
            parts = data.split('_')
            if len(parts) < 3:
                await query.edit_message_text("❌ 无效的回调数据格式")
                logger.error(f"无效的回调数据格式: {data}")
                return
                
            action = parts[1]
            
            # 获取群组ID
            try:
                group_id = int(parts[-1])
            except ValueError:
                await query.edit_message_text("❌ 无效的群组ID")
                logger.error(f"无效的群组ID: {parts[-1]}")
                return
                
            # 验证用户权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 你没有权限管理此群组")
                logger.warning(f"用户 {update.effective_user.id} 尝试管理无权限的群组 {group_id}")
                return
                
            # 处理不同的设置操作
            if action == "select":
                # 显示群组的设置菜单
                try:
                    await self._show_settings_menu(query, group_id)
                except Exception as e:
                    logger.error(f"显示群组 {group_id} 设置菜单失败: {e}", exc_info=True)
                    await query.edit_message_text(f"❌ 获取群组 {group_id} 设置失败，请重试")
            elif action == "switches":
                # 显示功能开关设置
                try:
                    await self._show_feature_switches(query, group_id)
                except Exception as e:
                    logger.error(f"显示功能开关设置失败 - 群组: {group_id}, 错误: {e}", exc_info=True)
                    await query.edit_message_text(f"❌ 获取功能开关设置失败，请重试")
            elif action in ["stats", "broadcast", "keywords"]:
                # 处理设置的各个子部分
                try:
                    await self._handle_settings_section(query, context, group_id, action)
                except Exception as e:
                    logger.error(f"处理设置子部分失败 - 群组: {group_id}, 操作: {action}, 错误: {e}", exc_info=True)
                    await query.edit_message_text(f"❌ 操作失败，请重试")
            else:
                # 处理其他类型的设置
                try:
                    await self._handle_settings_section(query, context, group_id, action)
                except Exception as e:
                    logger.error(f"处理设置子部分失败 - 群组: {group_id}, 操作: {action}, 错误: {e}", exc_info=True)
                    await query.edit_message_text(f"❌ 操作失败，请重试")
        except BadRequest as e:
            logger.error(f"回调查询失败: {e}")
            try:
                await context.bot.send_message(chat_id=query.message.chat_id, text="❌ 操作超时或消息已过期，请重试")
            except Exception as ex:
                logger.error(f"无法发送错误消息: {ex}", exc_info=True)
        except Exception as e:
            logger.error(f"处理设置回调时出错: {e}", exc_info=True)
            try:
                await query.edit_message_text("❌ 处理请求时出错，请重试")
            except Exception:
                try:
                    await context.bot.send_message(chat_id=query.message.chat_id, text="❌ 处理请求时出错，请重试")
                except Exception as ex:
                    logger.error(f"无法发送错误消息: {ex}", exc_info=True)

    async def _show_manageable_groups(self, query, context):
        """显示用户可管理的群组列表"""
        manageable_groups = await self.db.get_manageable_groups(query.from_user.id)
        if not manageable_groups:
            await query.edit_message_text("❌ 你没有权限管理任何群组")
            return  
            
        keyboard = []
        for group in manageable_groups:
            try:
                group_info = await context.bot.get_chat(group['group_id'])
                group_name = group_info.title or f"群组 {group['group_id']}"
            except Exception as e:
                logger.warning(f"获取群组 {group['group_id']} 信息失败: {e}")
                group_name = f"群组 {group['group_id']}"   
                
            keyboard.append([InlineKeyboardButton(group_name, callback_data=f"settings_select_{group['group_id']}")])
            
        await query.edit_message_text("请选择要管理的群组：", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_settings_menu(self, query, group_id: int):
        """显示群组设置菜单"""
        group = await self.db.get_group(group_id)
        if not group:
            await query.edit_message_text(f"❌ 找不到群组 {group_id} 的信息")
            return
            
        # 获取权限列表
        permissions = group.get('permissions', [])
        
        # 构建功能按钮
        buttons = []
        if 'stats' in permissions:
            buttons.append(InlineKeyboardButton("📊 统计设置", callback_data=f"settings_stats_{group_id}"))
        if 'broadcast' in permissions:
            buttons.append(InlineKeyboardButton("📢 轮播消息", callback_data=f"settings_broadcast_{group_id}"))
        if 'keywords' in permissions:
            buttons.append(InlineKeyboardButton("🔑 关键词设置", callback_data=f"settings_keywords_{group_id}"))
            
        # 添加开关设置按钮
        buttons.append(InlineKeyboardButton("⚙️ 开关设置", callback_data=f"settings_switches_{group_id}"))
        
        # 添加自动删除设置
        settings = await self.db.get_group_settings(group_id)
        auto_delete_status = '开启' if settings.get('auto_delete', False) else '关闭'
        buttons.append(InlineKeyboardButton(f"🗑️ 自动删除: {auto_delete_status}", 
                                           callback_data=f"auto_delete_toggle_{group_id}"))
                                           
        # 添加返回按钮
        buttons.append(InlineKeyboardButton("🔙 返回群组列表", callback_data="show_manageable_groups"))
        
        # 构建键盘
        keyboard = []
        for i in range(0, len(buttons), 2):
            row = buttons[i:i+2]
            keyboard.append(row)
            
        # 处理单个按钮的情况
        if len(buttons) % 2 != 0:
            keyboard[-1] = [buttons[-1]]
            
        # 显示设置菜单
        await query.edit_message_text(
            f"管理群组: {group_id}\n\n请选择要管理的功能：", 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    @handle_callback_errors
    async def _handle_broadcast_callback(self, update: Update, context):
        """处理轮播消息回调"""
        query = update.callback_query
        await query.answer()
        data = query.data
        parts = data.split('_')
        
        # 验证回调数据格式
        if len(parts) < 3:
            await query.edit_message_text("❌ 无效的操作")
            return
            
        action = parts[1]
        group_id = int(parts[-1])
        
        # 检查用户权限
        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return
            
        # 检查群组权限
        if not await self.has_permission(group_id, GroupPermission.BROADCAST):
            await query.edit_message_text("❌ 此群组未启用轮播功能")
            return 
            
        # 处理不同的操作
        if action == "add":
            # 开始添加轮播消息
            await self.settings_manager.start_setting(update.effective_user.id, 'broadcast', group_id)
            await query.edit_message_text(
                "请发送要轮播的内容：\n支持文本、图片、视频或文件\n\n发送 /cancel 取消"
            )  
        elif action == "detail":
            # 查看轮播消息详情
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的轮播消息ID")
                return
                
            broadcast_id = ObjectId(parts[2])
            broadcast = await self.db.db.broadcasts.find_one({'_id': broadcast_id, 'group_id': group_id}) 
            
            if not broadcast:
                await query.edit_message_text("❌ 未找到该轮播消息")
                return
                
            # 准备显示信息
            content = broadcast.get('content', '无内容')
            content_preview = str(content)[:50] + "..." if len(str(content)) > 50 else str(content)
            
            # 安全处理时间和间隔
            try:
                start_time = broadcast.get('start_time').astimezone(config.TIMEZONE).strftime('%Y-%m-%d %H:%M') if 'start_time' in broadcast else '未设置'
                end_time = broadcast.get('end_time').astimezone(config.TIMEZONE).strftime('%Y-%m-%d %H:%M') if 'end_time' in broadcast else '未设置'
            except Exception:
                start_time = '时间格式错误'
                end_time = '时间格式错误'
                
            interval = format_duration(broadcast.get('interval', 0))
            
            # 构建详情文本
            text = (
                f"📢 轮播消息详情：\n\n"
                f"🔹 类型：{broadcast.get('content_type', '未知类型')}\n"
                f"🔹 内容：{content_preview}\n"
                f"🔹 开始时间：{start_time}\n"
                f"🔹 结束时间：{end_time}\n"
                f"🔹 间隔：{interval}"
            )
            
            # 构建键盘
            keyboard = [
                [InlineKeyboardButton("❌ 删除此轮播消息", callback_data=f"broadcast_delete_{broadcast_id}_{group_id}")],
                [InlineKeyboardButton("🔙 返回列表", callback_data=f"settings_broadcast_{group_id}")]
            ]  
            
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            
        elif action == "delete":
            # 删除轮播消息
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的轮播消息ID")
                return         
                
            broadcast_id = ObjectId(parts[2])   
            
            # 检查轮播消息是否存在
            broadcast = await self.db.db.broadcasts.find_one({'_id': broadcast_id, 'group_id': group_id})
            if not broadcast:
                await query.edit_message_text("❌ 未找到该轮播消息")
                return       
                
            # 删除轮播消息
            await self.db.db.broadcasts.delete_one({'_id': broadcast_id, 'group_id': group_id})      
            
            # 更新轮播消息列表显示
            await self._show_broadcast_settings(query, group_id)

    @handle_callback_errors
    async def _handle_stats_edit_callback(self, update: Update, context):
        """处理统计设置编辑回调"""
        query = update.callback_query
        await query.answer()
        data = query.data
        logger.info(f"处理统计设置编辑回调: {data}")
        
        # 解析回调数据
        prefix = "stats_edit_"
        if not data.startswith(prefix):
            logger.error(f"无效的回调前缀: {data}")
            await query.edit_message_text("❌ 无效的操作")
            return
            
        data_without_prefix = data[len(prefix):]
        parts = data_without_prefix.rsplit('_', 1)
        if len(parts) != 2:
            logger.error(f"无效的回调数据格式: {data}")
            await query.edit_message_text("❌ 无效的操作")
            return
            
        setting_type = parts[0]
        
        try:
            group_id = int(parts[1])
        except ValueError:
            logger.error(f"无效的群组ID: {parts[1]}")
            await query.edit_message_text("❌ 无效的群组ID")
            return
            
        logger.info(f"统计设置编辑 - 类型: {setting_type}, 群组ID: {group_id}")
        
        # 权限检查
        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            logger.warning(f"用户 {update.effective_user.id} 无权限管理群组 {group_id}")
            await query.edit_message_text("❌ 无权限管理此群组")
            return
            
        if not await self.has_permission(group_id, GroupPermission.STATS):
            logger.warning(f"群组 {group_id} 未启用统计功能")
            await query.edit_message_text("❌ 此群组未启用统计功能")
            return
            
        # 获取当前设置
        try:
            settings = await self.db.get_group_settings(group_id)
            logger.info(f"群组 {group_id} 当前设置: {settings}")
        except Exception as e:
            logger.error(f"获取群组 {group_id} 设置失败: {e}", exc_info=True)
            await query.edit_message_text("❌ 获取设置信息失败")
            return
            
        # 根据设置类型处理不同的设置
        if setting_type == "min_bytes":
            # 设置最小统计字节数
            logger.info("开始设置最小统计字节数")
            try:
                await query.edit_message_text("请输入最小统计字节数：\n• 低于此值的消息将不计入统计\n• 输入 0 表示统计所有消息\n\n发送 /cancel 取消")
                await self.settings_manager.start_setting(update.effective_user.id, 'stats_min_bytes', group_id)
                logger.info(f"为用户 {update.effective_user.id}, 群组 {group_id} 启动最小字节数设置过程")
            except Exception as e:
                logger.error(f"启动最小字节数设置失败: {e}", exc_info=True)
                await query.edit_message_text("❌ 设置失败，请重试")
                
        elif setting_type == "toggle_media":
            # 切换是否统计多媒体
            logger.info("处理切换统计多媒体设置")
            try:
                # 切换设置并更新
                current_value = settings.get('count_media', False)
                new_value = not current_value
                settings['count_media'] = new_value
                await self.db.update_group_settings(group_id, settings)
                logger.info(f"更新群组 {group_id} 的count_media设置为 {new_value}")
                
                # 显示更新后的统计设置
                await self._show_stats_settings(query, group_id, settings)
                
            except Exception as e:
                logger.error(f"更新统计多媒体设置失败: {e}", exc_info=True)
                await query.edit_message_text("❌ 更新设置失败，请重试")
                
        elif setting_type == "daily_rank":
            # 设置日排行显示数量
            logger.info("开始设置日排行显示数量")
            try:
                await query.edit_message_text("请输入日排行显示的用户数量：\n• 建议在 5-20 之间\n\n发送 /cancel 取消")
                await self.settings_manager.start_setting(update.effective_user.id, 'stats_daily_rank', group_id)
                logger.info(f"为用户 {update.effective_user.id}, 群组 {group_id} 启动日排行设置过程")
            except Exception as e:
                logger.error(f"启动日排行设置失败: {e}", exc_info=True)
                await query.edit_message_text("❌ 设置失败，请重试")
                
        elif setting_type == "monthly_rank":
            # 设置月排行显示数量
            logger.info("开始设置月排行显示数量")
            try:
                await query.edit_message_text("请输入月排行显示的用户数量：\n• 建议在 5-20 之间\n\n发送 /cancel 取消")
                await self.settings_manager.start_setting(update.effective_user.id, 'stats_monthly_rank', group_id)
                logger.info(f"为用户 {update.effective_user.id}, 群组 {group_id} 启动月排行设置过程")
            except Exception as e:
                logger.error(f"启动月排行设置失败: {e}", exc_info=True)
                await query.edit_message_text("❌ 设置失败，请重试")
                
        else:
            # 未知的设置类型
            logger.warning(f"未知的设置类型: {setting_type}")
            await query.edit_message_text(f"❌ 未知的设置类型：{setting_type}")

    @handle_callback_errors
    async def _handle_auto_delete_callback(self, update: Update, context):
        """处理自动删除设置回调"""
        query = update.callback_query
        await query.answer()
        data = query.data
        parts = data.split('_')
        
        # 验证回调数据格式
        if len(parts) < 3:
            await query.edit_message_text("❌ 无效的操作")
            return
            
        action = parts[1]
        
        # 处理不同的操作
        if action in ["toggle", "timeout", "set", "custom"]:
            group_id = int(parts[-1])
            
            # 检查用户权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return
                
            # 获取当前设置
            settings = await self.db.get_group_settings(group_id)
            
            if action == "toggle":
                # 切换自动删除开关状态
                settings['auto_delete'] = not settings.get('auto_delete', False)
                await self.db.update_group_settings(group_id, settings)
                
                # 显示自动删除设置
                await self._show_auto_delete_settings(query, group_id, settings)
                
            elif action == "timeout":
                # 显示超时时间选择界面
                current_timeout = settings.get('auto_delete_timeout', config.AUTO_DELETE_SETTINGS['default_timeout'])
                
                # 构建选择键盘
                keyboard = [
                    [InlineKeyboardButton(f"{'✅' if current_timeout == 300 else ' '} 5分钟", callback_data=f"auto_delete_set_timeout_{group_id}_300")],
                    [InlineKeyboardButton(f"{'✅' if current_timeout == 600 else ' '} 10分钟", callback_data=f"auto_delete_set_timeout_{group_id}_600")],
                    [InlineKeyboardButton(f"{'✅' if current_timeout == 1800 else ' '} 30分钟", callback_data=f"auto_delete_set_timeout_{group_id}_1800")],
                    [InlineKeyboardButton("自定义", callback_data=f"auto_delete_custom_timeout_{group_id}")],
                    [InlineKeyboardButton("返回", callback_data=f"auto_delete_toggle_{group_id}")]
                ]
                
                await query.edit_message_text("请选择自动删除的超时时间：", reply_markup=InlineKeyboardMarkup(keyboard))
                
            elif action == "set":
                # 设置特定的超时时间
                if len(parts) < 4:
                    await query.edit_message_text("❌ 无效的超时时间")
                    return
                    
                timeout = int(parts[3])
                settings['auto_delete_timeout'] = timeout
                await self.db.update_group_settings(group_id, settings)
                
                # 显示更新后的自动删除设置
                await self._show_auto_delete_settings(query, group_id, settings)
                
            elif action == "custom":
                # 启动自定义超时设置流程
                await self.settings_manager.start_setting(update.effective_user.id, 'auto_delete_timeout', group_id)
                await query.edit_message_text("请输入自定义超时时间（单位：秒，60-86400）：\n\n发送 /cancel 取消")

    async def _show_auto_delete_settings(self, query, group_id: int, settings: dict):
        """显示自动删除设置"""
        status = '开启' if settings.get('auto_delete', False) else '关闭'
        timeout = settings.get('auto_delete_timeout', config.AUTO_DELETE_SETTINGS['default_timeout'])
        
        keyboard = [
            [InlineKeyboardButton(f"自动删除: {status}", callback_data=f"auto_delete_toggle_{group_id}")],
            [InlineKeyboardButton(f"超时时间: {format_duration(timeout)}", callback_data=f"auto_delete_timeout_{group_id}")],
            [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
        ]
        
        await query.edit_message_text(
            f"🗑️ 自动删除设置\n\n"
            f"当前状态: {'✅ 已开启' if settings.get('auto_delete', False) else '❌ 已关闭'}\n"
            f"超时时间: {format_duration(timeout)}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    @handle_callback_errors
    async def _show_feature_switches(self, query, group_id: int):
        """显示功能开关设置"""
        # 获取群组信息
        group = await self.db.get_group(group_id)
        if not group:
            await query.edit_message_text(f"❌ 找不到群组 {group_id} 的信息")
            return
            
        # 获取当前功能开关状态
        switches = group.get('feature_switches', {'keywords': True, 'stats': True, 'broadcast': True})
        
        # 构建功能开关菜单
        keyboard = []
        
        # 检查群组权限并显示相应的功能开关
        permissions = group.get('permissions', [])
        
        if 'stats' in permissions:
            status = '✅ 开启' if switches.get('stats', True) else '❌ 关闭'
            keyboard.append([InlineKeyboardButton(f"📊 统计功能: {status}", callback_data=f"switch_toggle_stats_{group_id}")])
            
        if 'broadcast' in permissions:
            status = '✅ 开启' if switches.get('broadcast', True) else '❌ 关闭'
            keyboard.append([InlineKeyboardButton(f"📢 轮播功能: {status}", callback_data=f"switch_toggle_broadcast_{group_id}")])
            
        if 'keywords' in permissions:
            status = '✅ 开启' if switches.get('keywords', True) else '❌ 关闭'
            keyboard.append([InlineKeyboardButton(f"🔑 关键词功能: {status}", callback_data=f"switch_toggle_keywords_{group_id}")])
            
        # 返回按钮
        keyboard.append([InlineKeyboardButton("🔙 返回设置菜单", callback_data=f"settings_select_{group_id}")])
        
        await query.edit_message_text(
            f"⚙️ 群组 {group_id} 功能开关设置\n\n"
            "点击相应按钮切换功能开关状态：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    @handle_callback_errors
    async def _handle_switch_toggle_callback(self, update, context):
        """处理功能开关切换回调"""
        query = update.callback_query
        await query.answer()
        data = query.data
        parts = data.split('_')
        
        # 验证回调数据格式
        if len(parts) < 4:
            await query.edit_message_text("❌ 无效的回调数据")
            return
            
        feature = parts[2]
        group_id = int(parts[3])
        
        # 检查用户权限
        if not await self.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 你没有权限管理此群组")
            return
            
        try:
            # 获取当前群组信息
            group = await self.db.get_group(group_id)
            if not group:
                await query.edit_message_text(f"❌ 找不到群组 {group_id} 的信息")
                return
                
            # 获取当前功能开关状态
            switches = group.get('feature_switches', {'keywords': True, 'stats': True, 'broadcast': True})
            
            # 检查该功能是否在群组权限中
            if feature not in group.get('permissions', []):
                await query.edit_message_text(f"❌ 群组 {group_id} 没有 {feature} 权限")
                return
                
            # 切换功能开关状态
            current_status = switches.get(feature, True)
            new_status = not current_status
            
            # 更新数据库
            await self.db.db.groups.update_one(
                {'group_id': group_id},
                {'$set': {f'feature_switches.{feature}': new_status}}
            )
            logger.info(f"用户 {update.effective_user.id} 将群组 {group_id} 的 {feature} 功能设置为 {new_status}")
            
            # 重新显示功能开关设置菜单
            await self._show_feature_switches(query, group_id)
            
        except Exception as e:
            logger.error(f"切换功能开关失败: {e}", exc_info=True)
            await query.edit_message_text(f"❌ 切换功能开关失败，请重试")

    async def _handle_settings_section(self, query, context, group_id: int, section: str):
        """处理设置的各个部分"""
        if section == "stats":
            # 显示统计设置
            settings = await self.db.get_group_settings(group_id)
            await self._show_stats_settings(query, group_id, settings)
        elif section == "broadcast":
            # 显示轮播消息设置
            await self._show_broadcast_settings(query, group_id)
        elif section == "keywords":
            # 显示关键词设置
            await self._show_keyword_settings(query, group_id)

    async def _show_stats_settings(self, query, group_id: int, settings: dict):
        """显示统计设置"""
        count_media_status = '✅ 开启' if settings.get('count_media', False) else '❌ 关闭'
        keyboard = [
            [InlineKeyboardButton(f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", callback_data=f"stats_edit_min_bytes_{group_id}")],
            [InlineKeyboardButton(f"统计多媒体: {count_media_status}", callback_data=f"stats_edit_toggle_media_{group_id}")],
            [InlineKeyboardButton(f"日排行显示数量: {settings.get('daily_rank_size', 15)}", callback_data=f"stats_edit_daily_rank_{group_id}")],
            [InlineKeyboardButton(f"月排行显示数量: {settings.get('monthly_rank_size', 15)}", callback_data=f"stats_edit_monthly_rank_{group_id}")],
            [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
        ]
        await query.edit_message_text(f"群组 {group_id} 的统计设置", reply_markup=InlineKeyboardMarkup(keyboard))
        
    async def _show_broadcast_settings(self, query, group_id: int):
        """显示轮播消息设置"""
        broadcasts = await self.db.get_broadcasts(group_id)
        keyboard = []  
        
        # 显示现有的轮播消息
        for bc in broadcasts:
            content_type = bc.get('content_type', '未知类型')
            content = bc.get('content', '')
            content_preview = str(content)[:20] + '...' if len(str(content)) > 20 else str(content)   
            keyboard.append([
                InlineKeyboardButton(
                    f"📢 {content_type}: {content_preview}", 
                    callback_data=f"broadcast_detail_{bc['_id']}_{group_id}"
                )
            ])
            
        # 添加功能按钮
        keyboard.append([InlineKeyboardButton("➕ 添加轮播消息", callback_data=f"broadcast_add_{group_id}")])
        keyboard.append([InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")])
        
        await query.edit_message_text(f"群组 {group_id} 的轮播消息设置", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_keyword_settings(self, query, group_id: int, page: int = 1):
        """显示关键词设置"""
        # 获取关键词列表
        keywords = await self.db.get_keywords(group_id)
        
        # 计算分页信息
        total_pages = (len(keywords) + 9) // 10
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
            
        # 获取当前页的关键词
        start_idx = (page - 1) * 10
        end_idx = min(start_idx + 10, len(keywords))
        page_keywords = keywords[start_idx:end_idx] if keywords else []
        
        # 构建关键词按钮
        keyboard = [
            [InlineKeyboardButton(f"🔑 {kw['pattern'][:20] + '...' if len(kw['pattern']) > 20 else kw['pattern']}", 
                                  callback_data=f"keyword_detail_{kw['_id']}_{group_id}")] 
            for kw in page_keywords
        ]
        
        # 添加分页导航按钮
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("◀️ 上一页", callback_data=f"keyword_list_page_{page-1}_{group_id}"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("下一页 ▶️", callback_data=f"keyword_list_page_{page+1}_{group_id}"))
            if nav_buttons:
                keyboard.append(nav_buttons)
                
        # 添加功能按钮
        keyboard.append([InlineKeyboardButton("➕ 添加关键词", callback_data=f"keyword_add_{group_id}")])
        keyboard.append([InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")])
        
        # 构建显示文本
        text = f"群组 {group_id} 的关键词设置" + (f"\n第 {page}/{total_pages} 页" if total_pages > 1 else "")
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    @check_command_usage
    async def _handle_start(self, update: Update, context):
        """处理/start命令"""
        if not update.effective_user or not update.message:
            return
            
        user_id = update.effective_user.id
        is_superadmin = await self.is_superadmin(user_id)
        is_admin = await self.is_admin(user_id)
        
        # 构建欢迎文本
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
        
        # 添加管理员命令
        if is_admin:
            welcome_text += (
                "\n管理员命令：\n"
                "👥 /admingroups - 查看可管理的群组\n"
                "⚙️ /settings - 群组设置管理\n"
            )
            
        # 添加超级管理员命令
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
        """处理/settings命令"""
        # 获取用户可管理的群组
        manageable_groups = await self.db.get_manageable_groups(update.effective_user.id)
        if not manageable_groups:
            await update.message.reply_text("❌ 你没有权限管理任何群组")
            return
            
        # 构建群组选择键盘
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
        """处理/tongji和/tongji30命令"""
        if not update.effective_chat or not update.effective_user or not update.message:
            return
            
        # 确定是哪个命令
        command = update.message.text.split('@')[0][1:]
        group_id = update.effective_chat.id
        
        # 检查权限
        if not await self.has_permission(group_id, GroupPermission.STATS):
            await update.message.reply_text("❌ 此群组未启用统计功能")
            return
            
        # 解析页码
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
        else:
            stats, total_pages = await self.stats_manager.get_monthly_stats(group_id, page)
            title = "📊 近30天发言排行"
            
        # 检查是否有统计数据
        if not stats:
            await update.effective_user.send_message("📊 暂无统计数据")
            return
            
        # 构建排行文本
        text = f"{title}\n\n"
        for i, stat in enumerate(stats, start=(page-1)*15+1):
            try:
                user = await context.bot.get_chat_member(group_id, stat['_id'])
                name = user.user.full_name or user.user.username or f"用户{stat['_id']}"
            except Exception:
                name = f"用户{stat['_id']}"
                
            text += f"{i}. {name}\n   消息数: {stat['total_messages']}\n\n"
            
        # 添加分页信息
        text += f"\n\n第 {page}/{total_pages} 页"
        if total_pages > 1:
            text += f"\n使用 /{command} <页码> 查看其他页"
            
        # 发送排行消息
        msg = await update.effective_user.send_message(text)
        
        # 处理自动删除
        settings = await self.db.get_group_settings(group_id)
        if settings.get('auto_delete', False):
            timeout = validate_delete_timeout(message_type='ranking')
            asyncio.create_task(self._schedule_delete(msg, timeout))

    @check_command_usage
    async def _handle_admin_groups(self, update: Update, context):
        """处理/admingroups命令"""
        # 检查权限
        if not await self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ 只有管理员可以使用此命令")
            return
            
        # 获取可管理的群组
        groups = await self.db.get_manageable_groups(update.effective_user.id)
        if not groups:
            await update.message.reply_text("📝 你目前没有可管理的群组")
            return
            
        # 构建群组列表文本
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
        """处理/addadmin命令"""
        # 检查权限
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以添加管理员")
            return
            
        # 检查参数
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/addadmin <用户ID>")
            return
            
        try:
            # 解析用户ID并添加管理员
            user_id = int(context.args[0])
            
            # 检查用户是否已经是管理员
            user = await self.db.get_user(user_id)
            if user and user['role'] in [UserRole.ADMIN.value, UserRole.SUPERADMIN.value]:
                await update.message.reply_text("❌ 该用户已经是管理员")
                return
                
            # 添加管理员
            await self.db.add_user({'user_id': user_id, 'role': UserRole.ADMIN.value})
            await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为管理员")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"添加管理员错误: {e}")
            await update.message.reply_text("❌ 添加管理员时出错")

    @check_command_usage
    async def _handle_del_admin(self, update: Update, context):
        """处理/deladmin命令"""
        # 检查权限
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除管理员")
            return
            
        # 检查参数
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deladmin <用户ID>")
            return
            
        try:
            # 解析用户ID
            user_id = int(context.args[0])
            
            # 检查用户
            user = await self.db.get_user(user_id)
            if not user:
                await update.message.reply_text("❌ 该用户不是管理员")
                return
                
            # 不能删除超级管理员
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
        """处理/addsuperadmin命令"""
        # 检查权限
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以添加超级管理员")
            return
            
        # 检查参数
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/addsuperadmin <用户ID>")
            return
            
        try:
            # 解析用户ID
            user_id = int(context.args[0])
            
            # 检查用户是否已经是超级管理员
            user = await self.db.get_user(user_id)
            if user and user['role'] == UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 该用户已经是超级管理员")
                return
                
            # 添加超级管理员
            await self.db.add_user({'user_id': user_id, 'role': UserRole.SUPERADMIN.value})
            await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为超级管理员")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"添加超级管理员错误: {e}")
            await update.message.reply_text("❌ 添加超级管理员时出错")

    @check_command_usage
    async def _handle_del_superadmin(self, update: Update, context):
        """处理/delsuperadmin命令"""
        # 检查权限
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除超级管理员")
            return
            
        # 检查参数
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/delsuperadmin <用户ID>")
            return
            
        try:
            # 解析用户ID
            user_id = int(context.args[0])
            
            # 不能删除自己
            if user_id == update.effective_user.id:
                await update.message.reply_text("❌ 不能删除自己的超级管理员权限")
                return
                
            # 检查用户
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
        """处理/checkconfig命令"""
        # 检查权限
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以查看配置")
            return
            
        # 获取配置信息
        superadmins = await self.db.get_users_by_role(UserRole.SUPERADMIN.value)
        superadmin_ids = [user['user_id'] for user in superadmins]
        groups = await self.db.find_all_groups()
        
        # 构建配置文本
        config_text = "🔧 当前配置信息：\n\n👥 超级管理员：\n" + "\n".join(f"• {admin_id}" for admin_id in superadmin_ids)
        config_text += "\n\n📋 已授权群组：\n" + "\n".join(f"• 群组 {group['group_id']}\n  权限: {', '.join(group.get('permissions', []))}" for group in groups)
        
        await update.message.reply_text(config_text)

    async def _handle_auth_group(self, update: Update, context):
        """处理/authgroup命令"""
        # 检查权限
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以授权群组")
            return
            
        # 检查参数
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：\n/authgroup <群组ID>")
            return
            
        try:
            # 解析群组ID
            group_id = int(context.args[0])
            
            # 获取群组信息
            try:
                group_info = await context.bot.get_chat(group_id)
                group_name = group_info.title
            except Exception:
                await update.message.reply_text("❌ 无法获取群组信息，请确保机器人已加入该群组")
                return
                
            # 授权群组
            all_permissions = [perm.value for perm in GroupPermission]
            await self.db.add_group({
                'group_id': group_id,
                'permissions': all_permissions,
                'settings': {'auto_delete': False, 'auto_delete_timeout': config.AUTO_DELETE_SETTINGS['default_timeout']},
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
        """处理/deauthgroup命令"""
        # 检查权限
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以解除群组授权")
            return
            
        # 检查参数
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deauthgroup <群组ID>")
            return
            
        try:
            # 解析群组ID
            group_id = int(context.args[0])
            
            # 检查群组
            group = await self.db.get_group(group_id)
            if not group:
                await update.message.reply_text("❌ 该群组未授权")
                return
                
            # 解除授权
            await self.db.remove_group(group_id)
            await update.message.reply_text(f"✅ 已解除群组 {group_id} 的所有授权")
            
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字")
        except Exception as e:
            logger.error(f"解除群组授权错误: {e}")
            await update.message.reply_text("❌ 解除群组授权时出错")

    @check_command_usage
    async def _handle_cancel(self, update: Update, context):
        """处理/cancel命令"""
        user_id = update.effective_user.id
        
        # 获取活动的设置
        active_settings = await self.settings_manager.get_active_settings(user_id)
        if not active_settings:
            await update.message.reply_text("❌ 当前没有正在进行的设置操作")
            return
            
        # 清除所有设置状态
        for setting_type in active_settings:
            await self.settings_manager.clear_setting_state(user_id, setting_type)
            
        await update.message.reply_text("✅ 已取消所有正在进行的设置操作")

    async def _handle_message(self, update: Update, context):
        """处理所有非命令消息"""
        logger.info("进入_handle_message方法")
        
        # 基本检查
        if not update.effective_message or not update.effective_user or not update.effective_chat:
            logger.warning("消息缺少基本属性")
            return
            
        message = update.effective_message
        user_id = update.effective_user.id
        group_id = update.effective_chat.id
        
        logger.info(f"处理消息 - 用户ID: {user_id}, 群组ID: {group_id}, 消息类型: {get_media_type(message) or 'text'}")
        
        # 检查用户活动设置状态
        active_settings = await self.settings_manager.get_active_settings(user_id)
        logger.info(f"用户 {user_id} 的活动设置: {active_settings}")
        
        # 处理关键词设置
        if await self._handle_keyword_setting(user_id, message):
            return
            
        # 处理轮播设置
        if await self._handle_broadcast_setting(user_id, group_id, message):
            return
            
        # 处理统计设置
        if await self.settings_manager.process_setting(user_id, 'stats_min_bytes', message, self._process_min_bytes_setting):
            return
            
        if await self.settings_manager.process_setting(user_id, 'stats_daily_rank', message, self._process_daily_rank_setting):
            return
            
        if await self.settings_manager.process_setting(user_id, 'stats_monthly_rank', message, self._process_monthly_rank_setting):
            return
            
        # 处理自动删除设置
        if await self.settings_manager.process_setting(user_id, 'auto_delete_timeout', message, self._process_auto_delete_timeout):
            return
        
        # 处理关键词回复
        if message.text and await self.has_permission(group_id, GroupPermission.KEYWORDS):
            logger.info(f"检查关键词匹配 - 群组: {group_id}, 文本: {message.text[:20]}...")
            response = await self.keyword_manager.match_keyword(group_id, message.text, message)
            
            if response:
                await self._send_keyword_response(message, response, group_id)
        
        # 处理消息统计
        if await self.has_permission(group_id, GroupPermission.STATS):
            try:
                await self.stats_manager.add_message_stat(group_id, user_id, message)
            except Exception as e:
                logger.error(f"添加消息统计失败: {e}", exc_info=True)

    async def _handle_keyword_setting(self, user_id: int, message: Message) -> bool:
        """处理关键词设置"""
        keyword_state = await self.settings_manager.get_setting_state(user_id, 'keyword')
        if not keyword_state:
            return False
            
        try:
            if keyword_state['step'] == 1:
                # 处理关键词模式
                pattern = message.text.strip()
                
                # 验证正则表达式
                if keyword_state['data'].get('match_type') == 'regex' and not validate_regex(pattern):
                    await message.reply_text("❌ 无效的正则表达式，请重新输入")
                    return True
                    
                # 更新状态并进入下一步
                await self.settings_manager.update_setting_state(
                    user_id, 'keyword', {'pattern': pattern}, next_step=True
                )
                await message.reply_text("请发送回复内容（支持文本、图片、视频或文件）：")
                return True
                
            elif keyword_state['step'] == 2:
                # 处理回复内容
                response_type = get_media_type(message) or 'text'
                response = message.text if response_type == 'text' else message.effective_attachment.file_id
                
                # 构建关键词数据
                keyword_data = {
                    'group_id': keyword_state['group_id'],
                    'pattern': keyword_state['data'].get('pattern', ''),
                    'type': keyword_state['data'].get('match_type', 'exact'),
                    'response_type': response_type,
                    'response': response
                }
                
                # 添加关键词到数据库
                await self.db.add_keyword(keyword_data)
                
                # 清理设置状态
                await self.settings_manager.clear_setting_state(user_id, 'keyword')
                
                # 通知用户完成
                await message.reply_text("✅ 关键词添加成功！")
                return True
                
        except Exception as e:
            logger.error(f"处理关键词设置出错: {e}", exc_info=True)
            await message.reply_text("❌ 设置过程出错，请重试或使用 /cancel 取消")
            return True
            
        return False

    async def _handle_broadcast_setting(self, user_id: int, group_id: int, message: Message) -> bool:
        """处理轮播设置"""
        broadcast_state = await self.settings_manager.get_setting_state(user_id, 'broadcast')
        if not broadcast_state or (broadcast_state['group_id'] != group_id and message.chat.type != 'private'):
            return False
            
        try:
            if broadcast_state['step'] == 1:
                # 处理轮播内容
                content_type = get_media_type(message) or 'text'
                content = message.text if content_type == 'text' else message.effective_attachment.file_id
                
                # 更新状态并进入下一步
                await self.settings_manager.update_setting_state(user_id, 'broadcast', {
                    'content_type': content_type,
                    'content': content
                }, next_step=True)
                
                await message.reply_text("请设置开始时间（格式：YYYY-MM-DD HH:MM）：")
                return True
                
            elif broadcast_state['step'] == 2:
                # 处理开始时间
                if not validate_time_format(message.text):
                    await message.reply_text("❌ 时间格式错误，请使用 YYYY-MM-DD HH:MM")
                    return True
                    
                start_time = datetime.strptime(message.text, '%Y-%m-%d %H:%M').replace(tzinfo=config.TIMEZONE)
                await self.settings_manager.update_setting_state(user_id, 'broadcast', {'start_time': start_time}, next_step=True)
                
                await message.reply_text("请设置结束时间（格式：YYYY-MM-DD HH:MM）：")
                return True
                
            elif broadcast_state['step'] == 3:
                # 处理结束时间
                if not validate_time_format(message.text):
                    await message.reply_text("❌ 时间格式错误，请使用 YYYY-MM-DD HH:MM")
                    return True
                    
                end_time = datetime.strptime(message.text, '%Y-%m-%d %H:%M').replace(tzinfo=config.TIMEZONE)
                if end_time <= broadcast_state['data']['start_time']:
                    await message.reply_text("❌ 结束时间必须晚于开始时间")
                    return True
                    
                await self.settings_manager.update_setting_state(user_id, 'broadcast', {'end_time': end_time}, next_step=True)
                
                await message.reply_text("请设置广播间隔（单位：秒，最小300秒）：")
                return True
                
            elif broadcast_state['step'] == 4:
                # 处理广播间隔
                interval = validate_interval(message.text)
                if not interval:
                    await message.reply_text("❌ 间隔必须是大于等于300秒的数字")
                    return True
                    
                # 构建广播数据
                broadcast_data = {
                    'group_id': broadcast_state['group_id'],  # 修复：使用broadcast_state
                    'content_type': broadcast_state['data']['content_type'],
                    'content': broadcast_state['data']['content'],
                    'start_time': broadcast_state['data']['start_time'],
                    'end_time': broadcast_state['data']['end_time'],
                    'interval': interval
                }
                
                # 添加广播到数据库
                await self.broadcast_manager.add_broadcast(broadcast_data)
                
                # 清理设置状态
                await self.settings_manager.clear_setting_state(user_id, 'broadcast')
                
                # 通知用户完成
                await message.reply_text("✅ 轮播消息添加成功！")
                return True
                
        except Exception as e:
            logger.error(f"处理轮播设置出错: {e}", exc_info=True)
            await message.reply_text("❌ 设置过程出错，请重试或使用 /cancel 取消")
            return True
            
        return False

    async def _send_keyword_response(self, original_message: Message, response: str, group_id: int):
        """发送关键词回复"""
        if response.startswith('__media__'):
            _, media_type, media_id = response.split('__', 2)
            
            if media_type == 'photo':
                msg = await original_message.reply_photo(media_id)
            elif media_type == 'video':
                msg = await original_message.reply_video(media_id)
            elif media_type == 'document':
                msg = await original_message.reply_document(media_id)
            else:
                return  # 不支持的媒体类型
        else:
            msg = await original_message.reply_text(response)
            
        # 处理自动删除
        settings = await self.db.get_group_settings(group_id)
        if settings.get('auto_delete', False):
            timeout = validate_delete_timeout(message_type='keyword')
            asyncio.create_task(self._schedule_delete(msg, timeout))

    async def _process_min_bytes_setting(self, state, message):
        """处理最小字节数设置"""
        group_id = state['group_id']
        try:
            value = int(message.text)
            if value < 0:
                await message.reply_text("❌ 最小字节数不能为负数")
                return
                
            # 更新设置
            settings = await self.db.get_group_settings(group_id)
            settings['min_bytes'] = value
            await self.db.update_group_settings(group_id, settings)
            
            # 清理设置状态
            await self.settings_manager.clear_setting_state(message.from_user.id, 'stats_min_bytes')
            
            # 通知用户完成
            await message.reply_text(f"✅ 最小统计字节数已设置为 {value} 字节")
        except ValueError:
            await message.reply_text("❌ 请输入一个有效的数字")

    async def _process_daily_rank_setting(self, state, message):
        """处理日排行显示数量设置"""
        group_id = state['group_id']
        try:
            value = int(message.text)
            if value < 1 or value > 50:
                await message.reply_text("❌ 显示数量必须在1-50之间")
                return
                
            # 更新设置
            settings = await self.db.get_group_settings(group_id)
            settings['daily_rank_size'] = value
            await self.db.update_group_settings(group_id, settings)
            
            # 清理设置状态
            await self.settings_manager.clear_setting_state(message.from_user.id, 'stats_daily_rank')
            
            # 通知用户完成
            await message.reply_text(f"✅ 日排行显示数量已设置为 {value}")
        except ValueError:
            await message.reply_text("❌ 请输入一个有效的数字")

    async def _process_monthly_rank_setting(self, state, message):
        """处理月排行显示数量设置"""
        group_id = state['group_id']
        try:
            value = int(message.text)
            if value < 1 or value > 50:
                await message.reply_text("❌ 显示数量必须在1-50之间")
                return
                
            # 更新设置
            settings = await self.db.get_group_settings(group_id)
            settings['monthly_rank_size'] = value
            await self.db.update_group_settings(group_id, settings)
            
            # 清理设置状态
            await self.settings_manager.clear_setting_state(message.from_user.id, 'stats_monthly_rank')
            
            # 通知用户完成
            await message.reply_text(f"✅ 月排行显示数量已设置为 {value}")
        except ValueError:
            await message.reply_text("❌ 请输入一个有效的数字")

    async def _process_auto_delete_timeout(self, state, message):
        """处理自动删除超时设置"""
        group_id = state['group_id']
        try:
            timeout = int(message.text)
            if timeout < 60 or timeout > 86400:
                await message.reply_text("❌ 超时时间必须在60-86400秒之间")
                return
                
            # 更新设置
            settings = await self.db.get_group_settings(group_id)
            settings['auto_delete_timeout'] = timeout
            await self.db.update_group_settings(group_id, settings)
            
            # 清理设置状态
            await self.settings_manager.clear_setting_state(message.from_user.id, 'auto_delete_timeout')
            
            # 通知用户完成
            await message.reply_text(f"✅ 自动删除超时时间已设置为 {format_duration(timeout)}")
        except ValueError:
            await message.reply_text("❌ 请输入一个有效的数字")

    async def _schedule_delete(self, message: Message, timeout: int):
        """计划删除消息"""
        await asyncio.sleep(timeout)
        try:
            await message.delete()
        except Exception as e:
            logger.error(f"删除消息失败: {e}")

# 启动函数
if __name__ == '__main__':
    asyncio.run(TelegramBot.main())
