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
from aiohttp import web
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes, CallbackContext
)
from dotenv import load_dotenv

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
            
        # 检查命令使用是否正确
        usage = CommandHelper.get_usage(command)
        if not usage:
            return await func(self, update, context, *args, **kwargs)
            
        # 检查管理员权限
        if usage['admin_only'] and not await self.is_admin(user_id):
            await update.message.reply_text("❌ 该命令仅管理员可用")
            return
            
        # 检查参数
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

# 工具函数模块
class Utils:
    @staticmethod
    def validate_time_format(time_str: str) -> Optional[datetime]:
        """验证时间格式"""
        try:
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M')
        except ValueError:
            return None
            
    @staticmethod
    def validate_interval(interval_str: str) -> Optional[int]:
        """验证间隔时间"""
        try:
            interval = int(interval_str)
            return interval if interval > 0 else None
        except ValueError:
            return None
            
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """格式化文件大小"""
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
        """验证正则表达式"""
        try:
            re.compile(pattern)
            return True
        except re.error:
            return False
            
    @staticmethod
    def get_media_type(message: Message) -> Optional[str]:
        """获取媒体类型"""
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
        return None
            
    @staticmethod
    def format_duration(seconds: int) -> str:
        """格式化时长"""
        if seconds < 60:
            return f"{seconds}秒"
        elif seconds < 3600:
            return f"{seconds//60}分{seconds%60}秒"
        else:
            return f"{seconds//3600}小时{(seconds%3600)//60}分{seconds%3600%60}秒"
            
    @staticmethod
    def validate_delete_timeout(message_type: str = None) -> int:
        """验证删除超时时间"""
        # 从配置中获取默认超时时间，这里使用一个默认值
        default_timeout = 300  # 5分钟
        
        # 根据消息类型可以返回不同的超时时间
        timeouts = {
            'photo': 600,    # 10分钟
            'video': 600,    # 10分钟
            'document': 600, # 10分钟
            'text': 300      # 5分钟
        }
        
        return timeouts.get(message_type, default_timeout)
            
    @staticmethod
    def is_auto_delete_exempt(role: str, command: str = None) -> bool:
        """检查是否免除自动删除"""
        # 超级管理员和管理员免除自动删除
        if role in [UserRole.ADMIN.value, UserRole.SUPERADMIN.value]:
            return True
            
        # 特定命令免除自动删除
        exempt_commands = ['/start', '/help', '/settings', '/tongji', '/tongji30']
        if command and command in exempt_commands:
            return True
            
        return False
            
    @staticmethod
    def get_message_metadata(message: Message) -> Dict:
        """获取消息元数据"""
        metadata = {
            'type': 'text',
            'size': 0,
            'duration': 0
        }
        
        if not message:
            return metadata
            
        # 设置消息类型和大小
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
        """解析命令参数"""
        if not message or not message.text:
            return []
            
        parts = message.text.split()
        if len(parts) <= 1:
            return []
            
        return parts[1:]
            
    @staticmethod
    def escape_markdown(text: str) -> str:
        """Markdown转义"""
        escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in escape_chars:
            text = text.replace(char, '\\' + char)
        return text
            
    @staticmethod
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

# 数据库模块
class Database:
    def __init__(self):
        self.client = None
        self.db = None
        
    async def connect(self, uri: str, db_name: str) -> bool:
        """连接数据库"""
        try:
            from motor.motor_asyncio import AsyncIOMotorClient
            
            self.client = AsyncIOMotorClient(uri)
            self.db = self.client[db_name]
            
            # 尝试访问以验证连接
            await self.db.command('ping')
            logger.info(f"成功连接到数据库 {db_name}")
            return True
            
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            return False
            
    async def close(self):
        """关闭数据库连接"""
        if self.client:
            self.client.close()
            logger.info("数据库连接已关闭")
            
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """获取用户"""
        return await self.db.users.find_one({'user_id': user_id})
            
    async def add_user(self, user_data: Dict) -> bool:
        """添加或更新用户"""
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
        """删除用户"""
        try:
            result = await self.db.users.delete_one({'user_id': user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除用户错误: {e}")
            return False
            
    async def get_users_by_role(self, role: str) -> List[Dict]:
        """通过角色获取用户"""
        return await self.db.users.find({'role': role}).to_list(None)
            
    async def is_user_banned(self, user_id: int) -> bool:
        """检查用户是否被封禁"""
        user = await self.get_user(user_id)
        return user and user.get('banned', False)
            
    async def get_group(self, group_id: int) -> Optional[Dict]:
        """获取群组"""
        return await self.db.groups.find_one({'group_id': group_id})
            
    async def add_group(self, group_data: Dict) -> bool:
        """添加或更新群组"""
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
        """删除群组"""
        try:
            result = await self.db.groups.delete_one({'group_id': group_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"删除群组错误: {e}")
            return False
            
    async def find_all_groups(self) -> List[Dict]:
        """获取所有群组"""
        return await self.db.groups.find().to_list(None)
            
    async def can_manage_group(self, user_id: int, group_id: int) -> bool:
        """检查用户是否可以管理群组"""
        # 获取用户信息
        user = await self.get_user(user_id)
        if not user:
            return False
            
        # 超级管理员可以管理所有群组
        if user['role'] == UserRole.SUPERADMIN.value:
            return True
            
        # 普通管理员只能管理指定的群组
        if user['role'] == UserRole.ADMIN.value:
            group = await self.get_group(group_id)
            return group is not None
            
        return False
            
    async def get_manageable_groups(self, user_id: int) -> List[Dict]:
        """获取用户可管理的群组"""
        user = await self.get_user(user_id)
        if not user:
            return []
            
        if user['role'] == UserRole.SUPERADMIN.value:
            return await self.find_all_groups()
        elif user['role'] == UserRole.ADMIN.value:
            # 管理员可以管理所有已授权群组
            return await self.find_all_groups()
            
        return []
            
    async def get_group_settings(self, group_id: int) -> Dict:
        """获取群组设置"""
        group = await self.get_group(group_id)
        if not group:
            return {}
            
        settings = group.get('settings', {})
        # 合并默认设置
        from config import DEFAULT_SETTINGS
        return {**DEFAULT_SETTINGS, **settings}
            
    async def update_group_settings(self, group_id: int, settings: Dict) -> bool:
        """更新群组设置"""
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
        """获取群组关键词列表"""
        return await self.db.keywords.find({'group_id': group_id}).to_list(None)
            
    async def add_keyword(self, keyword_data: Dict) -> ObjectId:
        """添加关键词"""
        result = await self.db.keywords.insert_one(keyword_data)
        return result.inserted_id
            
    async def remove_keyword(self, group_id: int, keyword_id: str) -> bool:
        """删除关键词"""
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
        """添加消息统计"""
        try:
            # 更新现有记录或插入新记录
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
                    }
                },
                upsert=True
            )
            return result.acknowledged
        except Exception as e:
            logger.error(f"添加消息统计错误: {e}")
            return False
            
    async def get_recent_message_count(self, user_id: int, seconds: int = 60) -> int:
        """获取用户最近的消息数量"""
        try:
            time_threshold = datetime.now() - timedelta(seconds=seconds)
            count = await self.db.message_stats.count_documents({
                'user_id': user_id,
                'timestamp': {'$gte': time_threshold}
            })
            return count
        except Exception as e:
            logger.error(f"获取最近消息数量错误: {e}")
            return 0
            
    async def cleanup_old_stats(self, days: int = 30) -> bool:
        """清理旧统计数据"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
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
        """更新设置状态"""
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

# 统计管理模块
class StatsManager:
    def __init__(self, db):
        self.db = db

    async def add_message_stat(self, group_id: int, user_id: int, message: Message):
        """添加消息统计"""
        media_type = Utils.get_media_type(message)
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
        """获取每日统计，仅统计消息数量
        
        Args:
            group_id: 群组ID
            page: 页码,从1开始
            
        Returns:
            Tuple[List[Dict], int]: 统计数据列表和总页数
        """
        
        today = datetime.now().strftime('%Y-%m-%d')
        # 每页15条,最多显示前100名
        limit = 15
        max_users = 100
        
        pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': today
            }},
            {'$group': {
                '_id': '$user_id',
                'total_messages': {'$sum': '$total_messages'}
            }},
            {'$sort': {'total_messages': -1}},
            {'$limit': max_users}
        ]
        all_stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
        
        # 计算总页数
        total_users = len(all_stats)
        total_pages = (total_users + limit - 1) // limit
        
        # 获取当前页的数据
        start_idx = (page - 1) * limit
        end_idx = min(start_idx + limit, total_users)
        stats = all_stats[start_idx:end_idx]
        
        return stats, total_pages

    async def get_monthly_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取近30日统计,仅统计消息数量
        
        Args:
            group_id: 群组ID
            page: 页码,从1开始
            
        Returns:
            Tuple[List[Dict], int]: 统计数据列表和总页数
        """
        
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        # 每页15条,最多显示前100名
        limit = 15
        max_users = 100
        
        pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': {'$gte': thirty_days_ago}
            }},
            {'$group': {
                '_id': '$user_id',
                'total_messages': {'$sum': '$total_messages'}
            }},
            {'$sort': {'total_messages': -1}},
            {'$limit': max_users}
        ]
        all_stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
        
        # 计算总页数
        total_users = len(all_stats)
        total_pages = (total_users + limit - 1) // limit
        
        # 获取当前页的数据
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
        return await self.db.db.broadcasts.find({'group_id': group_id}).to_list(None)
        
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
        now = datetime.now()
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
                {'$set': {'last_broadcast': datetime.now()}}
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

# 错误处理模块
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

# 中间件模块
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

# 命令帮助模块
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
            # 验证配置
            try:
                from config_validator import validate_config, ConfigValidationError
                import config
        
                try:
                    validate_config(config)
                except ConfigValidationError as e:
                    logger.error(f"配置验证失败: {e}")
                    return False
            except ImportError:
                logger.warning("配置验证模块未找到，跳过配置验证")
                
            logger.info("开始初始化机器人")
            
            # 从config导入必要配置
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
    
            # 初始化数据库
            self.db = Database()
            if not await self.db.connect(MONGODB_URI, MONGODB_DB):
                logger.error("数据库连接失败")
                return False
    
            # 初始化管理器
            self.error_handler = ErrorHandler(logger)
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
        
            # 初始化默认群组 - 所有群组默认拥有全部功能权限
            default_groups = [
                {
                    'group_id': -1001234567890,  # 替换为你的群组ID
                    'permissions': [perm.value for perm in GroupPermission]  # 全部权限
                }
                # 可以添加更多群组
            ]
        
            for group in default_groups:
                await self.db.add_group({
                    'group_id': group['group_id'],
                    'permissions': group['permissions']
                })
                logger.info(f"已设置群组权限: {group['group_id']}")
        
            # 获取webhook域名
            webhook_domain = os.getenv('WEBHOOK_DOMAIN')
            if not webhook_domain:
                logger.warning("WEBHOOK_DOMAIN环境变量未设置，使用默认值")
                webhook_domain = 'your-render-app-name.onrender.com'
        
            # 创建Telegram Bot应用
            self.application = (
                Application.builder()
                .token(TELEGRAM_TOKEN)
                .build()
            )
        
            # 注册处理器
            await self._register_handlers()
        
            # 创建 web 应用并添加路由
            self.web_app = web.Application()
            self.web_app.router.add_get('/', self.handle_healthcheck)
            self.web_app.router.add_get('/health', self.handle_healthcheck)

            # 设置webhook路径并添加路由
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

            # 禁用轮询
            self.application.updater = None
        
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
        try:
            # 从config导入必要配置
            from config import DEFAULT_SUPERADMINS
            
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
            
        except Exception as e:
            logger.error(f"验证初始化失败: {e}")
            return False

    @classmethod
    async def main(cls):
        """主函数"""
        try:
            # 验证环境变量
            Utils.verify_environment()
        
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
        
            await self.application.initialize()
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
                    from config import DEFAULT_SETTINGS
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
                await self.application.process_update(update)
                logger.info("成功处理更新")
            else:
                logger.warning("收到无效的更新数据")
        
            return web.Response(status=200)
        except Exception as e:
            logger.error(f"处理webhook错误: {e}", exc_info=True)
            return web.Response(status=500)

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
            parts = data.split('_')
    
            # 确保有足够的参数
            if len(parts) < 3:
                await query.edit_message_text("❌ 无效的操作")
                return

            action = parts[1]  # detail/add/edit/delete/type

            # 获取群组ID
            try:
                group_id = int(parts[-1])
            except ValueError:
                await query.edit_message_text("❌ 无效的群组ID")
                return

            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return

            if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
                await query.edit_message_text("❌ 此群组未启用关键词功能")
                return

            # 处理不同的关键词操作
            if action == "add":
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

            elif action == "type":
                match_type = parts[2]  # exact/regex
            
                # 记录详细日志
                logger.info(f"用户 {update.effective_user.id} 为群组 {group_id} 选择关键词匹配类型: {match_type}")
            
                # 检查是否已有正在进行的关键词设置
                active_settings = await self.settings_manager.get_active_settings(update.effective_user.id)
                if 'keyword' in active_settings:
                    # 清除之前的状态
                    await self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
            
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

                # 提示输入关键词
                match_type_text = "精确匹配" if match_type == "exact" else "正则匹配"
                await query.edit_message_text(
                    f"您选择了{match_type_text}方式\n\n"
                    f"请发送关键词内容：\n"
                    f"{'(支持正则表达式)' if match_type == 'regex' else ''}\n\n"
                    "发送 /cancel 取消"
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

                # 构建详情界面的键盘
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "❌ 删除此关键词", 
                            callback_data=f"keyword_delete_confirm_{keyword_id}_{group_id}"
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

            elif action == "delete_confirm":
                if len(parts) < 4:
                    await query.edit_message_text("❌ 无效的关键词ID")
                    return

                keyword_id = parts[2]
            
                # 获取关键词信息用于显示
                keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                if not keyword:
                    await query.edit_message_text("❌ 未找到该关键词")
                    return
                
                pattern = keyword['pattern']
            
                # 构建确认删除的键盘
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "✅ 确认删除", 
                            callback_data=f"keyword_delete_{keyword_id}_{group_id}"
                        ),
                        InlineKeyboardButton(
                            "❌ 取消", 
                            callback_data=f"keyword_detail_{keyword_id}_{group_id}"
                        )
                    ]
                ]
            
                await query.edit_message_text(
                    f"⚠️ 确定要删除关键词「{pattern}」吗？\n"
                    "此操作不可撤销！",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif action == "delete":
                if len(parts) < 4:
                    await query.edit_message_text("❌ 无效的关键词ID")
                    return

                keyword_id = parts[2]
            
                try:
                    # 获取关键词信息用于显示
                    keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                    pattern = keyword['pattern'] if keyword else "未知关键词"
                
                    # 执行删除
                    await self.db.remove_keyword(group_id, keyword_id)
                
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
                    page = int(parts[2])
                    await self.settings_manager.set_current_page(group_id, "keywords", page)
                    await self._show_keyword_settings(query, group_id, page)
                except ValueError:
                    await query.edit_message_text("❌ 无效的页码")

            else:
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
            parts = data.split('_')
        
            # 确保有足够的参数
            if len(parts) < 3:
                await query.edit_message_text("❌ 无效的操作")
                return
    
            group_id = int(parts[2])

            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
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

        except Exception as e:
            logger.error(f"处理关键词继续添加回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理操作时出错，请重试")

    async def _handle_settings_callback(self, update: Update, context):
        """处理设置菜单的回调查询"""
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

    @handle_callback_errors
    async def _handle_broadcast_callback(self, update: Update, context):
        """处理轮播消息的回调查询"""
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
            
            # 获取群组ID，通常在回调数据的最后一部分
            try:
                group_id = int(parts[-1])
            except ValueError:
                await query.edit_message_text("❌ 无效的群组ID")
                return

            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return

            if action == "add":
                # 开始添加轮播消息流程
                await self.settings_manager.start_setting(
                    update.effective_user.id,
                    'broadcast',
                    group_id
                )
                
                await query.edit_message_text(
                    "请发送要轮播的内容：\n"
                    "支持文本、图片、视频或文件\n\n"
                    "发送 /cancel 取消"
                )

            elif action == "detail":
                if len(parts) < 4:
                    await query.edit_message_text("❌ 无效的轮播消息ID")
                    return
                
                broadcast_id = ObjectId(parts[2])
                broadcast = await self.db.db.broadcasts.find_one({
                    '_id': broadcast_id,
                    'group_id': group_id
                })
                
                if not broadcast:
                    await query.edit_message_text("❌ 未找到该轮播消息")
                    return
                
                # 显示详情
                content_preview = str(broadcast['content'])
                if len(content_preview) > 50:
                    content_preview = content_preview[:47] + "..."
                
                start_time = broadcast['start_time'].strftime('%Y-%m-%d %H:%M')
                end_time = broadcast['end_time'].strftime('%Y-%m-%d %H:%M')
                interval = Utils.format_duration(broadcast['interval'])
                
                text = (
                    f"📢 轮播消息详情：\n\n"
                    f"🔹 类型：{broadcast['content_type']}\n"
                    f"🔹 内容：{content_preview}\n"
                    f"🔹 开始时间：{start_time}\n"
                    f"🔹 结束时间：{end_time}\n"
                    f"🔹 间隔：{interval}"
                )
                
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "❌ 删除此轮播消息", 
                            callback_data=f"broadcast_delete_{broadcast_id}_{group_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "🔙 返回列表", 
                            callback_data=f"settings_broadcast_{group_id}"
                        )
                    ]
                ]
                
                await query.edit_message_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif action == "delete":
                if len(parts) < 4:
                    await query.edit_message_text("❌ 无效的轮播消息ID")
                    return
                
                broadcast_id = ObjectId(parts[2])
                
                # 执行删除
                await self.db.db.broadcasts.delete_one({
                    '_id': broadcast_id,
                    'group_id': group_id
                })
                
                # 显示删除成功消息并返回列表
                await query.edit_message_text("✅ 已删除轮播消息")
                await asyncio.sleep(1)
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
            parts = data.split('_')
            
            # 确保有足够的参数
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的操作")
                return
            
            setting_type = parts[2]
            
            try:
                group_id = int(parts[-1])
            except ValueError:
                await query.edit_message_text("❌ 无效的群组ID")
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
                "✅ /authgroup <群组ID>  ... - 授权群组\n"
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

    @check_command_usage
    async def _handle_rank_command(self, update: Update, context):
        """处理统计命令（tongji/tongji30）"""
        if not update.effective_chat or not update.effective_user or not update.message:
            return
            
        try:
            command = update.message.text.split('@')[0][1:]  # 移除 / 和机器人用户名
            group_id = update.effective_chat.id
            
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
            
            for i, stat in enumerate(stats, start=(page-1)*15+1):
                try:
                    user = await context.bot.get_chat_member(group_id, stat['_id'])
                    name = user.user.full_name or user.user.username or f"用户{stat['_id']}"
                except Exception:
                    name = f"用户{stat['_id']}"
                
                text += f"{i}. {name}\n"
                text += f"   消息数: {stat['total_messages']}\n\n"
            
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

    @check_command_usage
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
            all_permissions = [perm.value for perm in GroupPermission]
        
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
                    callback_data=f"broadcast_detail_{bc['_id']}_{group_id}"
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
        """显示关键词设置页面"""
        keywords = await self.db.get_keywords(group_id)
    
        # 分页逻辑，每页显示10个关键词
        total_pages = (len(keywords) + 9) // 10
        if page < 1:
            page = 1
        if page > total_pages:
            page = total_pages
    
        start_idx = (page - 1) * 10
        end_idx = min(start_idx + 10, len(keywords))
        page_keywords = keywords[start_idx:end_idx] if keywords else []
    
        keyboard = []
        for kw in page_keywords:
            keyword_text = kw['pattern'][:20] + '...' if len(kw['pattern']) > 20 else kw['pattern']
            keyboard.append([
                InlineKeyboardButton(
                    f"🔑 {keyword_text}", 
                    callback_data=f"keyword_detail_{kw['_id']}_{group_id}"
                )
            ])
    
        # 添加分页导航按钮
        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton(
                        "◀️ 上一页", 
                        callback_data=f"keyword_list_page_{page-1}_{group_id}"
                    )
                )
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton(
                        "下一页 ▶️", 
                        callback_data=f"keyword_list_page_{page+1}_{group_id}"
                    )
                )
            if nav_buttons:
                keyboard.append(nav_buttons)
    
        # 添加新增关键词和返回按钮
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
    
        # 构建消息文本
        text = f"群组 {group_id} 的关键词设置"
        if total_pages > 1:
            text += f"\n第 {page}/{total_pages} 页"
    
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def _process_stats_setting(self, update: Update, context, stats_state, setting_type):
        """处理统计设置编辑"""
        try:
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

    async def _process_keyword_adding(self, update: Update, context, setting_state):
        """处理关键词添加流程"""
        try:
            if not setting_state:
                await update.message.reply_text("❌ 设置会话已过期，请重新开始")
                return
            
            step = setting_state.get('step', 1)
            group_id = setting_state.get('group_id')
            data = setting_state.get('data', {})
            match_type = data.get('match_type')
        
            # 接收关键词
            if step == 1:
                pattern = update.message.text.strip()
            
                # 更新状态
                new_data = {'pattern': pattern, 'type': match_type}
                await self.settings_manager.update_setting_state(update.effective_user.id, 'keyword', new_data)
            
                # 手动更新步骤到2
                state_key = f"setting_{update.effective_user.id}_keyword"
                async with asyncio.Lock():
                    if state_key in self.settings_manager._states:
                        self.settings_manager._states[state_key]['step'] = 2
                        self.settings_manager._states[state_key]['timestamp'] = datetime.now()
            
                # 提示用户输入回复内容
                await update.message.reply_text(
                    "✅ 关键词已设置\n\n"
                    "请发送此关键词的回复内容（支持文字/图片/视频/文件）：\n\n"
                    "发送 /cancel 取消设置"
                )
            
            elif step == 2:  # 添加回复内容
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
            
                if not response_type or response_content is None:
                    await update.message.reply_text("❌ 请发送有效的回复内容（文本/图片/视频/文件）")
                    return
                
                # 从状态中获取之前设置的数据
                pattern = data.get('pattern')
                pattern_type = data.get('type')
            
                if not pattern or not pattern_type:
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
                    await update.message.reply_text("❌ 保存关键词时出错，请重试")
                    await self.settings_manager.clear_setting_state(
                        update.effective_user.id, 
                        'keyword'
                    )
                
            else:
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
            await update.message.reply_text("❌ 只有管理员可以使用此命令")
            return

    async def _process_broadcast_adding(self, update: Update, context, setting_state):
        """处理轮播消息添加流程"""
        try:
            if not setting_state:
                await update.message.reply_text("❌ 设置会话已过期，请重新开始")
                return
            
            step = setting_state.get('step', 1)
            group_id = setting_state.get('group_id')
            data = setting_state.get('data', {})
            
            if step == 1:  # 获取消息内容
                # 自动检测消息类型
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

                # 检查内容限制
                if content_type == 'text' and len(content) > 4096:  # Telegram消息长度限制
                    await update.message.reply_text("❌ 文本内容过长")
                    await self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')
                    return

                # 更新状态
                await self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'broadcast',
                    {'content_type': content_type, 'content': content}
                )
            
                # 手动更新步骤到2
                state_key = f"setting_{update.effective_user.id}_broadcast"
                async with asyncio.Lock():
                    if state_key in self.settings_manager._states:
                        self.settings_manager._states[state_key]['step'] = 2
                        self.settings_manager._states[state_key]['timestamp'] = datetime.now()

                await update.message.reply_text(
                    "✅ 内容已设置\n\n"
                    "请设置轮播时间参数：\n"
                    "格式：开始时间 结束时间 间隔秒数\n"
                    "例如：2024-02-22 08:00 2024-03-22 20:00 3600\n\n"
                    "发送 /cancel 取消"
                )

            elif step == 2:  # 设置时间参数
                try:
                    parts = update.message.text.split()
                    if len(parts) != 5:
                        raise ValueError("参数数量不正确")

                    start_time = Utils.validate_time_format(f"{parts[0]} {parts[1]}")
                    end_time = Utils.validate_time_format(f"{parts[2]} {parts[3]}")
                    interval = Utils.validate_interval(parts[4])

                    if not all([start_time, end_time, interval]):
                        raise ValueError("时间格式无效")

                    if start_time >= end_time:
                        raise ValueError("结束时间必须晚于开始时间")

                    from config import BROADCAST_SETTINGS
                    if interval < BROADCAST_SETTINGS.get('min_interval', 60):
                        raise ValueError(f"间隔时间不能小于{BROADCAST_SETTINGS.get('min_interval', 60)}秒")

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

    # 消息处理相关
    def _create_navigation_keyboard(self,current_page: int,total_pages: int, base_callback: str) -> List[List[InlineKeyboardButton]]:
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

    async def handle_keyword_response(self, chat_id: int, response: str, context, original_message: Optional[Message] = None) -> Optional[Message]:
        """处理关键词响应,并可能进行自动删除
    
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
                metadata = Utils.get_message_metadata(original_message) if original_message else {}
            
                # 计算删除超时时间
                timeout = Utils.validate_delete_timeout(
                    message_type=metadata.get('type')
                )
            
                # 调度消息删除
                await self.message_deletion_manager.schedule_message_deletion(
                    sent_message, 
                    timeout
                )
        
            return sent_message

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

            # 处理取消操作
            if message.text and message.text.lower() == '/cancel':
                # 获取用户的活动设置状态
                active_settings = await self.settings_manager.get_active_settings(user_id)
                if active_settings:
                    for setting_type in active_settings:
                        await self.settings_manager.clear_setting_state(user_id, setting_type)
                    await message.reply_text(f"✅ 已取消设置操作")
                    return
                else:
                    await message.reply_text("❓ 当前没有进行中的设置操作")
                    return

            # 检查是否免除自动删除
            command = message.text.split()[0] if message.text else None
            if not Utils.is_auto_delete_exempt(user_role, command):
                # 获取消息元数据
                metadata = Utils.get_message_metadata(message)
                # 计算删除超时时间
                timeout = Utils.validate_delete_timeout(
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

# 入口点
if __name__ == '__main__':
    try:
        asyncio.run(TelegramBot.main())
    except KeyboardInterrupt:
        logger.info("机器人被用户停止")
    except Exception as e:
        logger.error(f"机器人停止，错误原因: {e}")
        logger.error(traceback.format_exc())
        raise
