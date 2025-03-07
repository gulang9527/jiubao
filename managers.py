import re
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple, Callable, Union
from bson import ObjectId

from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

from db import Database, GroupPermission
from utils import get_media_type

logger = logging.getLogger(__name__)

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
                import config
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
        import config
        state_key = f"page_{group_id}_{section}"
        async with asyncio.Lock():
            self._states[state_key] = {
                'page': page,
                'timestamp': datetime.now(config.TIMEZONE)
            }
            logger.info(f"设置页码: {state_key} => {page}")
            
    async def start_setting(self, user_id: int, setting_type: str, group_id: int):
        """开始设置会话"""
        import config
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
                'group_id': group_id,
                'step': 1,
                'data': {},
                'timestamp': datetime.now(config.TIMEZONE)
            }
            logger.info(f"创建设置状态: {state_key}, 群组: {group_id}")
        
    async def get_setting_state(self, user_id: int, setting_type: str) -> Optional[dict]:
        """获取设置状态"""
        import config
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
        import config
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
            'group_id': group_id,
            'user_id': user_id,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_messages': 1,
            'total_size': message_size,
            'media_type': media_type
        }
        await self.db.add_message_stat(stat_data)

    async def get_daily_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取每日统计数据"""
        today = datetime.now().strftime('%Y-%m-%d')
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
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
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
        # 验证必要字段
        required_fields = ['group_id', 'start_time', 'end_time', 'interval']
        for field in required_fields:
            if field not in broadcast_data:
                raise ValueError(f"缺少必要字段 '{field}'")
            
        # 确保至少有文本、媒体或按钮之一
        if not (broadcast_data.get('text') or broadcast_data.get('media') or broadcast_data.get('buttons')):
            raise ValueError("轮播消息必须包含文本、媒体或按钮中的至少一项")
        
        # 验证时间设置
        if broadcast_data['start_time'] >= broadcast_data['end_time']:
            raise ValueError("结束时间必须晚于开始时间")
        
        # 验证间隔设置
        import config
        min_interval = config.BROADCAST_SETTINGS['min_interval']
        if broadcast_data['interval'] < min_interval:
            raise ValueError(f"间隔不能小于 {min_interval} 秒")
    
        # 添加到数据库
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
        """更新最后广播时间"""
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
        """注册内置关键词处理函数"""
        self._built_in_keywords[pattern] = handler
        
    async def match_keyword(self, group_id: int, text: str, message: Message) -> Optional[str]:
        """匹配消息中的关键词"""
        logger.info(f"开始匹配关键词 - 群组: {group_id}, 文本: {text[:20]}...")

        # 匹配内置关键词
        for pattern, handler in self._built_in_keywords.items():
            logger.info(f"尝试匹配内置关键词: {pattern}")
            if text == pattern:
                logger.info(f"内置关键词匹配成功: {pattern}")
                return await handler(message)
        
        # 匹配自定义关键词
        keywords = await self.get_keywords(group_id)
        logger.info(f"群组 {group_id} 有 {len(keywords)} 个关键词")
            
        # 匹配自定义关键词
        keywords = await self.get_keywords(group_id)
        logger.info(f"群组 {group_id} 有 {len(keywords)} 个关键词")
    
        for kw in keywords:
            logger.info(f"尝试匹配关键词: {kw['pattern']}, 类型: {kw['type']}")
            try:
                if kw['type'] == 'regex':
                    pattern = re.compile(kw['pattern'])
                    if pattern.search(text):
                        return str(kw['_id'])  # 返回关键词ID
                else:
                    if text == kw['pattern']:
                        return str(kw['_id'])  # 返回关键词ID
            except Exception as e:
                logger.error(f"匹配关键词 {kw['pattern']} 时出错: {e}")
                continue
            
        # 检查URL链接模式
        url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        if url_pattern.search(text):
            # 遍历URL关键词
            for kw in keywords:
                if kw.get('is_url_handler', False):
                    return str(kw['_id'])
                
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
