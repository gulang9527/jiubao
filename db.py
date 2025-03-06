import logging
import asyncio
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
from bson import ObjectId

# 配置日志
logger = logging.getLogger(__name__)

class UserRole(Enum):
    USER = "user"
    ADMIN = "admin"
    SUPERADMIN = "superadmin"

class GroupPermission(Enum):
    KEYWORDS = "keywords"
    STATS = "stats"
    BROADCAST = "broadcast"

class Database:
    def __init__(self):
        self.client = None
        self.db = None
        self.uri = None
        self.database = None
        self._reconnect_task = None
        self.connected = asyncio.Event()
        
    async def connect(self, mongodb_uri: str, database: str) -> bool:
        """连接到MongoDB"""
        self.uri = mongodb_uri
        self.database = database
        
        try:
            self.client = AsyncIOMotorClient(mongodb_uri)
            # 验证连接
            await self.client.admin.command('ping')
            self.db = self.client[database]
            logger.info("数据库连接成功")
            
            # 验证集合是否存在
            collections = await self.db.list_collection_names()
            required_collections = [
                'users', 'groups', 'keywords', 'broadcasts', 
                'message_stats', 'admin_groups'
            ]
            
            for collection in required_collections:
                if collection not in collections:
                    logger.warning(f"创建集合: {collection}")
                    await self.db.create_collection(collection)
            
            # 初始化索引
            await self.init_indexes()
            
            # 设置连接状态
            self.connected.set()
            
            # 启动重连任务
            self._start_reconnect_task()
            
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            self.connected.clear()
            return False

    def _start_reconnect_task(self):
        """启动重连任务"""
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())
            
    async def _reconnect_loop(self):
        """重连循环"""
        while True:
            if not self.connected.is_set():
                try:
                    self.client = AsyncIOMotorClient(self.uri)
                    await self.client.admin.command('ping')
                    self.db = self.client[self.database]
                    logger.info("数据库重连成功")
                    self.connected.set()
                except Exception as e:
                    logger.error(f"数据库重连失败: {e}")
                    await asyncio.sleep(5)  # 等待5秒后重试
            await asyncio.sleep(60)  # 每分钟检查一次连接状态
            
    async def ensure_connected(self):
        """确保数据库已连接"""
        await self.connected.wait()

    async def close(self):
        """关闭数据库连接"""
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        
        if self.client:
            self.client.close()
            self.client = None
            self.connected.clear()
            logger.info("数据库连接已关闭")

    async def init_indexes(self):
        """初始化所有集合的索引"""
        try:
            # 用户索引
            await self.db.users.create_index(
                [("user_id", ASCENDING)],
                unique=True
            )
            
            # 群组索引
            await self.db.groups.create_index(
                [("group_id", ASCENDING)],
                unique=True
            )
            
            # 关键词索引
            await self.db.keywords.create_index([
                ("group_id", ASCENDING),
                ("pattern", ASCENDING)
            ])
            
            # 轮播消息索引
            await self.db.broadcasts.create_index([
                ("group_id", ASCENDING),
                ("end_time", ASCENDING)
            ])
            
            # 消息统计索引
            await self.db.message_stats.create_index([
                ("group_id", ASCENDING),
                ("user_id", ASCENDING),
                ("date", ASCENDING)
            ])
            
            # 群组管理员索引
            await self.db.admin_groups.create_index([
                ("admin_id", ASCENDING),
                ("group_id", ASCENDING)
            ], unique=True)
            
            logger.info("索引初始化完成")
        except Exception as e:
            logger.error(f"索引初始化失败: {e}")
            raise

    # User related methods
    async def add_user(self, user_data: Dict[str, Any]):
        """添加或更新用户"""
        await self.ensure_connected()
        try:
            await self.db.users.update_one(
                {'user_id': user_data['user_id']},
                {'$set': user_data},
                upsert=True
            )
        except Exception as e:
            logger.error(f"添加用户失败: {e}")
            raise

    async def remove_user(self, user_id: int):
        """删除用户"""
        await self.ensure_connected()
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    await self.db.users.delete_one(
                        {'user_id': user_id},
                        session=session
                    )
                    await self.db.admin_groups.delete_many(
                        {'admin_id': user_id},
                        session=session
                    )
                except Exception as e:
                    await session.abort_transaction()
                    logger.error(f"删除用户失败: {e}")
                    raise

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """获取用户信息"""
        await self.ensure_connected()
        try:
            return await self.db.users.find_one({'user_id': user_id})
        except Exception as e:
            logger.error(f"获取用户失败: {e}")
            return None

    async def get_users_by_role(self, role: str) -> List[Dict[str, Any]]:
        """获取指定角色的所有用户"""
        await self.ensure_connected()
        try:
            return await self.db.users.find({'role': role}).to_list(None)
        except Exception as e:
            logger.error(f"获取用户列表失败: {e}")
            return []

    async def is_user_banned(self, user_id: int) -> bool:
        """检查用户是否被封禁"""
        await self.ensure_connected()
        try:
            user = await self.db.users.find_one({
                'user_id': user_id,
                'is_banned': True
            })
            return bool(user)
        except Exception as e:
            logger.error(f"检查用户封禁状态失败: {e}")
            return False

    # Group related methods
    async def add_group(self, group_data: Dict[str, Any]):
        """添加或更新群组"""
        await self.ensure_connected()
        try:
            await self.db.groups.update_one(
                {'group_id': group_data['group_id']},
                {'$set': group_data},
                upsert=True
            )
        except Exception as e:
            logger.error(f"添加群组失败: {e}")
            raise

    async def remove_group(self, group_id: int):
        """删除群组"""
        await self.ensure_connected()
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    await self.db.groups.delete_one(
                        {'group_id': group_id},
                        session=session
                    )
                    await self.db.admin_groups.delete_many(
                        {'group_id': group_id},
                        session=session
                    )
                    await self.db.keywords.delete_many(
                        {'group_id': group_id},
                        session=session
                    )
                    await self.db.broadcasts.delete_many(
                        {'group_id': group_id},
                        session=session
                    )
                except Exception as e:
                    await session.abort_transaction()
                    logger.error(f"删除群组失败: {e}")
                    raise

    async def get_group(self, group_id: int) -> Optional[Dict[str, Any]]:
        """获取群组信息"""
        await self.ensure_connected()
        try:
            return await self.db.groups.find_one({'group_id': group_id})
        except Exception as e:
            logger.error(f"获取群组失败: {e}")
            return None

    async def find_all_groups(self) -> List[Dict[str, Any]]:
        """获取所有群组"""
        await self.ensure_connected()
        try:
            return await self.db.groups.find().to_list(None)
        except Exception as e:
            logger.error(f"获取群组列表失败: {e}")
            return []

    async def get_group_settings(self, group_id: int) -> Dict[str, Any]:
        """获取群组设置"""
        await self.ensure_connected()
        try:
            group = await self.get_group(group_id)
            return group.get('settings', {}) if group else {}
        except Exception as e:
            logger.error(f"获取群组设置失败: {e}")
            return {}

    async def update_group_settings(self, group_id: int, settings: Dict[str, Any]):
        """更新群组设置"""
        await self.ensure_connected()
        try:
            await self.db.groups.update_one(
                {'group_id': group_id},
                {'$set': {'settings': settings}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"更新群组设置失败: {e}")
            raise

    # Admin groups management
    async def can_manage_group(self, user_id: int, group_id: int) -> bool:
        """检查用户是否可以管理指定群组"""
        await self.ensure_connected()
        try:
            user = await self.get_user(user_id)
            if not user:
                return False

            # 超级管理员可以管理所有群组
            if user['role'] == UserRole.SUPERADMIN.value:
                return True

            # 普通管理员检查授权
            if user['role'] == UserRole.ADMIN.value:
                admin_group = await self.db.admin_groups.find_one({
                    'admin_id': user_id,
                    'group_id': group_id
                })
                return bool(admin_group)

            return False
        except Exception as e:
            logger.error(f"检查群组管理权限失败: {e}")
            return False

    async def get_manageable_groups(self, user_id: int) -> List[Dict[str, Any]]:
        """获取用户可管理的群组列表"""
        await self.ensure_connected()
        try:
            user = await self.get_user(user_id)
            if not user:
                return []

            # 超级管理员可以管理所有群组
            if user['role'] == UserRole.SUPERADMIN.value:
                return await self.find_all_groups()

            # 普通管理员只能管理被授权的群组
            if user['role'] == UserRole.ADMIN.value:
                admin_groups = await self.db.admin_groups.find({
                    'admin_id': user_id
                }).to_list(None)
                group_ids = [g['group_id'] for g in admin_groups]
                return await self.db.groups.find({
                    'group_id': {'$in': group_ids}
                }).to_list(None)

            return []
        except Exception as e:
            logger.error(f"获取可管理群组列表失败: {e}")
            return []

    # Keywords management
    async def add_keyword(self, keyword_data: Dict[str, Any]):
        """添加关键词"""
        await self.ensure_connected()
        try:
            # 验证必要字段
            required_fields = ['group_id', 'pattern', 'type']
            for field in required_fields:
                if field not in keyword_data:
                    raise ValueError(f"缺少必要字段 '{field}'")
                
            # 确保至少有回复文本、媒体或按钮中的一项
            if not (keyword_data.get('response') or keyword_data.get('media') or keyword_data.get('buttons')):
                raise ValueError("关键词回复必须包含文本、媒体或按钮中的至少一项")
            
            await self.db.keywords.update_one(
                {
                    'group_id': keyword_data['group_id'],
                    'pattern': keyword_data['pattern']
                },
                {'$set': keyword_data},
                upsert=True
            )
        except Exception as e:
            logger.error(f"添加关键词失败: {e}")
            raise

    async def remove_keyword(self, group_id: int, keyword_id: str):
        """删除关键词"""
        await self.ensure_connected()
        try:
            # 验证 keyword_id 是否为有效的 ObjectId
            try:
                from bson import ObjectId
                obj_id = ObjectId(keyword_id)
            except Exception as e:
                logger.error(f"无效的关键词ID: {keyword_id}, 错误: {e}")
                raise ValueError(f"无效的关键词ID: {keyword_id}")
        
            # 删除关键词
            await self.db.keywords.delete_one({
                'group_id': group_id,
                '_id': obj_id
            })
        except Exception as e:
            logger.error(f"删除关键词失败: {e}")
            raise

    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        """获取群组的关键词列表"""
        await self.ensure_connected()
        try:
            return await self.db.keywords.find({
                'group_id': group_id
            }).to_list(None)
        except Exception as e:
            logger.error(f"获取关键词列表失败: {e}")
            return []

    # Message stats management
    async def add_message_stat(self, stat_data: Dict[str, Any]):
        """添加消息统计"""
        await self.ensure_connected()
        try:
            await self.db.message_stats.insert_one({
                **stat_data,
                'created_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"添加消息统计失败: {e}")
            raise

    async def get_recent_message_count(self, user_id: int, seconds: int = 60) -> int:
        """获取用户最近的消息数量"""
        await self.ensure_connected()
        try:
            since = datetime.now() - timedelta(seconds=seconds)
            count = await self.db.message_stats.count_documents({
                'user_id': user_id,
                'created_at': {'$gte': since.isoformat()}
            })
            return count
        except Exception as e:
            logger.error(f"获取最近消息数量失败: {e}")
            return 0

    async def add_message_with_transaction(self, message_data: dict):
        """使用事务添加消息"""
        await self.ensure_connected()
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    # 添加消息统计
                    await self.db.message_stats.insert_one(
                        message_data,
                        session=session
                    )
                    
                    # 更新用户统计
                    await self.db.users.update_one(
                        {'user_id': message_data['user_id']},
                        {'$inc': {'total_messages': 1}},
                        session=session
                    )
                except Exception as e:
                    await session.abort_transaction()
                    logger.error(f"消息事务添加失败: {e}")
                    raise

    async def cleanup_old_stats(self, days: int = 30):
        """清理旧的统计数据"""
        await self.ensure_connected()
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            await self.db.message_stats.delete_many({
                'date': {'$lt': cutoff_date}
            })
            logger.info(f"已清理 {days} 天前的统计数据")
        except Exception as e:
            logger.error(f"清理统计数据失败: {e}")
            raise

    async def cleanup_old_data(self):
        """清理所有旧数据"""
        await self.ensure_connected()
        try:
            # 清理过期的统计数据
            cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            await self.db.message_stats.delete_many({
                'date': {'$lt': cutoff_date}
            })
            
            # 清理过期的轮播消息
            now = datetime.now()
            await self.db.broadcasts.delete_many({
                'end_time': {'$lt': now}
            })
            
            logger.info("已完成数据清理")
        except Exception as e:
            logger.error(f"数据清理失败: {e}")
            raise

    # Broadcast management
    async def add_broadcast(self, broadcast_data: Dict[str, Any]):
        """添加轮播消息"""
        await self.ensure_connected()
        try:
            # 确保必要字段存在
            required_fields = ['group_id', 'start_time', 'end_time', 'interval']
            for field in required_fields:
                if field not in broadcast_data:
                    raise ValueError(f"缺少必要字段 '{field}'")
                
            # 确保至少有文本、媒体或按钮之一
            if not (broadcast_data.get('text') or broadcast_data.get('media') or broadcast_data.get('buttons')):
                raise ValueError("轮播消息必须包含文本、媒体或按钮中的至少一项")
            
            await self.db.broadcasts.insert_one(broadcast_data)
        except Exception as e:
            logger.error(f"添加轮播消息失败: {e}")
            raise

    async def remove_broadcast(self, group_id: int, broadcast_id: str):
        """删除轮播消息"""
        await self.ensure_connected()
        try:
            await self.db.broadcasts.delete_one({
                'group_id': group_id,
                '_id': ObjectId(broadcast_id)
            })
        except Exception as e:
            logger.error(f"删除轮播消息失败: {e}")
            raise

    async def get_broadcasts(self, group_id: int) -> List[Dict[str, Any]]:
        """获取群组的轮播消息列表"""
        await self.ensure_connected()
        try:
            return await self.db.broadcasts.find({
                'group_id': group_id
            }).to_list(None)
        except Exception as e:
            logger.error(f"获取轮播消息列表失败: {e}")
            return []

    async def get_active_broadcasts(self) -> List[Dict[str, Any]]:
        """获取所有活动的轮播消息"""
        await self.ensure_connected()
        now = datetime.now()
        try:
            return await self.db.broadcasts.find({
                'start_time': {'$lte': now},
                'end_time': {'$gt': now}
            }).to_list(None)
        except Exception as e:
            logger.error(f"获取活动轮播消息失败: {e}")
            return []

    async def update_broadcast_time(self, broadcast_id: str, last_broadcast: datetime):
        """更新轮播消息的最后发送时间"""
        await self.ensure_connected()
        try:
            await self.db.broadcasts.update_one(
                {'_id': ObjectId(broadcast_id)},
                {'$set': {'last_broadcast': last_broadcast}}
            )
        except Exception as e:
            logger.error(f"更新轮播消息时间失败: {e}")
            raise

    # Stats aggregation methods
    async def get_daily_stats(self, group_id: int, date: str) -> List[Dict[str, Any]]:
        """获取指定日期的统计数据"""
        await self.ensure_connected()
        try:
            pipeline = [
                {'$match': {'group_id': group_id, 'date': date}},
                {'$group': {
                    '_id': '$user_id',
                    'total_messages': {'$sum': '$total_messages'},
                    'total_size': {'$sum': '$total_size'}
                }},
                {'$sort': {'total_messages': -1}}
            ]
            return await self.db.message_stats.aggregate(pipeline).to_list(None)
        except Exception as e:
            logger.error(f"获取日统计数据失败: {e}")
            return []

    async def get_monthly_stats(self, group_id: int, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """获取指定月份的统计数据"""
        await self.ensure_connected()
        try:
            pipeline = [
                {
                    '$match': {
                        'group_id': group_id,
                        'date': {'$gte': start_date, '$lte': end_date}
                    }
                },
                {
                    '$group': {
                        '_id': '$user_id',
                        'total_messages': {'$sum': '$total_messages'},
                        'total_size': {'$sum': '$total_size'}
                    }
                },
                {'$sort': {'total_messages': -1}}
            ]
            return await self.db.message_stats.aggregate(pipeline).to_list(None)
        except Exception as e:
            logger.error(f"获取月统计数据失败: {e}")
            return []
