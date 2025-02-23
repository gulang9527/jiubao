from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
from bson import ObjectId

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

    async def connect(self, mongodb_uri: str, database: str):
        """连接到MongoDB"""
        try:
            self.client = AsyncIOMotorClient(mongodb_uri)
            # 验证连接
            await self.client.admin.command('ping')
            self.db = self.client[database]
            logger.info("数据库连接成功")
            
            # 验证集合是否存在
            collections = await self.db.list_collection_names()
            required_collections = ['users', 'groups', 'keywords', 'broadcasts', 'message_stats']
            
            for collection in required_collections:
                if collection not in collections:
                    logger.warning(f"创建集合: {collection}")
                    await self.db.create_collection(collection)
            
            # 初始化索引
            await self.init_indexes()
            
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.client:
            self.client.close()

    async def init_indexes(self):
        """初始化所有集合的索引"""
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

    # User related methods
    async def add_user(self, user_data: Dict[str, Any]):
        """添加或更新用户"""
        await self.db.users.update_one(
            {'user_id': user_data['user_id']},
            {'$set': user_data},
            upsert=True
        )

    async def remove_user(self, user_id: int):
        """删除用户"""
        await self.db.users.delete_one({'user_id': user_id})
        # 同时删除相关的群组管理权限
        await self.db.admin_groups.delete_many({'admin_id': user_id})

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """获取用户信息"""
        return await self.db.users.find_one({'user_id': user_id})

    async def get_users_by_role(self, role: str) -> List[Dict[str, Any]]:
        """获取指定角色的所有用户"""
        return await self.db.users.find({'role': role}).to_list(None)

    # Group related methods
    async def add_group(self, group_data: Dict[str, Any]):
        """添加或更新群组"""
        await self.db.groups.update_one(
            {'group_id': group_data['group_id']},
            {'$set': group_data},
            upsert=True
        )

    async def remove_group(self, group_id: int):
        """删除群组"""
        await self.db.groups.delete_one({'group_id': group_id})
        # 同时删除相关的管理权限和设置
        await self.db.admin_groups.delete_many({'group_id': group_id})
        await self.db.keywords.delete_many({'group_id': group_id})
        await self.db.broadcasts.delete_many({'group_id': group_id})

    async def get_group(self, group_id: int) -> Optional[Dict[str, Any]]:
        """获取群组信息"""
        return await self.db.groups.find_one({'group_id': group_id})

    async def find_all_groups(self) -> List[Dict[str, Any]]:
        """获取所有群组"""
        return await self.db.groups.find().to_list(None)

    async def get_group_settings(self, group_id: int) -> Dict[str, Any]:
        """获取群组设置"""
        group = await self.get_group(group_id)
        return group.get('settings', {}) if group else {}

    async def update_group_settings(self, group_id: int, settings: Dict[str, Any]):
        """更新群组设置"""
        await self.db.groups.update_one(
            {'group_id': group_id},
            {'$set': {'settings': settings}},
            upsert=True
        )

    # Admin groups management
    async def can_manage_group(self, user_id: int, group_id: int) -> bool:
        """检查用户是否可以管理指定群组"""
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

    async def get_manageable_groups(self, user_id: int) -> List[Dict[str, Any]]:
        """获取用户可管理的群组列表"""
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

    # Keywords management
    async def add_keyword(self, keyword_data: Dict[str, Any]):
        """添加关键词"""
        await self.db.keywords.update_one(
            {
                'group_id': keyword_data['group_id'],
                'pattern': keyword_data['pattern']
            },
            {'$set': keyword_data},
            upsert=True
        )

    async def remove_keyword(self, group_id: int, keyword_id: str):
        """删除关键词"""
        await self.db.keywords.delete_one({
            'group_id': group_id,
            '_id': ObjectId(keyword_id)
        })

    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        """获取群组的关键词列表"""
        return await self.db.keywords.find({
            'group_id': group_id
        }).to_list(None)

    # Message stats management
    async def add_message_stat(self, stat_data: Dict[str, Any]):
        """添加消息统计"""
        await self.db.message_stats.insert_one({
            **stat_data,
            'created_at': datetime.now().isoformat()
        })

    async def cleanup_old_stats(self, days: int = 30):
        """清理旧的统计数据"""
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        await self.db.message_stats.delete_many({
            'date': {'$lt': cutoff_date}
        })

# Export classes
__all__ = ['Database', 'UserRole', 'GroupPermission']
