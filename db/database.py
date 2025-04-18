"""
数据库操作类，提供与MongoDB的交互功能
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
from bson import ObjectId

from db.models import UserRole, GroupPermission

# 配置日志
logger = logging.getLogger(__name__)

class Database:
    """数据库操作类，处理与MongoDB的交互"""
    def __init__(self):
        """初始化数据库连接"""
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
            
            # 只启动重连任务，不启动保活任务
            self._start_reconnect_task()
        
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}", exc_info=True)
            self.connected.clear()
            return False

    def _start_reconnect_task(self):
        """启动重连任务"""
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())
            logger.info("数据库重连任务已启动")
            
    async def _reconnect_loop(self):
        """重连循环"""
        while True:
            if not self.connected.is_set():
                try:
                    logger.info("尝试重新连接数据库...")
                    self.client = AsyncIOMotorClient(self.uri)
                    await self.client.admin.command('ping')
                    self.db = self.client[self.database]
                    logger.info("数据库重连成功")
                    self.connected.set()
                except Exception as e:
                    logger.error(f"数据库重连失败: {e}", exc_info=True)
                    await asyncio.sleep(5)  # 等待5秒后重试
            await asyncio.sleep(60)  # 每分钟检查一次连接状态

    async def _start_keepalive_task(self):
        """启动保活任务"""
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        logger.info("数据库保活任务已启动")
        
    async def _keepalive_loop(self):
        """保活循环"""
        from config import KEEP_ALIVE_INTERVAL
        
        while True:
            if self.connected.is_set():
                try:
                    # 执行一个轻量级操作
                    await self.db.command("ping")
                    # 更新系统状态集合
                    await self.db.system_status.update_one(
                        {"_id": "keepalive"},
                        {"$set": {"last_ping": datetime.now(), "status": "active"}},
                        upsert=True
                    )
                    logger.debug("数据库保活操作成功执行")
                except Exception as e:
                    logger.error(f"数据库保活操作失败: {e}", exc_info=True)
                    self.connected.clear()  # 标记为未连接，触发重连
            
            # 等待下一次保活
            await asyncio.sleep(KEEP_ALIVE_INTERVAL)
            
    async def ensure_connected(self):
        """确保数据库已连接，如果未连接则等待连接"""
        await self.connected.wait()

    async def close(self):
        """关闭数据库连接"""
        if hasattr(self, '_keepalive_task') and self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            
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

            # 系统标志索引
            await self.db.system_flags.create_index([
                ("name", ASCENDING)
            ], unique=True)
            
            logger.info("索引初始化完成")
        except Exception as e:
            logger.error(f"索引初始化失败: {e}", exc_info=True)
            raise

    #######################################
    # 用户相关方法
    #######################################
    
    async def add_user(self, user_data: Dict[str, Any]):
        """
        添加或更新用户
        
        参数:
            user_data: 用户数据，必须包含user_id字段
        """
        await self.ensure_connected()
        try:
            # 验证必要字段
            if 'user_id' not in user_data:
                raise ValueError("用户数据必须包含user_id字段")
                
            # 检查role字段
            if 'role' not in user_data:
                logger.warning(f"用户数据缺少role字段，设置为默认值USER: user_id={user_data['user_id']}")
                from db.models import UserRole
                user_data['role'] = UserRole.USER.value
            else:
                # 验证role值是否有效
                from db.models import UserRole
                valid_roles = [role.value for role in UserRole]
                if user_data['role'] not in valid_roles:
                    logger.warning(f"用户数据包含无效的role值: {user_data['role']}，设置为默认值USER: user_id={user_data['user_id']}")
                    user_data['role'] = UserRole.USER.value
                
            # 更新时间戳
            user_data['updated_at'] = datetime.now()
            
            # 如果是新用户，添加创建时间
            await self.db.users.update_one(
                {'user_id': user_data['user_id']},
                {
                    '$set': user_data,
                    '$setOnInsert': {'created_at': datetime.now()}
                },
                upsert=True
            )
            logger.info(f"已更新/添加用户: user_id={user_data['user_id']}, role={user_data['role']}")
        except Exception as e:
            logger.error(f"添加用户失败: {e}", exc_info=True)
            raise

    async def remove_user(self, user_id: int):
        """
        删除用户
        
        参数:
            user_id: 用户ID
        """
        await self.ensure_connected()
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    # 同时删除用户和对应的管理员群组记录
                    await self.db.users.delete_one(
                        {'user_id': user_id},
                        session=session
                    )
                    await self.db.admin_groups.delete_many(
                        {'admin_id': user_id},
                        session=session
                    )
                    logger.info(f"已删除用户: {user_id}")
                except Exception as e:
                    await session.abort_transaction()
                    logger.error(f"删除用户失败: {e}", exc_info=True)
                    raise

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        获取用户信息
        
        参数:
            user_id: 用户ID
            
        返回:
            用户信息字典或None
        """
        await self.ensure_connected()
        try:
            return await self.db.users.find_one({'user_id': user_id})
        except Exception as e:
            logger.error(f"获取用户失败: {e}", exc_info=True)
            return None

    async def get_users_by_role(self, role: str) -> List[Dict[str, Any]]:
        """
        获取指定角色的所有用户
        
        参数:
            role: 用户角色
            
        返回:
            用户列表
        """
        await self.ensure_connected()
        try:
            return await self.db.users.find({'role': role}).to_list(None)
        except Exception as e:
            logger.error(f"获取用户列表失败: {e}", exc_info=True)
            return []

    async def is_user_banned(self, user_id: int) -> bool:
        """
        检查用户是否被封禁
        
        参数:
            user_id: 用户ID
            
        返回:
            是否被封禁
        """
        await self.ensure_connected()
        try:
            user = await self.db.users.find_one({
                'user_id': user_id,
                'is_banned': True
            })
            return bool(user)
        except Exception as e:
            logger.error(f"检查用户封禁状态失败: {e}", exc_info=True)
            return False

    #######################################
    # 群组相关方法
    #######################################
    
    async def add_group(self, group_data: Dict[str, Any]):
        """
        添加或更新群组
        
        参数:
            group_data: 群组数据，必须包含group_id字段
        """
        await self.ensure_connected()
        try:
            # 验证必要字段
            if 'group_id' not in group_data:
                raise ValueError("群组数据必须包含group_id字段")
                
            # 更新时间戳
            group_data['updated_at'] = datetime.now()
            
            # 如果是新群组，添加创建时间
            await self.db.groups.update_one(
                {'group_id': group_data['group_id']},
                {
                    '$set': group_data,
                    '$setOnInsert': {'created_at': datetime.now()}
                },
                upsert=True
            )
            logger.info(f"已更新/添加群组: {group_data['group_id']}")
        except Exception as e:
            logger.error(f"添加群组失败: {e}", exc_info=True)
            raise

    async def remove_group(self, group_id: int):
        """
        删除群组
        
        参数:
            group_id: 群组ID
        """
        await self.ensure_connected()
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    # 删除群组及相关的所有数据
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
                    logger.info(f"已删除群组: {group_id}")
                except Exception as e:
                    await session.abort_transaction()
                    logger.error(f"删除群组失败: {e}", exc_info=True)
                    raise

    async def cleanup_invalid_groups(self):
        """
        清理所有群组ID不合法的群组
        
        群组ID不合法的情况:
        1. 群组ID为0或null
        2. 群组ID为正数(应该是负数)
        3. 群组ID为默认值-1001234567890
        
        返回:
            删除的群组数量
        """
        await self.ensure_connected()
        try:
            # 查找所有需要删除的群组
            groups_to_delete = await self.db.groups.find({
                "$or": [
                    {"group_id": 0},
                    {"group_id": None},
                    {"group_id": {"$gt": 0}},  # 群组ID应该是负数
                    {"group_id": -1001234567890}  # 默认群组ID
                ]
            }).to_list(None)
            
            deleted_count = 0
            # 删除这些群组
            for group in groups_to_delete:
                group_id = group.get('group_id')
                try:
                    logger.info(f"删除无效群组: ID={group_id}, 数据={group}")
                    await self.remove_group(group_id)
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"删除群组 {group_id} 时出错: {e}", exc_info=True)
            
            return deleted_count
        except Exception as e:
            logger.error(f"清理无效群组失败: {e}", exc_info=True)
            return 0

    async def get_group(self, group_id: int) -> Optional[Dict[str, Any]]:
        """
        获取群组信息
        
        参数:
            group_id: 群组ID
            
        返回:
            群组信息字典或None
        """
        await self.ensure_connected()
        try:
            return await self.db.groups.find_one({'group_id': group_id})
        except Exception as e:
            logger.error(f"获取群组失败: {e}", exc_info=True)
            return None

    async def find_all_groups(self) -> List[Dict[str, Any]]:
        """
        获取所有群组
        
        返回:
            群组列表
        """
        await self.ensure_connected()
        try:
            return await self.db.groups.find().to_list(None)
        except Exception as e:
            logger.error(f"获取群组列表失败: {e}", exc_info=True)
            return []

    async def get_group_settings(self, group_id: int) -> Dict[str, Any]:
        """
        获取群组设置
        
        参数:
            group_id: 群组ID
            
        返回:
            群组设置字典
        """
        await self.ensure_connected()
        try:
            group = await self.get_group(group_id)
            return group.get('settings', {}) if group else {}
        except Exception as e:
            logger.error(f"获取群组设置失败: {e}", exc_info=True)
            return {}

    async def update_group_settings(self, group_id: int, settings: Dict[str, Any]):
        """
        更新群组设置
        
        参数:
            group_id: 群组ID
            settings: 新的设置字典
        """
        await self.ensure_connected()
        try:
            await self.db.groups.update_one(
                {'group_id': group_id},
                {
                    '$set': {
                        'settings': settings,
                        'updated_at': datetime.now()
                    }
                },
                upsert=True
            )
            logger.info(f"已更新群组 {group_id} 的设置")
        except Exception as e:
            logger.error(f"更新群组设置失败: {e}", exc_info=True)
            raise

    async def update_group_settings_field(self, group_id: int, field_updates: Dict[str, Any]):
        """
        更新群组设置中的特定字段
        
        参数:
            group_id: 群组ID
            field_updates: 要更新的字段及其值的字典
        """
        await self.ensure_connected()
        try:
            # 构建更新对象
            updates = {}
            for key, value in field_updates.items():
                updates[f'settings.{key}'] = value
            
            # 更新字段而不是整个设置对象
            await self.db.groups.update_one(
                {'group_id': group_id},
                {
                    '$set': updates,
                    '$currentDate': {'updated_at': True}
                },
                upsert=True
            )
            logger.info(f"已更新群组 {group_id} 的设置字段 {list(field_updates.keys())}")
        except Exception as e:
            logger.error(f"更新群组设置字段失败: {e}", exc_info=True)
            raise

    #######################################
    # 管理员群组关系方法
    #######################################
    
    async def can_manage_group(self, user_id: int, group_id: int) -> bool:
        """
        检查用户是否可以管理指定群组
        
        参数:
            user_id: 用户ID
            group_id: 群组ID
            
        返回:
            是否可以管理
        """
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
            logger.error(f"检查群组管理权限失败: {e}", exc_info=True)
            return False

    async def get_manageable_groups(self, user_id: int) -> List[Dict[str, Any]]:
        """
        获取用户可管理的群组列表
        
        参数:
            user_id: 用户ID
            
        返回:
            可管理的群组列表
        """
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
            logger.error(f"获取可管理群组列表失败: {e}", exc_info=True)
            return []

    async def add_admin_group(self, admin_id: int, group_id: int):
        """
        添加管理员与群组的关联
        
        参数:
            admin_id: 管理员ID
            group_id: 群组ID
        """
        await self.ensure_connected()
        try:
            await self.db.admin_groups.update_one(
                {
                    'admin_id': admin_id,
                    'group_id': group_id
                },
                {
                    '$set': {
                        'admin_id': admin_id,
                        'group_id': group_id,
                        'updated_at': datetime.now()
                    },
                    '$setOnInsert': {'created_at': datetime.now()}
                },
                upsert=True
            )
            logger.info(f"已添加管理员群组关联: 管理员={admin_id}, 群组={group_id}")
        except Exception as e:
            logger.error(f"添加管理员群组关联失败: {e}", exc_info=True)
            raise

    async def remove_admin_group(self, admin_id: int, group_id: int):
        """
        移除管理员与群组的关联
        
        参数:
            admin_id: 管理员ID
            group_id: 群组ID
        """
        await self.ensure_connected()
        try:
            await self.db.admin_groups.delete_one({
                'admin_id': admin_id,
                'group_id': group_id
            })
            logger.info(f"已移除管理员群组关联: 管理员={admin_id}, 群组={group_id}")
        except Exception as e:
            logger.error(f"移除管理员群组关联失败: {e}", exc_info=True)
            raise

    #######################################
    # 关键词管理方法
    #######################################
    
    async def add_keyword(self, keyword_data: Dict[str, Any]):
        """
        添加关键词
        
        参数:
            keyword_data: 关键词数据
        """
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
            
            # 添加时间戳
            keyword_data['updated_at'] = datetime.now()
            
            result = await self.db.keywords.update_one(
                {
                    'group_id': keyword_data['group_id'],
                    'pattern': keyword_data['pattern']
                },
                {
                    '$set': keyword_data,
                    '$setOnInsert': {'created_at': datetime.now()}
                },
                upsert=True
            )
            logger.info(f"已添加关键词: {keyword_data['pattern']}")
            return result
        except Exception as e:
            logger.error(f"添加关键词失败: {e}", exc_info=True)
            raise

    async def remove_keyword(self, group_id: int, keyword_id: str):
        """
        删除关键词
        
        参数:
            group_id: 群组ID
            keyword_id: 关键词ID
        """
        await self.ensure_connected()
        try:
            # 验证 keyword_id 是否为有效的 ObjectId
            try:
                obj_id = ObjectId(keyword_id)
            except Exception as e:
                logger.error(f"无效的关键词ID: {keyword_id}, 错误: {e}")
                raise ValueError(f"无效的关键词ID: {keyword_id}")
        
            # 删除关键词
            result = await self.db.keywords.delete_one({
                'group_id': group_id,
                '_id': obj_id
            })
            
            if result.deleted_count == 0:
                logger.warning(f"未找到要删除的关键词: group_id={group_id}, keyword_id={keyword_id}")
            else:
                logger.info(f"已删除关键词: {keyword_id}")
        except Exception as e:
            logger.error(f"删除关键词失败: {e}", exc_info=True)
            raise

    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        """
        获取群组的关键词列表
        
        参数:
            group_id: 群组ID
            
        返回:
            关键词列表
        """
        await self.ensure_connected()
        try:
            return await self.db.keywords.find({
                'group_id': group_id
            }).to_list(None)
        except Exception as e:
            logger.error(f"获取关键词列表失败: {e}", exc_info=True)
            return []

    async def get_keyword_by_id(self, group_id: int, keyword_id: str) -> Optional[Dict[str, Any]]:
        """
        通过ID获取关键词
        
        参数:
            group_id: 群组ID
            keyword_id: 关键词ID或模式
                
        返回:
            关键词数据或None
        """
        await self.ensure_connected()
        try:
            logger.info(f"尝试获取关键词 - group_id: {group_id}, keyword_id: {keyword_id}")
            
            # 首先尝试作为ObjectId查询
            try:
                obj_id = ObjectId(keyword_id)
                logger.info(f"已转换为ObjectId: {obj_id}")
                
                # 尝试方法1：使用提供的群组ID和对象ID查询
                result = await self.db.keywords.find_one({
                    'group_id': group_id,
                    '_id': obj_id
                })
                
                if result:
                    logger.info(f"使用group_id={group_id}和ObjectId查找成功")
                    return result
                    
                # 尝试方法2：仅使用对象ID查询
                logger.warning(f"使用group_id={group_id}查找失败，尝试仅用ID查询")
                alt_result = await self.db.keywords.find_one({'_id': obj_id})
                
                if alt_result:
                    actual_group_id = alt_result.get('group_id')
                    logger.warning(f"找到关键词，但群组ID不匹配: 预期={group_id}, 实际={actual_group_id}")
                    return alt_result
                
            except Exception as e:
                logger.info(f"'{keyword_id}'不是有效的ObjectId，尝试作为关键词模式查找")
                
            # 尝试作为关键词模式查询
            pattern_result = await self.db.keywords.find_one({
                'group_id': group_id,
                'pattern': keyword_id
            })
            
            if pattern_result:
                logger.info(f"使用pattern='{keyword_id}'查找成功")
                return pattern_result
            
            logger.warning(f"关键词ID或模式 '{keyword_id}' 在数据库中不存在")
            return None
                    
        except Exception as e:
            logger.error(f"获取关键词失败: {e}", exc_info=True)
            return None

    #######################################
    # 消息统计方法
    #######################################
    
    async def add_message_stat(self, stat_data: Dict[str, Any]):
        """
        添加消息统计
        
        参数:
            stat_data: 统计数据
        """
        await self.ensure_connected()
        try:
            # 确保包含必要字段
            required_fields = ['group_id', 'user_id', 'date']
            for field in required_fields:
                if field not in stat_data:
                    raise ValueError(f"缺少必要字段 '{field}'")
                    
            await self.db.message_stats.insert_one({
                **stat_data,
                'created_at': datetime.now()
            })
        except Exception as e:
            logger.error(f"添加消息统计失败: {e}", exc_info=True)
            raise

    async def get_recent_message_count(self, user_id: int, seconds: int = 60) -> int:
        """
        获取用户最近的消息数量
        
        参数:
            user_id: 用户ID
            seconds: 时间范围（秒）
            
        返回:
            消息数量
        """
        await self.ensure_connected()
        try:
            since = datetime.now() - timedelta(seconds=seconds)
            count = await self.db.message_stats.count_documents({
                'user_id': user_id,
                'created_at': {'$gte': since}
            })
            return count
        except Exception as e:
            logger.error(f"获取最近消息数量失败: {e}", exc_info=True)
            return 0

    async def add_message_with_transaction(self, message_data: dict):
        """
        使用事务添加消息
        
        参数:
            message_data: 消息数据
        """
        await self.ensure_connected()
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    # 添加消息统计
                    await self.db.message_stats.insert_one(
                        {
                            **message_data,
                            'created_at': datetime.now()
                        },
                        session=session
                    )
                    
                    # 更新用户统计
                    await self.db.users.update_one(
                        {'user_id': message_data['user_id']},
                        {'$inc': {'total_messages': 1}},
                        session=session
                    )
                    
                    logger.info(f"已添加消息统计: user_id={message_data['user_id']}")
                except Exception as e:
                    await session.abort_transaction()
                    logger.error(f"消息事务添加失败: {e}", exc_info=True)
                    raise

    async def cleanup_old_stats(self, days: int = 30):
        """
        清理旧的统计数据
        
        参数:
            days: 保留天数
        """
        await self.ensure_connected()
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            result = await self.db.message_stats.delete_many({
                'date': {'$lt': cutoff_date}
            })
            logger.info(f"已清理 {days} 天前的统计数据，共 {result.deleted_count} 条")
        except Exception as e:
            logger.error(f"清理统计数据失败: {e}", exc_info=True)
            raise

    async def cleanup_old_data(self):
        """清理所有旧数据"""
        await self.ensure_connected()
        try:
            # 清理过期的统计数据
            cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            stats_result = await self.db.message_stats.delete_many({
                'date': {'$lt': cutoff_date}
            })
            
            # 清理过期的轮播消息
            now = datetime.now()
            broadcast_result = await self.db.broadcasts.delete_many({
                'end_time': {'$lt': now}
            })
            
            logger.info(f"数据清理完成: 删除了 {stats_result.deleted_count} 条统计数据和 {broadcast_result.deleted_count} 条过期轮播消息")
        except Exception as e:
            logger.error(f"数据清理失败: {e}", exc_info=True)
            raise

    #######################################
    # 统计聚合方法
    #######################################
    
    async def get_daily_stats(self, group_id: int, date: str) -> List[Dict[str, Any]]:
        """
        获取指定日期的统计数据
        
        参数:
            group_id: 群组ID
            date: 日期字符串 (YYYY-MM-DD)
            
        返回:
            统计数据列表
        """
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
            logger.error(f"获取日统计数据失败: {e}", exc_info=True)
            return []

    async def get_monthly_stats(self, group_id: int, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        获取指定月份的统计数据
        
        参数:
            group_id: 群组ID
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        返回:
            统计数据列表
        """
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
            logger.error(f"获取月统计数据失败: {e}", exc_info=True)
            return []

    #######################################
    # 轮播消息方法
    #######################################
    
    async def add_broadcast(self, broadcast_data: Dict[str, Any]):
        """
        添加轮播消息
        
        参数:
            broadcast_data: 轮播消息数据
        """
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
            
            # 添加时间戳
            broadcast_data['updated_at'] = datetime.now()
            
            # 提取和保存固定的时间部分
            if broadcast_data.get('start_time') and isinstance(broadcast_data['start_time'], datetime):
                # 保存小时:分钟格式的调度时间
                schedule_time = f"{broadcast_data['start_time'].hour}:{broadcast_data['start_time'].minute:02d}"
                broadcast_data['schedule_time'] = schedule_time
                logger.info(f"设置轮播消息的固定调度时间为: {schedule_time}")
                
            result = await self.db.broadcasts.insert_one({
                **broadcast_data,
                'created_at': datetime.now()
            })
            logger.info(f"已添加轮播消息: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"添加轮播消息失败: {e}", exc_info=True)
            raise

    async def remove_broadcast(self, group_id: int, broadcast_id: str):
        """
        删除轮播消息
        
        参数:
            group_id: 群组ID
            broadcast_id: 轮播消息ID
        """
        await self.ensure_connected()
        try:
            # 验证 broadcast_id 是否为有效的 ObjectId
            try:
                obj_id = ObjectId(broadcast_id)
            except Exception as e:
                logger.error(f"无效的轮播消息ID: {broadcast_id}, 错误: {e}")
                raise ValueError(f"无效的轮播消息ID: {broadcast_id}")
                
            result = await self.db.broadcasts.delete_one({
                'group_id': group_id,
                '_id': obj_id
            })
            
            if result.deleted_count == 0:
                logger.warning(f"未找到要删除的轮播消息: group_id={group_id}, broadcast_id={broadcast_id}")
            else:
                logger.info(f"已删除轮播消息: {broadcast_id}")
        except Exception as e:
            logger.error(f"删除轮播消息失败: {e}", exc_info=True)
            raise

    async def delete_broadcast(self, broadcast_id: str) -> bool:
        """
        仅通过ID删除轮播消息
        
        参数:
            broadcast_id: 轮播消息ID
            
        返回:
            bool: 是否成功删除
        """
        await self.ensure_connected()
        try:
            # 验证 broadcast_id 是否为有效的 ObjectId
            try:
                obj_id = ObjectId(broadcast_id)
            except Exception as e:
                logger.error(f"无效的轮播消息ID: {broadcast_id}, 错误: {e}")
                return False
                
            # 删除轮播消息
            result = await self.db.broadcasts.delete_one({'_id': obj_id})
            
            if result.deleted_count == 0:
                logger.warning(f"未找到要删除的轮播消息: broadcast_id={broadcast_id}")
                return False
            else:
                logger.info(f"已删除轮播消息: {broadcast_id}")
                return True
        except Exception as e:
            logger.error(f"删除轮播消息失败: {e}", exc_info=True)
            return False

    async def get_broadcasts(self, group_id: int) -> List[Dict[str, Any]]:
        """
        获取群组的轮播消息列表
        
        参数:
            group_id: 群组ID
            
        返回:
            轮播消息列表
        """
        await self.ensure_connected()
        try:
            return await self.db.broadcasts.find({
                'group_id': group_id
            }).to_list(None)
        except Exception as e:
            logger.error(f"获取轮播消息列表失败: {e}", exc_info=True)
            return []

    async def get_active_broadcasts(self) -> List[Dict[str, Any]]:
        """
        获取所有活动的轮播消息
        
        返回:
            活动轮播消息列表
        """
        await self.ensure_connected()
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            return await self.db.broadcasts.find({
                '$or': [
                    # 处理datetime对象
                    {'start_time': {'$lte': now}, 'end_time': {'$gt': now}},
                    # 处理字符串格式
                    {'start_time': {'$lte': now_str}, 'end_time': {'$gt': now_str}}
                ]
            }).to_list(None)
        except Exception as e:
            logger.error(f"获取活动轮播消息失败: {e}", exc_info=True)
            return []

    async def get_due_broadcasts(self) -> List[Dict[str, Any]]:
        """获取所有应该发送的轮播消息"""
        await self.ensure_connected()
        now = datetime.now()
        logger.info(f"查询应该发送的轮播消息，当前时间: {now}")
        
        try:
            # 1. 首先确保所有轮播消息的时间字段都是datetime类型
            normalized_count = await self.normalize_broadcast_datetimes()
            logger.info(f"标准化了 {normalized_count} 条轮播消息的时间字段")
            
            # 2. 查询未发送过的轮播消息
            not_sent_query = {
                'start_time': {'$lte': now},
                'end_time': {'$gt': now},
                '$or': [
                    {'last_broadcast': {'$exists': False}},
                    {'last_broadcast': None}
                ]
            }
            
            not_sent_broadcasts = await self.db.broadcasts.find(not_sent_query).to_list(None)
            logger.info(f"找到 {len(not_sent_broadcasts)} 个未发送过的轮播消息")
            
            # 打印详细日志
            for bc in not_sent_broadcasts:
                logger.info(f"未发送轮播: ID={bc['_id']}, 群组={bc.get('group_id')}, "
                           f"开始时间={bc.get('start_time')}, 结束时间={bc.get('end_time')}")
            
            # 3. 查询已发送但应再次发送的轮播消息
            interval_query = {
                'start_time': {'$lte': now},
                'end_time': {'$gt': now},
                'last_broadcast': {'$exists': True, '$ne': None},
                'interval': {'$gt': 0},
                'repeat_type': {'$ne': 'once'}  # 排除单次发送的消息
            }
            
            interval_broadcasts = await self.db.broadcasts.find(interval_query).to_list(None)
            logger.info(f"找到 {len(interval_broadcasts)} 个可能需要再次发送的轮播消息")
            
            # 不检查间隔时间，直接全部添加到待发送列表，让轮播管理器基于锚点时间判断
            due_interval_broadcasts = []
            for bc in interval_broadcasts:
                bc_id = str(bc['_id'])
                last_broadcast = bc.get('last_broadcast')
                interval_minutes = bc.get('interval', 0)
                
                # 确保last_broadcast是datetime对象（保留这部分，确保数据类型正确）
                if not isinstance(last_broadcast, datetime):
                    logger.warning(f"轮播 {bc_id} 的last_broadcast不是datetime类型: {type(last_broadcast)}")
                    if isinstance(last_broadcast, str):
                        try:
                            last_broadcast = datetime.strptime(last_broadcast, '%Y-%m-%d %H:%M:%S')
                            # 更新数据库中的值
                            await self.db.broadcasts.update_one(
                                {'_id': bc['_id']},
                                {'$set': {'last_broadcast': last_broadcast}}
                            )
                            logger.info(f"已将轮播 {bc_id} 的last_broadcast从字符串转换为datetime")
                        except ValueError:
                            logger.error(f"无法解析轮播 {bc_id} 的last_broadcast: {last_broadcast}")
                            continue
                    else:
                        continue
                
                # 确保schedule_time字段存在且格式正确
                if not bc.get('schedule_time'):
                    logger.warning(f"轮播 {bc_id} 缺少schedule_time字段，设置为默认值")
                    # 使用start_time或last_broadcast时间作为基准设置schedule_time
                    reference_time = bc.get('start_time') or last_broadcast
                    schedule_time = f"{reference_time.hour}:{reference_time.minute:02d}"
                    await self.db.broadcasts.update_one(
                        {'_id': bc['_id']},
                        {'$set': {'schedule_time': schedule_time}}
                    )
                    bc['schedule_time'] = schedule_time
                    logger.info(f"为轮播 {bc_id} 设置默认schedule_time: {schedule_time}")
                
                # 移除间隔检查，直接添加所有轮播消息
                # 添加记录时间差的日志，但不用它来过滤消息
                time_diff = (now - last_broadcast).total_seconds() / 60
                logger.info(f"轮播 {bc_id}: 上次发送: {last_broadcast}, 距现在: {time_diff:.2f}分钟, 配置间隔: {interval_minutes}分钟")
                logger.info(f"轮播 {bc_id} 添加到待发送列表（锚点模式，不检查时间间隔）")
                due_interval_broadcasts.append(bc)
            
            # 4. 合并并返回所有需要发送的轮播消息
            due_broadcasts = not_sent_broadcasts + due_interval_broadcasts
            logger.info(f"总共有 {len(due_broadcasts)} 个轮播消息需要发送（使用锚点模式，最终发送由 _should_send_broadcast 决定）")
            
            return due_broadcasts
        
        except Exception as e:
            logger.error(f"获取应发送轮播消息失败: {e}", exc_info=True)
            return []
        
    async def update_broadcast(self, broadcast_id: str, update_data: Dict[str, Any]):
        """
        更新轮播消息
        
        参数:
            broadcast_id: 轮播消息ID
            update_data: 要更新的数据
        """
        await self.ensure_connected()
        try:
            obj_id = ObjectId(broadcast_id)
            update_data['updated_at'] = datetime.now()
            
            # 确保时间字段是datetime对象
            for field in ['start_time', 'end_time', 'last_broadcast']:
                if field in update_data and isinstance(update_data[field], str):
                    try:
                        update_data[field] = datetime.strptime(update_data[field], '%Y-%m-%d %H:%M:%S')
                        logger.info(f"将更新数据中的 {field} 从字符串转换为datetime")
                    except ValueError:
                        logger.warning(f"无法解析更新数据中的 {field}: {update_data[field]}")
            
            result = await self.db.broadcasts.update_one(
                {'_id': obj_id},
                {'$set': update_data}
            )
            
            if result.modified_count == 0:
                logger.warning(f"未能更新轮播消息: {broadcast_id}")
            else:
                logger.info(f"已更新轮播消息: {broadcast_id}")
                
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"更新轮播消息失败: {e}", exc_info=True)
            raise

    async def update_broadcast_time(self, broadcast_id: str, last_broadcast: datetime):
        """
        更新轮播消息的最后发送时间
        
        参数:
            broadcast_id: 轮播消息ID
            last_broadcast: 最后发送时间
        """
        await self.ensure_connected()
        try:
            obj_id = ObjectId(broadcast_id)
            result = await self.db.broadcasts.update_one(
                {'_id': obj_id},
                {'$set': {'last_broadcast': last_broadcast}}
            )
            
            if result.modified_count == 0:
                logger.warning(f"未能更新轮播消息的最后发送时间: {broadcast_id}")
            else:
                logger.info(f"已更新轮播消息的最后发送时间: {broadcast_id}")
                
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"更新轮播消息最后发送时间失败: {e}", exc_info=True)
            return False
    async def get_broadcast_by_id(self, broadcast_id: str) -> Optional[Dict[str, Any]]:
        """
        通过ID获取轮播消息
        
        参数:
            broadcast_id: 轮播消息ID
            
        返回:
            轮播消息数据或None
        """
        await self.ensure_connected()
        try:
            obj_id = ObjectId(broadcast_id)
            return await self.db.broadcasts.find_one({'_id': obj_id})
        except Exception as e:
            logger.error(f"获取轮播消息失败: {e}", exc_info=True)
            return None

    async def inspect_broadcast(self, broadcast_id: str):
        """
        检查特定轮播消息的详情，用于调试
        
        参数:
            broadcast_id: 轮播消息ID
        """
        await self.ensure_connected()
        try:
            obj_id = ObjectId(broadcast_id)
            broadcast = await self.db.broadcasts.find_one({'_id': obj_id})
            
            if not broadcast:
                logger.error(f"找不到轮播消息: {broadcast_id}")
                return False
                
            # 打印详细信息
            logger.info(f"========== 轮播消息检查 ==========")
            logger.info(f"ID: {broadcast_id}")
            logger.info(f"群组ID: {broadcast.get('group_id')}")
            
            # 检查时间字段
            now = datetime.now()
            for field in ['start_time', 'end_time', 'last_broadcast']:
                value = broadcast.get(field)
                if value:
                    logger.info(f"{field}: {value} (类型: {type(value).__name__})")
                    
                    if isinstance(value, datetime):
                        if field == 'start_time':
                            logger.info(f"  start_time {'≤' if value <= now else '>'} 当前时间")
                        elif field == 'end_time':
                            logger.info(f"  end_time {'>' if value > now else '≤'} 当前时间")
                        elif field == 'last_broadcast' and broadcast.get('interval'):
                            time_diff = (now - value).total_seconds() / 60
                            interval = broadcast.get('interval')
                            logger.info(f"  距上次发送: {time_diff:.2f}分钟, 配置间隔: {interval}分钟")
                            logger.info(f"  是否应该发送: {'是' if time_diff >= interval else '否'}")
                else:
                    logger.info(f"{field}: 未设置")
                    
            # 检查其他关键字段
            logger.info(f"interval: {broadcast.get('interval')}")
            logger.info(f"repeat_type: {broadcast.get('repeat_type')}")
            logger.info(f"===================================")
            
            return True
        except Exception as e:
            logger.error(f"检查轮播消息失败: {e}", exc_info=True)
            return False

    async def migrate_broadcast_datetime_fields(self):
        """将轮播消息中的字符串时间字段转换为datetime对象"""
        await self.ensure_connected()
        try:
            broadcasts = await self.db.broadcasts.find().to_list(None)
            updated_count = 0
            
            for bc in broadcasts:
                updates = {}
                # 检查并转换start_time
                if 'start_time' in bc and isinstance(bc['start_time'], str):
                    try:
                        updates['start_time'] = datetime.strptime(bc['start_time'], '%Y-%m-%d %H:%M:%S')
                        logger.info(f"将轮播 {bc['_id']} 的start_time从字符串转换为datetime")
                    except ValueError:
                        logger.warning(f"无法解析start_time: {bc['start_time']} for broadcast {bc['_id']}")
                        
                # 检查并转换end_time
                if 'end_time' in bc and isinstance(bc['end_time'], str):
                    try:
                        updates['end_time'] = datetime.strptime(bc['end_time'], '%Y-%m-%d %H:%M:%S')
                        logger.info(f"将轮播 {bc['_id']} 的end_time从字符串转换为datetime")
                    except ValueError:
                        logger.warning(f"无法解析end_time: {bc['end_time']} for broadcast {bc['_id']}")
                
                # 如果有需要更新的字段
                if updates:
                    result = await self.db.broadcasts.update_one(
                        {'_id': bc['_id']},
                        {'$set': updates}
                    )
                    if result.modified_count > 0:
                        updated_count += 1
            
            logger.info(f"时间字段迁移完成，共更新了 {updated_count} 条轮播消息")
            return updated_count
        except Exception as e:
            logger.error(f"时间字段迁移失败: {e}", exc_info=True)
            return 0

    async def normalize_broadcast_datetimes(self):
        """将所有轮播消息的时间字段标准化为datetime对象"""
        await self.ensure_connected()
        logger.info("开始标准化所有轮播消息时间字段")
        try:
            broadcasts = await self.db.broadcasts.find().to_list(None)
            normalized_count = 0
            error_count = 0
            
            for bc in broadcasts:
                updates = {}
                fields_to_check = ['start_time', 'end_time', 'last_broadcast']
                
                for field in fields_to_check:
                    if field in bc and bc[field] is not None:
                        if isinstance(bc[field], str):
                            try:
                                updates[field] = datetime.strptime(bc[field], '%Y-%m-%d %H:%M:%S')
                                logger.info(f"将轮播 {bc['_id']} 的 {field} 从字符串转换为datetime: {bc[field]} -> {updates[field]}")
                            except ValueError as e:
                                logger.error(f"无法解析 {field}: {bc[field]} for broadcast {bc['_id']}: {e}")
                                error_count += 1
                
                if updates:
                    try:
                        result = await self.db.broadcasts.update_one(
                            {'_id': bc['_id']},
                            {'$set': updates}
                        )
                        if result.modified_count > 0:
                            normalized_count += 1
                    except Exception as e:
                        logger.error(f"更新轮播 {bc['_id']} 时出错: {e}")
                        error_count += 1
            
            logger.info(f"时间字段标准化完成: 更新了 {normalized_count} 条轮播消息, 失败 {error_count} 条")
            return normalized_count
        except Exception as e:
            logger.error(f"时间字段标准化失败: {e}", exc_info=True)
            return 0

    async def get_system_flag(self, flag_name: str) -> Any:
        """
        获取系统标志的值
        
        参数:
            flag_name: 标志名称
            
        返回:
            标志值，不存在时返回None
        """
        await self.ensure_connected()
        try:
            result = await self.db.system_flags.find_one({'name': flag_name})
            return result['value'] if result else None
        except Exception as e:
            logger.error(f"获取系统标志失败: {e}", exc_info=True)
            return None
    
    async def set_system_flag(self, flag_name: str, value: Any) -> bool:
        """
        设置系统标志的值
        
        参数:
            flag_name: 标志名称
            value: 标志值
            
        返回:
            是否成功
        """
        await self.ensure_connected()
        try:
            await self.db.system_flags.update_one(
                {'name': flag_name},
                {'$set': {'value': value, 'updated_at': datetime.now()}},
                upsert=True
            )
            logger.info(f"设置系统标志 {flag_name}={value}")
            return True
        except Exception as e:
            logger.error(f"设置系统标志失败: {e}", exc_info=True)
            return False
