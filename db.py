import motor.motor_asyncio
from typing import Dict, Any, List, Optional
from config import Config

class Database:
    """数据库管理类"""
    def __init__(self):
        self.config = Config()
        self.client = None
        self.db = None

    async def connect(self) -> bool:
        """连接到MongoDB"""
        try:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(self.config.get_mongodb_uri())
            self.db = self.client[self.config.get_mongodb_db_name()]
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            print(f"连接到MongoDB失败: {e}")
            return False

    async def close(self):
        """关闭MongoDB连接"""
        if self.client:
            self.client.close()

    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        """获取群组的关键词列表"""
        collection = self.db['keywords']
        return await collection.find({'group_id': group_id}).to_list(length=None)

    async def get_group(self, group_id: int) -> Optional[Dict[str, Any]]:
        """获取群组信息"""
        collection = self.db['groups']
        return await collection.find_one({'group_id': group_id})

    async def is_user_banned(self, user_id: int) -> bool:
        """检查用户是否被封禁"""
        collection = self.db['banned_users']
        return await collection.find_one({'user_id': user_id}) is not None

    async def insert_keyword(self, keyword: Dict[str, Any]) -> str:
        """插入关键词"""
        collection = self.db['keywords']
        result = await collection.insert_one(keyword)
        return str(result.inserted_id)

    async def update_keyword(self, keyword_id: str, update: Dict[str, Any]) -> int:
        """更新关键词"""
        collection = self.db['keywords']
        result = await collection.update_one({'_id': motor.motor_asyncio.AsyncIOMotorObjectId(keyword_id)}, {'$set': update})
        return result.modified_count

    async def delete_keyword(self, keyword_id: str) -> int:
        """删除关键词"""
        collection = self.db['keywords']
        result = await collection.delete_one({'_id': motor.motor_asyncio.AsyncIOMotorObjectId(keyword_id)})
        return result.deleted_count