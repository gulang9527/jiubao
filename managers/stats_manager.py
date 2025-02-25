from typing import Dict, Any, Optional
from datetime import datetime
from db import Database

class StatsManager:
    """统计管理类"""
    def __init__(self, db):
        self.db = db

    async def record_message(self, user_id: int, group_id: int, message_text: str):
        """记录消息"""
        collection = self.db['messages']
        await collection.insert_one({
            'user_id': user_id,
            'group_id': group_id,
            'message_text': message_text,
            'timestamp': datetime.now()
        })

    async def get_stats(self, group_id: int, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """获取统计信息"""
        collection = self.db['messages']
        pipeline = [
            {'$match': {
                'group_id': group_id,
                'timestamp': {'$gte': start_date, '$lte': end_date}
            }},
            {'$group': {
                '_id': None,
                'total_messages': {'$sum': 1},
                'unique_users': {'$sum': {'$cond': [{'$addToSet': '$user_id'}, 1, 0]}}
            }}
        ]
                result = await collection.aggregate(pipeline).to_list(length=None)
        return result[0] if result else {}
