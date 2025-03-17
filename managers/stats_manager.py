"""
统计管理器，处理消息统计
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple

from telegram import Message

logger = logging.getLogger(__name__)

class StatsManager:
    """
    统计管理器，处理消息统计相关功能
    """
    def __init__(self, db):
        """
        初始化统计管理器
        
        参数:
            db: 数据库实例
        """
        self.db = db
        
    async def add_message_stat(self, group_id: int, user_id: int, message: Message):
        try:
            # 获取消息元数据
            date = datetime.now().strftime('%Y-%m-%d')
            message_size = len(message.text or '') if message.text else 0
            media_type = None
            
            logger.info(f"开始处理消息统计 - 群组: {group_id}, 用户: {user_id}, 初始消息大小: {message_size}")
            
            # 检查是否是媒体消息
            if message.photo:
                media_type = 'photo'
                if message.photo:
                    message_size += message.photo[-1].file_size
                    logger.info(f"处理照片消息，文件大小: {message.photo[-1].file_size}")
            elif message.video:
                media_type = 'video'
                message_size += message.video.file_size
                logger.info(f"处理视频消息，文件大小: {message.video.file_size}")
            elif message.document:
                media_type = 'document'
                message_size += message.document.file_size
                logger.info(f"处理文档消息，文件大小: {message.document.file_size}")
            # ... 其他媒体类型检查 ...
            
            logger.info(f"消息处理结果 - 类型: {media_type or '文本'}, 最终大小: {message_size} 字节")
            
            # 获取群组设置，检查是否需要忽略某些消息
            group_settings = await self.db.get_group_settings(group_id)
            min_bytes = group_settings.get('min_bytes', 0)
            count_media = group_settings.get('count_media', True)
            
            logger.info(f"群组设置 - 最小字节: {min_bytes}, 统计媒体: {count_media}")
            
            # 判断是否满足统计条件
            if (message_size < min_bytes) or (media_type and not count_media):
                logger.warning(f"消息不满足统计条件: size={message_size}, min_bytes={min_bytes}, media_type={media_type}, count_media={count_media}")
                return
            
            # 准备统计数据
            stat_data = {
                'group_id': group_id,
                'user_id': user_id,
                'date': date,
                'total_messages': 1,
                'total_size': message_size,
                'media_type': media_type
            }
            
            logger.info(f"即将添加统计数据: {stat_data}")
            
            # 添加到数据库
            await self.db.add_message_stat(stat_data)
            logger.info(f"成功添加消息统计: group_id={group_id}, user_id={user_id}, size={message_size}")
            
            # 更新用户总消息数
            await self.db.db.users.update_one(
                {'user_id': user_id},
                {'$inc': {'total_messages': 1}},
                upsert=True
            )
            logger.info(f"已更新用户 {user_id} 的总消息数")
            
        except Exception as e:
            logger.error(f"添加消息统计失败: {e}", exc_info=True)
                   
    async def get_daily_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict[str, Any]], int]:
        """
        获取每日统计数据
        
        参数:
            group_id: 群组ID
            page: 页码
            
        返回:
            (统计数据列表, 总页数)
        """
        try:
            # 获取群组设置
            group_settings = await self.db.get_group_settings(group_id)
            limit = group_settings.get('daily_rank_size', 15)
            
            # 获取当天日期
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 获取统计数据
            stats = await self.db.get_daily_stats(group_id, today)
            
            # 计算总页数
            total_users = len(stats)
            total_pages = max(1, (total_users + limit - 1) // limit)
            
            # 调整页码
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages
                
            # 分页处理
            start_idx = (page - 1) * limit
            end_idx = min(start_idx + limit, total_users)
            page_stats = stats[start_idx:end_idx]
            
            return page_stats, total_pages
            
        except Exception as e:
            logger.error(f"获取每日统计数据失败: {e}", exc_info=True)
            return [], 1
            
    async def get_monthly_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict[str, Any]], int]:
        """
        获取月度统计数据
        
        参数:
            group_id: 群组ID
            page: 页码
            
        返回:
            (统计数据列表, 总页数)
        """
        try:
            # 获取群组设置
            group_settings = await self.db.get_group_settings(group_id)
            limit = group_settings.get('monthly_rank_size', 15)
            
            # 计算日期范围
            today = datetime.now()
            thirty_days_ago = (today - timedelta(days=30)).strftime('%Y-%m-%d')
            today_str = today.strftime('%Y-%m-%d')
            
            # 获取统计数据
            stats = await self.db.get_monthly_stats(group_id, thirty_days_ago, today_str)
            
            # 计算总页数
            total_users = len(stats)
            total_pages = max(1, (total_users + limit - 1) // limit)
            
            # 调整页码
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages
                
            # 分页处理
            start_idx = (page - 1) * limit
            end_idx = min(start_idx + limit, total_users)
            page_stats = stats[start_idx:end_idx]
            
            return page_stats, total_pages
            
        except Exception as e:
            logger.error(f"获取月度统计数据失败: {e}", exc_info=True)
            return [], 1
    
    async def get_user_stats(self, group_id: int, user_id: int, days: int = 30) -> Dict[str, Any]:
        """
        获取用户统计数据
    
        参数:
            group_id: 群组ID
            user_id: 用户ID
            days: 天数
            
        返回:
            用户统计数据
        """
        try:
            # 计算日期范围
            today = datetime.now()
            start_date = (today - timedelta(days=days)).strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
            
            # 构建聚合管道
            pipeline = [
                {
                    '$match': {
                        'group_id': group_id,
                        'user_id': user_id,
                        'date': {'$gte': start_date, '$lte': end_date}
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'total_messages': {'$sum': '$total_messages'},
                        'total_size': {'$sum': '$total_size'},
                        'days_active': {'$addToSet': '$date'}
                    }
                }
            ]
            
            # 执行聚合查询
            result = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
            
            if not result:
                return {
                    'user_id': user_id,
                    'total_messages': 0,
                    'total_size': 0,
                    'days_active': 0,
                    'avg_messages_per_day': 0
                }
                
            # 处理结果
            stat = result[0]
            days_active = len(stat.get('days_active', []))
            total_messages = stat.get('total_messages', 0)
            
            return {
                'user_id': user_id,
                'total_messages': total_messages,
                'total_size': stat.get('total_size', 0),
                'days_active': days_active,
                'avg_messages_per_day': round(total_messages / max(days_active, 1), 2)
            }
            
        except Exception as e:
            logger.error(f"获取用户统计数据失败: {e}", exc_info=True)
            return {
                'user_id': user_id,
                'total_messages': 0,
                'total_size': 0,
                'days_active': 0,
                'avg_messages_per_day': 0
            }
    
    async def get_group_stats(self, group_id: int, days: int = 30) -> Dict[str, Any]:
        """
        获取群组统计数据
        
        参数:
            group_id: 群组ID
            days: 天数
            
        返回:
            群组统计数据
        """
        try:
            # 计算日期范围
            today = datetime.now()
            start_date = (today - timedelta(days=days)).strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
            
            # 构建聚合管道
            pipeline = [
                {
                    '$match': {
                        'group_id': group_id,
                        'date': {'$gte': start_date, '$lte': end_date}
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'total_messages': {'$sum': '$total_messages'},
                        'total_size': {'$sum': '$total_size'},
                        'unique_users': {'$addToSet': '$user_id'},
                        'days_active': {'$addToSet': '$date'}
                    }
                }
            ]
            
            # 执行聚合查询
            result = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
            
            if not result:
                return {
                    'group_id': group_id,
                    'total_messages': 0,
                    'total_size': 0,
                    'unique_users': 0,
                    'days_active': 0,
                    'avg_messages_per_day': 0
                }
                
            # 处理结果
            stat = result[0]
            days_active = len(stat.get('days_active', []))
            total_messages = stat.get('total_messages', 0)
            unique_users = len(stat.get('unique_users', []))
            
            return {
                'group_id': group_id,
                'total_messages': total_messages,
                'total_size': stat.get('total_size', 0),
                'unique_users': unique_users,
                'days_active': days_active,
                'avg_messages_per_day': round(total_messages / max(days_active, 1), 2)
            }
            
        except Exception as e:
            logger.error(f"获取群组统计数据失败: {e}", exc_info=True)
            return {
                'group_id': group_id,
                'total_messages': 0,
                'total_size': 0,
                'unique_users': 0,
                'days_active': 0,
                'avg_messages_per_day': 0
            }
