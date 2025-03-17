"""
统计恢复模块 - 负责在机器人重启后恢复中断期间的消息统计
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class StatisticsRecoverySystem:
    """
    消息统计恢复系统 - 在机器人重启后尝试恢复中断期间的消息统计
    """
    
    def __init__(self, bot_instance):
        """
        初始化统计恢复系统
        
        参数:
            bot_instance: 机器人实例
        """
        self.bot = bot_instance
        self.db = bot_instance.db
        self.stats_manager = bot_instance.stats_manager
        
    async def check_and_recover(self):
        """
        检查是否需要恢复统计并执行恢复
        """
        try:
            logger.info("开始检查是否需要恢复消息统计...")
            
            # 获取上次运行时间
            last_run_time = await self.get_last_run_time()
            current_time = datetime.now()
            
            # 记录本次启动时间
            await self.set_last_run_time(current_time)
            
            # 如果没有上次运行时间记录，则无需恢复
            if not last_run_time:
                logger.info("首次运行，无需恢复统计")
                return
                
            # 计算中断时间
            downtime = current_time - last_run_time
            downtime_seconds = downtime.total_seconds()
            
            # 如果中断时间很短（小于5分钟），无需恢复
            if downtime_seconds < 300:
                logger.info(f"中断时间较短 ({downtime_seconds:.2f}秒)，无需恢复统计")
                return
                
            # 限制最大恢复时间为48小时，避免过度负载
            max_recovery_time = timedelta(hours=48)
            if downtime > max_recovery_time:
                logger.warning(f"中断时间过长 ({downtime})，限制恢复范围为最近48小时")
                last_run_time = current_time - max_recovery_time
            
            logger.info(f"检测到中断时间: {downtime}，开始尝试恢复统计...")
            
            # 获取所有需要恢复统计的群组
            groups = await self.get_groups_to_recover()
            
            # 对每个群组执行恢复
            total_recovered = 0
            for group in groups:
                group_id = group['group_id']
                recovered = await self.recover_group_statistics(group_id, last_run_time, current_time)
                total_recovered += recovered
                
            logger.info(f"统计恢复完成，共恢复 {total_recovered} 条消息记录")
            
        except Exception as e:
            logger.error(f"恢复统计时出错: {e}", exc_info=True)
    
    async def get_last_run_time(self) -> Optional[datetime]:
        """
        获取机器人上次运行时间
        
        返回:
            上次运行时间或None（如果是首次运行）
        """
        try:
            # 从数据库获取系统状态
            system_status = await self.db.db.system_status.find_one({"_id": "bot_status"})
            
            if system_status and "last_run_time" in system_status:
                return system_status["last_run_time"]
                
            return None
            
        except Exception as e:
            logger.error(f"获取上次运行时间失败: {e}", exc_info=True)
            return None
    
    async def set_last_run_time(self, timestamp: datetime):
        """
        设置机器人本次运行时间
        
        参数:
            timestamp: 当前时间戳
        """
        try:
            # 更新或插入系统状态
            await self.db.db.system_status.update_one(
                {"_id": "bot_status"},
                {"$set": {"last_run_time": timestamp}},
                upsert=True
            )
            logger.info(f"已更新运行时间: {timestamp}")
            
        except Exception as e:
            logger.error(f"设置运行时间失败: {e}", exc_info=True)
    
    async def get_groups_to_recover(self) -> List[Dict[str, Any]]:
        """
        获取需要恢复统计的群组列表
        
        返回:
            群组列表
        """
        try:
            # 只恢复启用了统计功能的群组
            from db.models import GroupPermission
            
            all_groups = await self.db.find_all_groups()
            groups_to_recover = []
            
            for group in all_groups:
                group_id = group['group_id']
                # 检查群组是否启用统计功能
                if await self.bot.has_permission(group_id, GroupPermission.STATS):
                    # 检查机器人是否有管理员权限
                    is_admin = await self.check_bot_admin_in_group(group_id)
                    if is_admin:
                        groups_to_recover.append(group)
                    else:
                        logger.warning(f"群组 {group_id} 中机器人没有管理员权限，跳过恢复")
            
            logger.info(f"找到 {len(groups_to_recover)} 个需要恢复统计的群组")
            return groups_to_recover
            
        except Exception as e:
            logger.error(f"获取群组列表失败: {e}", exc_info=True)
            return []
    
    async def check_bot_admin_in_group(self, group_id: int) -> bool:
        """
        检查机器人在群组中是否有管理员权限
        """
        try:
            # 检查应用程序是否已初始化
            if not hasattr(self.bot.application, 'bot') or not getattr(self.bot.application.bot, '_initialized', False):
                logger.warning(f"机器人应用未完全初始化，无法检查群组 {group_id} 的权限")
                return True  # 假设有权限，后续会自动适应
                
            bot_id = self.bot.application.bot.id
            member = await self.bot.application.bot.get_chat_member(group_id, bot_id)
            return member.status in ['administrator', 'creator']
        except Exception as e:
            logger.error(f"检查机器人权限失败: {e}")
            # 出错时默认允许恢复，避免因权限检查失败而跳过恢复
            return True
    
    async def recover_group_statistics(self, group_id: int, start_time: datetime, end_time: datetime) -> int:
        """
        恢复指定群组在给定时间段内的消息统计
        
        参数:
            group_id: 群组ID
            start_time: 开始时间
            end_time: 结束时间
            
        返回:
            恢复的消息数量
        """
        try:
            logger.info(f"开始恢复群组 {group_id} 的统计数据...")
            
            # 获取群组设置
            settings = await self.db.get_group_settings(group_id)
            
            # 获取最小字节数和媒体消息统计设置
            min_bytes = settings.get('min_bytes', 0)
            count_media = settings.get('count_media', True)
            
            # 恢复消息统计
            # 由于使用Telegram API获取历史消息需要管理员权限，而且可能受到API限制
            # 这里使用保守的恢复方法：基于平均统计进行估算补充
            
            # 计算平均每日消息数
            avg_messages = await self.calculate_average_messages(group_id)
            if avg_messages <= 0:
                logger.warning(f"群组 {group_id} 没有足够的历史数据用于估算")
                return 0
                
            # 基于日均消息数估算中断期间的消息
            days = (end_time - start_time).days + 1
            estimated_total = avg_messages * days
            
            # 获取用户统计比例
            user_ratios = await self.get_user_message_ratios(group_id)
            
            # 估算每个用户的消息数
            recovered_count = 0
            
            # 确定日期列表(可能跨多天)
            date_range = []
            current_date = start_time.date()
            end_date = end_time.date()
            
            while current_date <= end_date:
                date_range.append(current_date.strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)
                
            # 为每个活跃用户在每个日期添加估算的消息数
            for user_id, ratio in user_ratios.items():
                # 估算该用户在该时间段的消息数
                estimated_user_messages = int(estimated_total * ratio)
                
                if estimated_user_messages <= 0:
                    continue
                    
                # 将消息数平均分配到每天
                daily_messages = max(1, estimated_user_messages // len(date_range))
                
                # 遍历每个日期添加记录
                for date_str in date_range:
                    # 添加用户当天的消息统计
                    stat_data = {
                        'group_id': group_id,
                        'user_id': user_id,
                        'date': date_str,
                        'total_messages': daily_messages,
                        'total_size': daily_messages * 50,  # 假设平均每条消息50字节
                        'media_type': None,
                        'recovered': True  # 标记为恢复的数据
                    }
                    
                    # 避免重复添加
                    existing = await self.db.db.message_stats.find_one({
                        'group_id': group_id,
                        'user_id': user_id,
                        'date': date_str
                    })
                    
                    if not existing:
                        await self.db.db.message_stats.insert_one(stat_data)
                        recovered_count += daily_messages
                        
            logger.info(f"群组 {group_id} 恢复完成，估算添加了 {recovered_count} 条消息记录")
            return recovered_count
            
        except Exception as e:
            logger.error(f"恢复群组 {group_id} 统计失败: {e}", exc_info=True)
            return 0
    
    async def calculate_average_messages(self, group_id: int) -> float:
        """
        计算群组的日均消息数
        
        参数:
            group_id: 群组ID
            
        返回:
            日均消息数
        """
        try:
            # 获取最近10天的消息统计
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=10)
            
            pipeline = [
                {
                    '$match': {
                        'group_id': group_id,
                        'date': {
                            '$gte': start_date.strftime("%Y-%m-%d"),
                            '$lte': end_date.strftime("%Y-%m-%d")
                        }
                    }
                },
                {
                    '$group': {
                        '_id': '$date',
                        'total': {'$sum': '$total_messages'}
                    }
                }
            ]
            
            # 执行聚合查询
            daily_stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
            
            if not daily_stats:
                return 0
                
            # 计算日均消息数
            total_messages = sum(day['total'] for day in daily_stats)
            return total_messages / len(daily_stats)
            
        except Exception as e:
            logger.error(f"计算日均消息数失败: {e}", exc_info=True)
            return 0
    
    async def get_user_message_ratios(self, group_id: int) -> Dict[int, float]:
        """
        获取群组中各用户的消息比例
        
        参数:
            group_id: 群组ID
            
        返回:
            用户ID到消息比例的映射
        """
        try:
            # 获取最近30天的用户消息统计
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
            
            pipeline = [
                {
                    '$match': {
                        'group_id': group_id,
                        'date': {
                            '$gte': start_date.strftime("%Y-%m-%d"),
                            '$lte': end_date.strftime("%Y-%m-%d")
                        }
                    }
                },
                {
                    '$group': {
                        '_id': '$user_id',
                        'total': {'$sum': '$total_messages'}
                    }
                }
            ]
            
            # 执行聚合查询
            user_stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
            
            if not user_stats:
                return {}
                
            # 计算总消息数
            total_messages = sum(stat['total'] for stat in user_stats)
            
            if total_messages <= 0:
                return {}
                
            # 计算每个用户的消息比例
            return {stat['_id']: stat['total'] / total_messages for stat in user_stats}
            
        except Exception as e:
            logger.error(f"获取用户消息比例失败: {e}", exc_info=True)
            return {}
