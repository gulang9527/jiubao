"""
轮播消息时间校准模块，处理系统休眠后的时间校准
"""
import logging
from datetime import datetime, timedelta
import asyncio
import time
import math

logger = logging.getLogger(__name__)

class TimeCalibrationManager:
    """
    时间校准管理器，用于处理系统休眠后的轮播消息时间同步
    """
    def __init__(self, db, broadcast_manager=None):
        """
        初始化时间校准管理器
        
        参数:
            db: 数据库实例
            broadcast_manager: 轮播消息管理器实例
        """
        self.db = db
        self.broadcast_manager = broadcast_manager
        self.last_active_time = datetime.now()
        self.calibration_interval = 60  # 每60秒检查一次时间同步
        self._running = False
        self._task = None
        self._system_wake = asyncio.Event()
        self._system_wake.set()  # 初始状态设为醒着
        
        # 记录每个轮播消息的下一次预期执行时间
        self.next_broadcast_times = {}
        
    async def start(self):
        """启动时间校准服务"""
        if self._running:
            return
            
        self._running = True
        self.last_active_time = datetime.now()
        self._task = asyncio.create_task(self._calibration_loop())
        logger.info("时间校准服务已启动")
        
    async def stop(self):
        """停止时间校准服务"""
        if not self._running:
            return
            
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("时间校准服务已停止")
    
    async def _calibration_loop(self):
        """时间校准循环"""
        try:
            while self._running:
                await self._check_time_drift()
                await asyncio.sleep(self.calibration_interval)
        except asyncio.CancelledError:
            logger.info("时间校准循环已取消")
            raise
        except Exception as e:
            logger.error(f"时间校准循环异常: {e}", exc_info=True)
    
    async def _check_time_drift(self):
        """检查时间偏移"""
        try:
            current_time = datetime.now()
            expected_time = self.last_active_time + timedelta(seconds=self.calibration_interval)
            
            # 计算时间偏移（秒）
            time_drift = (current_time - expected_time).total_seconds()
            
            # 更新最后活动时间
            self.last_active_time = current_time
            
            # 如果时间偏移超过阈值（比如30秒），可能发生了休眠
            if time_drift > 30:
                logger.warning(f"检测到系统可能休眠，时间偏移: {time_drift:.2f}秒")
                await self._handle_system_wake(time_drift)
            
            # 定期更新下一次预期执行时间（不管是否发生休眠）
            await self._update_next_broadcast_times()
            
        except Exception as e:
            logger.error(f"检查时间偏移失败: {e}", exc_info=True)
    
    async def _handle_system_wake(self, time_drift):
        """
        处理系统唤醒后的时间校准
        
        参数:
            time_drift: 时间偏移（秒）
        """
        logger.info(f"系统唤醒，开始时间校准，偏移: {time_drift:.2f}秒")
        
        # 设置系统唤醒事件
        self._system_wake.set()
        
        try:
            # 1. 调整所有预期的轮播时间
            await self._adjust_broadcast_times(time_drift)
            
            # 2. 检查是否有错过的轮播消息需要立即发送
            await self._process_missed_broadcasts()
            
            # 3. 如果有broadcast_manager，通知其进行一次立即检查
            if self.broadcast_manager:
                await self.broadcast_manager.force_check()
                
            logger.info("时间校准完成")
            
        except Exception as e:
            logger.error(f"处理系统唤醒时出错: {e}", exc_info=True)
    
    async def _adjust_broadcast_times(self, time_drift):
        """
        调整所有轮播消息的下一次执行时间
        
        参数:
            time_drift: 时间偏移（秒）
        """
        try:
            # 获取所有活动的轮播消息
            active_broadcasts = await self.db.get_active_broadcasts()
            
            for broadcast in active_broadcasts:
                broadcast_id = str(broadcast["_id"])
                
                # 计算此轮播消息的新的下一次执行时间
                if broadcast_id in self.next_broadcast_times:
                    old_next_time = self.next_broadcast_times[broadcast_id]
                    
                    # 计算新的下一次执行时间，考虑到时间偏移
                    # 如果是周期性的，需要计算下一个周期点
                    interval_minutes = broadcast.get('interval', 0)
                    
                    if interval_minutes > 0:
                        # 计算在休眠期间应该发送多少次
                        minutes_drift = time_drift / 60  # 转换为分钟
                        cycles_missed = math.floor(minutes_drift / interval_minutes)
                        
                        # 计算下一个发送时间点
                        next_time = old_next_time + timedelta(minutes=interval_minutes * (cycles_missed + 1))
                        
                        # 记录新的下一次发送时间
                        self.next_broadcast_times[broadcast_id] = next_time
                        
                        logger.info(f"轮播 {broadcast_id} 调整后的下一次发送时间: {next_time}")
                    else:
                        # 单次发送，如果已过期则移除
                        if old_next_time < datetime.now():
                            self.next_broadcast_times.pop(broadcast_id, None)
                            logger.info(f"单次轮播 {broadcast_id} 已过期，从跟踪中移除")
            
            logger.info(f"已调整 {len(active_broadcasts)} 个轮播消息的时间")
            
        except Exception as e:
            logger.error(f"调整轮播时间失败: {e}", exc_info=True)
    
    async def _process_missed_broadcasts(self):
        """处理错过的轮播消息"""
        try:
            current_time = datetime.now()
            
            # 获取所有活动的轮播消息
            active_broadcasts = await self.db.get_active_broadcasts()
            
            for broadcast in active_broadcasts:
                broadcast_id = str(broadcast["_id"])
                last_broadcast = broadcast.get('last_broadcast')
                interval = broadcast.get('interval', 0)
                
                # 只处理周期性的轮播消息
                if interval <= 0:
                    continue
                
                # 如果从未发送过，应该立即发送一次
                if not last_broadcast:
                    if self.broadcast_manager:
                        await self.broadcast_manager.send_broadcast(broadcast)
                        logger.info(f"发送初始化的轮播消息: {broadcast_id}")
                    continue
                
                # 计算应该发送的次数
                time_since_last = (current_time - last_broadcast).total_seconds() / 60  # 转换为分钟
                
                # 如果至少错过了一个周期，立即发送一次
                if time_since_last >= interval:
                    if self.broadcast_manager:
                        await self.broadcast_manager.send_broadcast(broadcast)
                        logger.info(f"发送错过的轮播消息: {broadcast_id}, 已经 {time_since_last:.2f} 分钟未发送")
            
        except Exception as e:
            logger.error(f"处理错过的轮播消息失败: {e}", exc_info=True)
    
    async def _update_next_broadcast_times(self):
        """更新所有轮播消息的下一次执行时间"""
        try:
            current_time = datetime.now()
            active_broadcasts = await self.db.get_active_broadcasts()
            
            for broadcast in active_broadcasts:
                broadcast_id = str(broadcast["_id"])
                last_broadcast = broadcast.get('last_broadcast')
                interval = broadcast.get('interval', 0)
                
                # 计算下一次执行时间
                if interval > 0 and last_broadcast:
                    # 周期性轮播
                    next_time = last_broadcast + timedelta(minutes=interval)
                    
                    # 如果已经过了下一次执行时间，计算之后的下一个周期点
                    if next_time <= current_time:
                        # 计算已经错过了多少个完整周期
                        minutes_elapsed = (current_time - last_broadcast).total_seconds() / 60
                        cycles_elapsed = math.floor(minutes_elapsed / interval)
                        
                        # 计算下一个未来周期点
                        next_time = last_broadcast + timedelta(minutes=interval * (cycles_elapsed + 1))
                else:
                    # 单次轮播或首次发送的周期性轮播
                    next_time = broadcast.get('start_time')
                    if not next_time or next_time <= current_time:
                        next_time = current_time
                
                # 更新下一次执行时间
                self.next_broadcast_times[broadcast_id] = next_time
            
            # 清理已不活跃的轮播记录
            for broadcast_id in list(self.next_broadcast_times.keys()):
                if not any(str(b["_id"]) == broadcast_id for b in active_broadcasts):
                    self.next_broadcast_times.pop(broadcast_id, None)
            
        except Exception as e:
            logger.error(f"更新轮播执行时间失败: {e}", exc_info=True)
            
    async def register_broadcast(self, broadcast):
        """
        注册新的轮播消息到时间校准系统
        
        参数:
            broadcast: 轮播消息对象
        """
        broadcast_id = str(broadcast["_id"])
        
        # 计算初始的下一次执行时间
        start_time = broadcast.get('start_time')
        
        # 如果开始时间在未来，使用开始时间作为下一次执行时间
        if start_time and start_time > datetime.now():
            self.next_broadcast_times[broadcast_id] = start_time
        else:
            # 否则使用当前时间作为下一次执行时间
            self.next_broadcast_times[broadcast_id] = datetime.now()
        
        logger.info(f"已注册轮播消息到时间校准系统: {broadcast_id}, 下一次执行时间: {self.next_broadcast_times[broadcast_id]}")
        
    async def handle_broadcast_sent(self, broadcast_id):
        """
        处理轮播消息发送后的时间更新
        
        参数:
            broadcast_id: 轮播消息ID
        """
        try:
            # 获取轮播消息
            broadcast = await self.db.get_broadcast_by_id(broadcast_id)
            if not broadcast:
                logger.warning(f"找不到轮播消息: {broadcast_id}")
                return
            
            # 更新最后发送时间
            current_time = datetime.now()
            
            # 计算下一次发送时间
            interval = broadcast.get('interval', 0)
            if interval > 0:
                # 周期性轮播，计算下一个发送时间
                next_time = current_time + timedelta(minutes=interval)
                self.next_broadcast_times[broadcast_id] = next_time
                logger.info(f"轮播消息 {broadcast_id} 已发送，下一次执行时间: {next_time}")
            else:
                # 单次轮播，移除记录
                self.next_broadcast_times.pop(broadcast_id, None)
                logger.info(f"单次轮播消息 {broadcast_id} 已发送，从跟踪中移除")
                
        except Exception as e:
            logger.error(f"处理轮播消息发送后更新失败: {broadcast_id}, 错误: {e}", exc_info=True)
            
    async def get_next_execution_time(self, broadcast_id):
        """
        获取轮播消息的下一次执行时间
        
        参数:
            broadcast_id: 轮播消息ID
            
        返回:
            下一次执行时间或None
        """
        return self.next_broadcast_times.get(str(broadcast_id))
        
    async def force_recalculate_all(self):
        """
        强制重新计算所有轮播消息的下一次执行时间
        用于系统启动或重启时
        """
        logger.info("开始强制重新计算所有轮播消息的执行时间")
        try:
            current_time = datetime.now()
            active_broadcasts = await self.db.get_active_broadcasts()
            
            for broadcast in active_broadcasts:
                broadcast_id = str(broadcast["_id"])
                last_broadcast = broadcast.get('last_broadcast')
                interval = broadcast.get('interval', 0)
                start_time = broadcast.get('start_time')
                
                # 针对不同类型的轮播消息计算下一次执行时间
                if interval <= 0:
                    # 单次轮播
                    if start_time and start_time > current_time:
                        self.next_broadcast_times[broadcast_id] = start_time
                    else:
                        # 如果单次轮播已经过期但未发送，设置为立即发送
                        if not last_broadcast:
                            self.next_broadcast_times[broadcast_id] = current_time
                        else:
                            # 如果已经发送过，从跟踪中移除
                            self.next_broadcast_times.pop(broadcast_id, None)
                else:
                    # 周期性轮播
                    if not last_broadcast:
                        # 首次发送
                        if start_time and start_time > current_time:
                            self.next_broadcast_times[broadcast_id] = start_time
                        else:
                            self.next_broadcast_times[broadcast_id] = current_time
                    else:
                        # 已经发送过，计算下一次发送时间
                        next_time = last_broadcast + timedelta(minutes=interval)
                        
                        # 如果下一次发送时间已经过去，计算新的发送时间
                        if next_time <= current_time:
                            # 计算已经错过了多少个完整周期
                            minutes_elapsed = (current_time - last_broadcast).total_seconds() / 60
                            cycles_elapsed = math.floor(minutes_elapsed / interval)
                            
                            # 计算下一个未来周期点
                            next_time = last_broadcast + timedelta(minutes=interval * (cycles_elapsed + 1))
                        
                        self.next_broadcast_times[broadcast_id] = next_time
            
            logger.info(f"已重新计算 {len(active_broadcasts)} 个轮播消息的执行时间")
            
        except Exception as e:
            logger.error(f"强制重新计算轮播执行时间失败: {e}", exc_info=True)
            
    def get_time_drift(self):
        """
        获取当前检测到的时间偏移
        
        返回:
            时间偏移（秒）
        """
        current_time = datetime.now()
        expected_time = self.last_active_time + timedelta(seconds=self.calibration_interval)
        return (current_time - expected_time).total_seconds()
