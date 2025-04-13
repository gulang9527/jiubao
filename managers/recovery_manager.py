"""
恢复管理器，负责系统恢复和活动状态监控
"""
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SimpleRecoveryManager:
    """
    简化版恢复管理器
    仅负责监控系统活动状态
    """
    
    def __init__(self, bot_instance):
        """初始化恢复管理器"""
        self.bot = bot_instance
        self.last_activity_time = datetime.now()
        self.check_interval = 60  # 每60秒检查一次
        self._running = False
        self._task = None
        
    async def start(self):
        """启动恢复管理器"""
        if self._running:
            return
            
        self._running = True
        self.last_activity_time = datetime.now()
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info("恢复管理器已启动")
        
    def update_activity(self):
        """更新活动时间"""
        self.last_activity_time = datetime.now()
        
    async def _monitor_loop(self):
        """简化的监控循环，只记录系统不活动状态"""
        try:
            while self._running:
                # 检测系统是否长时间不活动
                current_time = datetime.now()
                inactive_time = (current_time - self.last_activity_time).total_seconds()
                
                # 如果超过5分钟不活动，记录日志
                if inactive_time > 300:
                    logger.warning(f"系统已经 {inactive_time:.2f} 秒没有活动")
                    
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            logger.info("恢复管理器监控循环已取消")
            raise
        except Exception as e:
            logger.error(f"恢复管理器监控循环异常: {e}", exc_info=True)
            
    async def shutdown(self):
        """关闭恢复管理器"""
        if not self._running:
            return
            
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("恢复管理器已关闭")
