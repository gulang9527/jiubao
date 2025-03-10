"""
设置管理器，管理用户设置状态
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from telegram import Message, Update

logger = logging.getLogger(__name__)

class SettingsManager:
    """
    管理用户设置状态的类
    用于处理设置流程中的状态保持和转换
    """
    def __init__(self, db):
        """
        初始化设置管理器
        
        参数:
            db: 数据库实例
        """
        self.db = db
        self._states = {}  # 存储用户设置状态
        self._global_lock = asyncio.Lock()  # 全局锁
        self._user_locks = {}  # 用户锁
        self._cleanup_task = None
        
    async def start(self):
        """启动设置管理器"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("设置管理器已启动")
        
    async def stop(self):
        """停止设置管理器"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("设置管理器已停止")
        
    async def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        """获取用户锁"""
        async with self._global_lock:
            if user_id not in self._user_locks:
                self._user_locks[user_id] = asyncio.Lock()
            return self._user_locks[user_id]
            
    async def _cleanup_loop(self):
        """定期清理过期状态"""
        while True:
            try:
                await asyncio.sleep(300)  # 每5分钟清理一次
                
                now = datetime.now()
                expired_keys = []
                
                async with self._global_lock:
                    # 查找过期的设置状态
                    for key, state in self._states.items():
                        if now - state.get('timestamp', now) > timedelta(minutes=10):
                            expired_keys.append(key)
                    
                    # 删除过期的设置状态
                    for key in expired_keys:
                        if key in self._states:
                            del self._states[key]
                            logger.info(f"已清理过期的设置状态: {key}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"清理过期设置状态失败: {e}", exc_info=True)
                await asyncio.sleep(60)  # 出错后等待1分钟
                
    async def start_setting(self, user_id: int, setting_type: str, group_id: int):
        """
        开始设置过程
        
        参数:
            user_id: 用户ID
            setting_type: 设置类型
            group_id: 群组ID
        """
        key = f"{user_id}_{setting_type}"
        user_lock = await self._get_user_lock(user_id)
        
        async with user_lock:
            # 如果已存在，先清理
            await self.clear_setting_state(user_id, setting_type)
            
            # 初始化新状态
            async with self._global_lock:
                self._states[key] = {
                    'group_id': group_id,
                    'step': 1,
                    'data': {},
                    'timestamp': datetime.now()
                }
            
            logger.info(f"已开始设置过程: user_id={user_id}, type={setting_type}, group_id={group_id}")
            
    async def get_setting_state(self, user_id: int, setting_type: str) -> Optional[Dict[str, Any]]:
        """
        获取设置状态
        
        参数:
            user_id: 用户ID
            setting_type: 设置类型
            
        返回:
            设置状态或None
        """
        key = f"{user_id}_{setting_type}"
        
        async with self._global_lock:
            state = self._states.get(key)
            if state:
                # 更新时间戳
                state['timestamp'] = datetime.now()
            return state.copy() if state else None
            
    async def update_setting_state(self, user_id: int, setting_type: str, data: Dict[str, Any], next_step: bool = False):
        """
        更新设置状态
        
        参数:
            user_id: 用户ID
            setting_type: 设置类型
            data: 新数据
            next_step: 是否进入下一步
        """
        key = f"{user_id}_{setting_type}"
        user_lock = await self._get_user_lock(user_id)
        
        async with user_lock:
            async with self._global_lock:
                if key not in self._states:
                    logger.warning(f"尝试更新不存在的设置状态: {key}")
                    return
                    
                # 更新数据
                self._states[key]['data'].update(data)
                
                # 更新步骤（如果需要）
                if next_step:
                    self._states[key]['step'] += 1
                    
                # 更新时间戳
                self._states[key]['timestamp'] = datetime.now()
                
            logger.info(f"已更新设置状态: {key}, step={self._states[key]['step']}")
            
    async def clear_setting_state(self, user_id: int, setting_type: str):
        """
        清除设置状态
        
        参数:
            user_id: 用户ID
            setting_type: 设置类型
        """
        key = f"{user_id}_{setting_type}"
        
        async with self._global_lock:
            if key in self._states:
                del self._states[key]
                logger.info(f"已清除设置状态: {key}")
                
    async def get_active_settings(self, user_id: int) -> List[str]:
        """
        获取用户当前活动的设置类型
        
        参数:
            user_id: 用户ID
            
        返回:
            设置类型列表
        """
        user_id_str = str(user_id) + "_"
        
        async with self._global_lock:
            active_settings = []
            for key in self._states:
                if key.startswith(str(user_id) + "_"):
                    setting_type = key[len(str(user_id)) + 1:]
                    active_settings.append(setting_type)
            return active_settings
            
    async def process_setting(self, user_id: int, setting_type: str, message: Message, processor):
        """
        处理设置输入
        
        参数:
            user_id: 用户ID
            setting_type: 设置类型
            message: 消息对象
            processor: 处理函数，接收(state, message)参数
            
        返回:
            是否成功处理
        """
        state = await self.get_setting_state(user_id, setting_type)
        if not state:
            return False
            
        try:
            await processor(state, message)
            return True
        except Exception as e:
            logger.error(f"处理设置输入出错: {e}", exc_info=True)
            # 通知用户出错
            try:
                await message.reply_text(f"❌ 设置处理过程出错，请重试或使用 /cancel 取消")
            except Exception:
                pass
            return True  # 虽然出错，但仍然算处理了输入
