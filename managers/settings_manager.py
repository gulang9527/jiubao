import asyncio
from typing import Dict, Any, Optional
from datetime import datetime

class SettingsManager:
    """设置管理类"""
    def __init__(self, db):
        self.db = db
        self._states = {}
        self._locks = {}

    async def start(self):
        """启动设置管理器"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        
    async def stop(self):
        """停止设置管理器"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_loop(self):
        """清理过期状态"""
        while True:
            try:
                now = datetime.now()
                expired_keys = []
                async with asyncio.Lock():
                    for key, state in self._states.items():
                        if (now - state['timestamp']).total_seconds() > 300:
                            expired_keys.append(key)
                    for key in expired_keys:
                        await self._cleanup_state(key)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break

    async def _cleanup_state(self, key: str):
        """清理特定状态"""
        if key in self._states:
            del self._states[key]
        if key in self._locks:
            del self._locks[key]

    async def start_setting(self, user_id: int, setting_type: str, group_id: int):
        """开始设置过程"""
        async with self._get_state_lock(user_id):
            state_key = f"setting_{user_id}_{setting_type}"
            if state_key in self._states:
                del self._states[state_key]
            self._states[state_key] = {
                'group_id': group_id,
                'step': 1,
                'data': {},
                'timestamp': datetime.now()
            }

    async def get_setting_state(self, user_id: int, setting_type: str) -> Optional[Dict[str, Any]]:
        """获取设置状态"""
        state_key = f"setting_{user_id}_{setting_type}"
        return self._states.get(state_key)

    async def update_setting_state(self, user_id: int, setting_type: str, data: Dict[str, Any], next_step: bool = False):
        """更新设置状态"""
        state_key = f"setting_{user_id}_{setting_type}"
        async with self._get_state_lock(user_id):
            if state_key not in self._states:
                return
            self._states[state_key]['data'].update(data)
            if next_step:
                self._states[state_key]['step'] += 1
            self._states[state_key]['timestamp'] = datetime.now()

    async def clear_setting_state(self, user_id: int, setting_type: str):
        """清除设置状态"""
        state_key = f"setting_{user_id}_{setting_type}"
        async with self._get_state_lock(user_id):
            if state_key in self._states:
                del self._states[state_key]

    async def _get_state_lock(self, user_id: int):
        """获取用户状态锁"""
        if user_id not in self._locks:
            self._locks[user_id] = asyncio.Lock()
        return self._locks[user_id]