"""
状态机实现，用于管理表单流程和用户状态
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Callable, Any, Optional, List

from telegram import Update, Message
from telegram.ext import CallbackContext

logger = logging.getLogger(__name__)

class State:
    """
    状态类，表示状态机中的一个状态
    """
    def __init__(self, name: str, handler: Callable, next_states: Optional[List[str]] = None):
        """
        初始化状态
        
        参数:
            name: 状态名称
            handler: 状态处理函数，接收(context, update, state_data)参数
            next_states: 可能的下一个状态列表
        """
        self.name = name
        self.handler = handler
        self.next_states = next_states or []
        
    async def process(self, context: Dict[str, Any], update: Update, state_data: Dict[str, Any]) -> Optional[str]:
        """
        处理当前状态
        
        参数:
            context: 上下文对象
            update: 更新对象
            state_data: 状态数据
            
        返回:
            下一个状态的名称或None
        """
        return await self.handler(context, update, state_data)

class StateMachine:
    """
    状态机类，管理状态转换和处理用户输入
    """
    def __init__(self, initial_state: str, states: Dict[str, State]):
        """
        初始化状态机
        
        参数:
            initial_state: 初始状态名称
            states: 状态字典，键为状态名称，值为State对象
        """
        self.initial_state = initial_state
        self.states = states
        self.current_state = initial_state
        self.state_data: Dict[str, Any] = {}
        
    async def process(self, context: Dict[str, Any], update: Update) -> bool:
        """
        处理输入
        
        参数:
            context: 上下文对象
            update: 更新对象
            
        返回:
            是否处理了输入
        """
        if self.current_state not in self.states:
            logger.error(f"无效的当前状态: {self.current_state}")
            return False
            
        current_state = self.states[self.current_state]
        next_state = await current_state.process(context, update, self.state_data)
        
        if next_state:
            if next_state in self.states:
                logger.info(f"状态转换: {self.current_state} -> {next_state}")
                self.current_state = next_state
                return True
            else:
                logger.error(f"无效的下一个状态: {next_state}")
                
        return True
    
    def reset(self):
        """重置状态机到初始状态"""
        self.current_state = self.initial_state
        self.state_data = {}
        
    def get_current_state(self) -> Optional[State]:
        """获取当前状态对象"""
        return self.states.get(self.current_state)
        
    def set_data(self, key: str, value: Any):
        """设置状态数据"""
        self.state_data[key] = value
        
    def get_data(self, key: str, default: Any = None) -> Any:
        """获取状态数据"""
        return self.state_data.get(key, default)
        
    def is_in_state(self, state_name: str) -> bool:
        """检查是否处于指定状态"""
        return self.current_state == state_name

class FormStateMachine(StateMachine):
    """
    表单状态机，专门用于处理表单流程
    """
    def __init__(self, form_id: str, states: Dict[str, State], field_order: List[str], completion_handler: Callable):
        """
        初始化表单状态机
        
        参数:
            form_id: 表单ID
            states: 状态字典
            field_order: 字段顺序列表
            completion_handler: 表单完成时的处理函数
        """
        super().__init__("initial", states)
        self.form_id = form_id
        self.field_order = field_order
        self.current_field_idx = 0
        self.completion_handler = completion_handler
        self.form_data: Dict[str, Any] = {}
        self.created_at = datetime.now()
        
    async def process_field(self, context: Dict[str, Any], update: Update) -> bool:
        """
        处理当前字段
        
        参数:
            context: 上下文对象
            update: 更新对象
            
        返回:
            是否处理了输入
        """
        if self.current_field_idx >= len(self.field_order):
            # 表单已完成，执行完成处理
            await self.completion_handler(context, update, self.form_data)
            return True
            
        current_field = self.field_order[self.current_field_idx]
        field_state = f"field_{current_field}"
        
        if field_state in self.states:
            self.current_state = field_state
            result = await super().process(context, update)
            
            # 如果字段处理成功，前进到下一个字段
            if result and self.is_in_state(f"validated_{current_field}"):
                self.current_field_idx += 1
                if self.current_field_idx < len(self.field_order):
                    next_field = self.field_order[self.current_field_idx]
                    self.current_state = f"field_{next_field}"
                else:
                    self.current_state = "completed"
                    # 执行完成处理
                    await self.completion_handler(context, update, self.form_data)
            
            return True
        else:
            logger.error(f"未找到字段状态: {field_state}")
            return False
            
    def set_field_value(self, field_name: str, value: Any):
        """设置字段值"""
        self.form_data[field_name] = value
        
    def get_field_value(self, field_name: str, default: Any = None) -> Any:
        """获取字段值"""
        return self.form_data.get(field_name, default)
        
    def goto_field(self, field_name: str) -> bool:
        """跳转到指定字段"""
        try:
            field_idx = self.field_order.index(field_name)
            self.current_field_idx = field_idx
            self.current_state = f"field_{field_name}"
            return True
        except ValueError:
            logger.error(f"未找到字段: {field_name}")
            return False
            
    def get_current_field(self) -> Optional[str]:
        """获取当前字段名称"""
        if 0 <= self.current_field_idx < len(self.field_order):
            return self.field_order[self.current_field_idx]
        return None
        
    def is_completed(self) -> bool:
        """检查表单是否已完成"""
        return self.current_field_idx >= len(self.field_order)
    
    def get_completion_percentage(self) -> int:
        """获取表单完成百分比"""
        if not self.field_order:
            return 100
        return min(100, int((self.current_field_idx * 100) / len(self.field_order)))

class StateMachineManager:
    """
    状态机管理器，管理多个用户的状态机
    """
    def __init__(self, cleanup_interval: int = 3600, max_idle_time: int = 1800):
        """
        初始化状态机管理器
        
        参数:
            cleanup_interval: 清理间隔（秒）
            max_idle_time: 最大空闲时间（秒）
        """
        self.machines: Dict[str, Dict[int, StateMachine]] = {}  # 类型->用户ID->状态机
        self.last_activity: Dict[str, Dict[int, datetime]] = {}  # 类型->用户ID->最后活动时间
        self.cleanup_interval = cleanup_interval
        self.max_idle_time = max_idle_time
        self.cleanup_task = None
        self.locks: Dict[str, asyncio.Lock] = {}  # 类型->锁
        
    async def start(self):
        """启动状态机管理器"""
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("状态机管理器已启动")
        
    async def stop(self):
        """停止状态机管理器"""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("状态机管理器已停止")
        
    async def _cleanup_loop(self):
        """定期清理过期状态机的循环"""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_expired_machines()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"清理状态机出错: {e}", exc_info=True)
                await asyncio.sleep(60)  # 出错时等待短暂时间后重试
                
    async def _cleanup_expired_machines(self):
        """清理过期的状态机"""
        now = datetime.now()
        for machine_type in list(self.last_activity.keys()):
            lock = await self._get_lock(machine_type)
            async with lock:
                for user_id in list(self.last_activity.get(machine_type, {}).keys()):
                    last_active = self.last_activity[machine_type].get(user_id)
                    if last_active and (now - last_active).total_seconds() > self.max_idle_time:
                        # 清理过期的状态机
                        if machine_type in self.machines and user_id in self.machines[machine_type]:
                            del self.machines[machine_type][user_id]
                        if user_id in self.last_activity[machine_type]:
                            del self.last_activity[machine_type][user_id]
                        logger.info(f"已清理过期状态机: 类型={machine_type}, 用户ID={user_id}")
        
    async def _get_lock(self, machine_type: str) -> asyncio.Lock:
        """获取指定类型的锁"""
        if machine_type not in self.locks:
            self.locks[machine_type] = asyncio.Lock()
        return self.locks[machine_type]
    
    async def has_machine(self, machine_type: str, user_id: int) -> bool:
        """
        检查用户是否有指定类型的状态机
        
        参数:
            machine_type: 状态机类型
            user_id: 用户ID
            
        返回:
            是否存在状态机
        """
        lock = await self._get_lock(machine_type)
        async with lock:
            return (machine_type in self.machines and 
                    user_id in self.machines[machine_type])
    
    async def get_machine(self, machine_type: str, user_id: int) -> Optional[StateMachine]:
        """
        获取用户的状态机
        
        参数:
            machine_type: 状态机类型
            user_id: 用户ID
            
        返回:
            状态机实例或None
        """
        lock = await self._get_lock(machine_type)
        async with lock:
            if (machine_type in self.machines and 
                user_id in self.machines[machine_type]):
                # 更新最后活动时间
                if machine_type not in self.last_activity:
                    self.last_activity[machine_type] = {}
                self.last_activity[machine_type][user_id] = datetime.now()
                return self.machines[machine_type][user_id]
            return None
    
    async def set_machine(self, machine_type: str, user_id: int, machine: StateMachine):
        """
        设置用户的状态机
        
        参数:
            machine_type: 状态机类型
            user_id: 用户ID
            machine: 状态机实例
        """
        lock = await self._get_lock(machine_type)
        async with lock:
            # 初始化字典结构（如果需要）
            if machine_type not in self.machines:
                self.machines[machine_type] = {}
            if machine_type not in self.last_activity:
                self.last_activity[machine_type] = {}
                
            # 设置状态机和最后活动时间
            self.machines[machine_type][user_id] = machine
            self.last_activity[machine_type][user_id] = datetime.now()
            logger.info(f"已设置状态机: 类型={machine_type}, 用户ID={user_id}")
    
    async def remove_machine(self, machine_type: str, user_id: int):
        """
        移除用户的状态机
        
        参数:
            machine_type: 状态机类型
            user_id: 用户ID
        """
        lock = await self._get_lock(machine_type)
        async with lock:
            if machine_type in self.machines and user_id in self.machines[machine_type]:
                del self.machines[machine_type][user_id]
                logger.info(f"已移除状态机: 类型={machine_type}, 用户ID={user_id}")
            
            if machine_type in self.last_activity and user_id in self.last_activity[machine_type]:
                del self.last_activity[machine_type][user_id]
    
    async def process_update(self, machine_type: str, user_id: int, update: Update, context: Dict[str, Any]) -> bool:
        """
        处理更新
        
        参数:
            machine_type: 状态机类型
            user_id: 用户ID
            update: 更新对象
            context: 上下文对象
            
        返回:
            是否处理了更新
        """
        machine = await self.get_machine(machine_type, user_id)
        if not machine:
            return False
            
        # 处理更新
        try:
            return await machine.process(context, update)
        except Exception as e:
            logger.error(f"处理状态机更新出错: {e}", exc_info=True)
            return False
    
    async def get_all_machines(self, machine_type: str = None) -> Dict[int, StateMachine]:
        """
        获取所有状态机
        
        参数:
            machine_type: 可选的状态机类型过滤
            
        返回:
            用户ID到状态机的映射
        """
        if machine_type:
            # 返回指定类型的所有状态机
            lock = await self._get_lock(machine_type)
            async with lock:
                return self.machines.get(machine_type, {}).copy()
        else:
            # 返回所有状态机（合并不同类型）
            result = {}
            for m_type in self.machines:
                lock = await self._get_lock(m_type)
                async with lock:
                    for user_id, machine in self.machines[m_type].items():
                        result[f"{m_type}_{user_id}"] = machine
            return result
