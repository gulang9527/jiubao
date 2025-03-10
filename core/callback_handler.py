"""
统一的回调处理框架，用于管理和分发回调查询
"""
import logging
from typing import Dict, Callable, Any, List, Tuple, Optional
from telegram import Update
from telegram.ext import CallbackContext

logger = logging.getLogger(__name__)

class CallbackHandler:
    """
    统一的回调处理框架，管理并分发回调查询到对应的处理函数
    """
    def __init__(self):
        """初始化回调处理器"""
        self.handlers: Dict[str, Callable] = {}  # 前缀到处理函数的映射
        logger.info("初始化回调处理器")
        
    def register(self, prefix: str, handler: Callable):
        """
        注册回调处理函数
        
        参数:
            prefix: 回调数据前缀
            handler: 处理函数，接收(update, context, data)参数
        """
        self.handlers[prefix] = handler
        logger.info(f"已注册回调处理函数: {prefix}")
        
    async def handle(self, update: Update, context: CallbackContext) -> bool:
        """
        处理回调查询
        
        参数:
            update: 更新对象
            context: 回调上下文
            
        返回:
            是否处理了回调
        """
        query = update.callback_query
        if not query:
            return False
            
        data = query.data
        if not data:
            return False
        
        # 尝试匹配处理函数
        for prefix, handler in self.handlers.items():
            if data.startswith(prefix):
                try:
                    # 调用匹配的处理函数
                    await handler(update, context, data)
                    return True
                except Exception as e:
                    logger.error(f"处理回调 {prefix} 出错: {e}", exc_info=True)
                    await self.handle_error(update, e)
                    return True
        
        logger.warning(f"未找到回调处理函数: {data}")
        return False
    
    async def handle_error(self, update: Update, error: Exception):
        """
        处理回调错误
        
        参数:
            update: 更新对象
            error: 异常对象
        """
        if update.callback_query:
            try:
                await update.callback_query.answer("处理回调时出错，请重试")
                await update.callback_query.edit_message_text("❌ 操作出错，请重试")
            except Exception as e:
                logger.error(f"错误处理失败: {e}")
    
    @staticmethod
    def parse_data(data: str) -> Tuple[str, List[str]]:
        """
        解析回调数据
        
        参数:
            data: 回调数据字符串
            
        返回:
            (前缀, 参数列表)
        """
        parts = data.split('_')
        if len(parts) < 2:
            return data, []
        
        # 确定前缀的长度
        prefix_parts = 1
        for i in range(1, min(3, len(parts))):
            potential_prefix = '_'.join(parts[:i])
            if potential_prefix in ["settings", "kwform", "bcform", "auto_delete", "switch_toggle"]:
                prefix_parts = i
                break
        
        prefix = '_'.join(parts[:prefix_parts])
        args = parts[prefix_parts:]
        return prefix, args
    
    @staticmethod
    def build_data(*parts: Any) -> str:
        """
        构建回调数据
        
        参数:
            *parts: 回调数据部分
            
        返回:
            构建的回调数据字符串
        """
        return '_'.join(str(part) for part in parts)
    
    @staticmethod
    def get_group_id(data: str) -> Optional[int]:
        """
        从回调数据中提取群组ID
        
        参数:
            data: 回调数据字符串
            
        返回:
            群组ID或None
        """
        parts = data.split('_')
        if not parts:
            return None
        
        # 尝试从最后一个部分获取群组ID
        try:
            return int(parts[-1])
        except (ValueError, IndexError):
            # 尝试从倒数第二个部分获取
            if len(parts) >= 2:
                try:
                    return int(parts[-2])
                except ValueError:
                    pass
        
        # 扫描所有部分
        for part in parts:
            try:
                part_int = int(part)
                # 群组ID通常是负数
                if part_int < 0:
                    return part_int
            except ValueError:
                continue
        
        return None
