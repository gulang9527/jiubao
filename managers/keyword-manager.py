"""
关键词管理器，处理关键词匹配和回复
"""
import re
import logging
from typing import Dict, Any, Optional, List, Callable

from telegram import Message

logger = logging.getLogger(__name__)

class KeywordManager:
    """
    关键词管理器，处理关键词匹配和回复
    """
    def __init__(self, db):
        """
        初始化关键词管理器
        
        参数:
            db: 数据库实例
        """
        self.db = db
        self._built_in_handlers = {}  # 内置关键词处理函数
        
    def register_built_in_handler(self, pattern: str, handler: Callable):
        """
        注册内置关键词处理函数
        
        参数:
            pattern: 关键词模式
            handler: 处理函数
        """
        self._built_in_handlers[pattern] = handler
        logger.info(f"已注册内置关键词处理函数: {pattern}")
        
    async def match_keyword(self, group_id: int, text: str, message: Message) -> Optional[str]:
        """
        匹配关键词
        
        参数:
            group_id: 群组ID
            text: 消息文本
            message: 消息对象
            
        返回:
            匹配的关键词ID或None
        """
        # 首先检查内置处理函数
        for pattern, handler in self._built_in_handlers.items():
            if self._match_pattern(pattern, text, 'exact'):
                logger.info(f"内置关键词匹配成功: {pattern}")
                try:
                    result = await handler(message)
                    return result
                except Exception as e:
                    logger.error(f"执行内置关键词处理函数失败: {e}", exc_info=True)
                    return None
        
        # 然后检查自定义关键词
        keywords = await self.db.get_keywords(group_id)
        logger.debug(f"群组 {group_id} 有 {len(keywords)} 个关键词")
        
        # 先精确匹配
        for keyword in keywords:
            if keyword.get('match_type', 'exact') == 'exact' and self._match_pattern(keyword['pattern'], text, 'exact'):
                logger.info(f"精确匹配关键词成功: {keyword['pattern']}")
                return str(keyword['_id'])
                
        # 再正则匹配
        for keyword in keywords:
            if keyword.get('match_type', 'exact') == 'regex' and self._match_pattern(keyword['pattern'], text, 'regex'):
                logger.info(f"正则匹配关键词成功: {keyword['pattern']}")
                return str(keyword['_id'])
                
        # 最后检查URL处理器
        has_url = bool(re.search(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', text))
        if has_url:
            for keyword in keywords:
                if keyword.get('is_url_handler', False):
                    logger.info(f"URL处理器匹配成功: {keyword['pattern']}")
                    return str(keyword['_id'])
                    
        return None
        
    def _match_pattern(self, pattern: str, text: str, match_type: str) -> bool:
        """
        匹配模式
        
        参数:
            pattern: 匹配模式
            text: 文本
            match_type: 匹配类型（'exact'或'regex'）
            
        返回:
            是否匹配
        """
        try:
            if match_type == 'exact':
                return pattern == text
            elif match_type == 'regex':
                return bool(re.search(pattern, text))
            return False
        except Exception as e:
            logger.error(f"匹配模式失败: {e}, pattern={pattern}, match_type={match_type}")
            return False
            
    async def add_keyword(self, keyword_data: Dict[str, Any]) -> bool:
        """
        添加关键词
        
        参数:
            keyword_data: 关键词数据
            
        返回:
            是否成功
        """
        try:
            await self.db.add_keyword(keyword_data)
            return True
        except Exception as e:
            logger.error(f"添加关键词失败: {e}", exc_info=True)
            return False
            
    async def remove_keyword(self, group_id: int, keyword_id: str) -> bool:
        """
        删除关键词
        
        参数:
            group_id: 群组ID
            keyword_id: 关键词ID
            
        返回:
            是否成功
        """
        try:
            await self.db.remove_keyword(group_id, keyword_id)
            return True
        except Exception as e:
            logger.error(f"删除关键词失败: {e}", exc_info=True)
            return False
            
    async def get_keyword_by_id(self, group_id: int, keyword_id: str) -> Optional[Dict[str, Any]]:
        """
        获取关键词
        
        参数:
            group_id: 群组ID
            keyword_id: 关键词ID
            
        返回:
            关键词数据或None
        """
        try:
            return await self.db.get_keyword_by_id(group_id, keyword_id)
        except Exception as e:
            logger.error(f"获取关键词失败: {e}", exc_info=True)
            return None
            
    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        """
        获取群组的所有关键词
        
        参数:
            group_id: 群组ID
            
        返回:
            关键词列表
        """
        try:
            return await self.db.get_keywords(group_id)
        except Exception as e:
            logger.error(f"获取关键词列表失败: {e}", exc_info=True)
            return []
