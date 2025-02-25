import re
from typing import List, Dict, Any, Optional
from db import Database

class KeywordManager:
    """关键词管理类"""
    def __init__(self, db):
        self.db = db
        self._built_in_keywords = {}

    def register_built_in_keyword(self, pattern: str, handler: callable):
        """注册内置关键词"""
        self._built_in_keywords[pattern] = handler

    async def match_keyword(self, group_id: int, text: str, message: Any) -> Optional[str]:
        """匹配关键词并返回回复"""
        # 匹配内置关键词
        for pattern, handler in self._built_in_keywords.items():
            if text == pattern:
                return await handler(message)

        # 匹配自定义关键词
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            try:
                if kw['type'] == 'regex':
                    if re.search(kw['pattern'], text):
                        return self._format_response(kw)
                else:
                    if text == kw['pattern']:
                        return self._format_response(kw)
            except Exception as e:
                print(f"Error matching keyword {kw['pattern']}: {e}")
                continue

        return None

    def _format_response(self, keyword: Dict[str, Any]) -> str:
        """格式化关键词回复"""
        if keyword['response_type'] == 'text':
            return keyword['response']
        elif keyword['response_type'] in ['photo', 'video', 'document']:
            return f"__media__{keyword['response_type']}__{keyword['response']}"
        else:
            return "❌ 不支持的回复类型"

    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        """获取群组的关键词列表"""
        return await self.db.get_keywords(group_id)

    async def get_keyword_by_id(self, group_id: int, keyword_id: str) -> Optional[Dict[str, Any]]:
        """通过ID获取关键词"""
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            if str(kw['_id']) == keyword_id:
                return kw
        return None