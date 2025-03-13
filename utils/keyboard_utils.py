"""
键盘生成工具，提供创建不同类型的键盘布局的实用函数
"""
import logging
from typing import List, Dict, Any, Optional, Union, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)

class KeyboardBuilder:
    """键盘构建器，简化键盘布局创建"""
    
    @staticmethod
    def build_menu(
        buttons: List[InlineKeyboardButton],
        n_cols: int = 1,
        header_buttons: Optional[List[InlineKeyboardButton]] = None,
        footer_buttons: Optional[List[InlineKeyboardButton]] = None
    ) -> List[List[InlineKeyboardButton]]:
        """
        创建按钮菜单
        
        参数:
            buttons: 按钮列表
            n_cols: 每行的按钮数
            header_buttons: 头部按钮列表（每个按钮独占一行）
            footer_buttons: 底部按钮列表（每个按钮独占一行）
            
        返回:
            键盘布局
        """
        menu = []
        
        # 添加头部按钮
        if header_buttons:
            for button in header_buttons:
                menu.append([button])
        
        # 添加主体按钮
        for i in range(0, len(buttons), n_cols):
            row = buttons[i:i+n_cols]
            menu.append(row)
        
        # 添加底部按钮
        if footer_buttons:
            for button in footer_buttons:
                menu.append([button])
                
        return menu
    
    @staticmethod
    def create_button(text: str, callback_data: str) -> InlineKeyboardButton:
        """
        创建回调按钮
        
        参数:
            text: 按钮文本
            callback_data: 回调数据
            
        返回:
            按钮对象
        """
        return InlineKeyboardButton(text, callback_data=callback_data)
    
    @staticmethod
    def create_url_button(text: str, url: str) -> InlineKeyboardButton:
        """
        创建URL按钮
        
        参数:
            text: 按钮文本
            url: 网址
            
        返回:
            按钮对象
        """
        return InlineKeyboardButton(text, url=url)
    
    @staticmethod
    def create_keyboard(
        buttons: List[List[InlineKeyboardButton]]
    ) -> InlineKeyboardMarkup:
        """
        创建内联键盘
        
        参数:
            buttons: 按钮布局
            
        返回:
            内联键盘标记
        """
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def create_simple_keyboard(
        buttons: List[Tuple[str, str]], 
        n_cols: int = 1
    ) -> InlineKeyboardMarkup:
        """
        创建简单的内联键盘
        
        参数:
            buttons: 按钮列表，每个元素是 (文本, 回调数据) 元组
            n_cols: 每行的按钮数
            
        返回:
            内联键盘标记
        """
        keyboard_buttons = [
            InlineKeyboardButton(text, callback_data=callback_data)
            for text, callback_data in buttons
        ]
        return InlineKeyboardMarkup(
            KeyboardBuilder.build_menu(keyboard_buttons, n_cols=n_cols)
        )
    
    @classmethod
    def create_settings_keyboard(
        cls, 
        group_id: int, 
        permissions: List[str]
    ) -> InlineKeyboardMarkup:
        """
        创建设置菜单键盘
        
        参数:
            group_id: 群组ID
            permissions: 权限列表
            
        返回:
            内联键盘标记
        """
        buttons = []
        
        # 添加功能按钮
        if 'stats' in permissions:
            buttons.append((
                "📊 统计设置", 
                f"settings_stats_{group_id}"
            ))
        
        if 'broadcast' in permissions:
            buttons.append((
                "📢 轮播消息", 
                f"settings_broadcast_{group_id}"
            ))
        
        if 'keywords' in permissions:
            buttons.append((
                "🔑 关键词设置", 
                f"settings_keywords_{group_id}"
            ))
            
        # 添加开关设置按钮
        buttons.append((
            "⚙️ 开关设置", 
            f"settings_switches_{group_id}"
        ))
        
        # 添加自动删除设置按钮
        buttons.append((
            "🗑️ 自动删除设置", 
            f"auto_delete_settings_{group_id}"
        ))
            
        # 添加返回按钮
        buttons.append((
            "🔙 返回群组列表", 
            "show_manageable_groups"
        ))
        
        return cls.create_simple_keyboard(buttons, n_cols=2)
    
    @classmethod
    def create_paginated_keyboard(
        cls,
        items: List[Tuple[str, str]],
        page: int,
        total_pages: int,
        prefix: str,
        suffix: str = "",
        n_cols: int = 1
    ) -> InlineKeyboardMarkup:
        """
        创建分页键盘
        
        参数:
            items: 当前页的项目，每个元素是 (文本, 回调数据) 元组
            page: 当前页码
            total_pages: 总页数
            prefix: 分页回调数据前缀
            suffix: 分页回调数据后缀
            n_cols: 每行的按钮数
            
        返回:
            内联键盘标记
        """
        # 构建项目按钮
        keyboard_buttons = [
            InlineKeyboardButton(text, callback_data=callback_data)
            for text, callback_data in items
        ]
        
        # 构建分页导航按钮
        nav_buttons = []
        if page > 1:
            nav_buttons.append(
                InlineKeyboardButton(
                    "◀️ 上一页", 
                    callback_data=f"{prefix}_page_{page-1}{suffix}"
                )
            )
        
        if page < total_pages:
            nav_buttons.append(
                InlineKeyboardButton(
                    "下一页 ▶️", 
                    callback_data=f"{prefix}_page_{page+1}{suffix}"
                )
            )
        
        # 构建完整键盘
        keyboard = cls.build_menu(keyboard_buttons, n_cols=n_cols)
        
        # 添加分页导航按钮
        if nav_buttons:
            keyboard.append(nav_buttons)
            
        # 添加返回按钮
        keyboard.append([
            InlineKeyboardButton("🔙 返回", callback_data=f"{prefix}_back{suffix}")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    @classmethod
    def create_confirm_keyboard(
        cls,
        confirm_data: str,
        cancel_data: str,
        confirm_text: str = "✅ 确认",
        cancel_text: str = "❌ 取消"
    ) -> InlineKeyboardMarkup:
        """
        创建确认键盘
        
        参数:
            confirm_data: 确认按钮回调数据
            cancel_data: 取消按钮回调数据
            confirm_text: 确认按钮文本
            cancel_text: 取消按钮文本
            
        返回:
            内联键盘标记
        """
        buttons = [
            [
                InlineKeyboardButton(confirm_text, callback_data=confirm_data),
                InlineKeyboardButton(cancel_text, callback_data=cancel_data)
            ]
        ]
        
        return InlineKeyboardMarkup(buttons)
    
    @classmethod
    def create_options_keyboard(
        cls,
        options: List[Tuple[str, str]],
        cancel_data: str,
        cancel_text: str = "❌ 取消",
        n_cols: int = 2
    ) -> InlineKeyboardMarkup:
        """
        创建选项键盘
        
        参数:
            options: 选项列表，每个元素是 (文本, 回调数据) 元组
            cancel_data: 取消按钮回调数据
            cancel_text: 取消按钮文本
            n_cols: 每行的按钮数
            
        返回:
            内联键盘标记
        """
        keyboard_buttons = [
            InlineKeyboardButton(text, callback_data=callback_data)
            for text, callback_data in options
        ]
        
        footer_buttons = [InlineKeyboardButton(cancel_text, callback_data=cancel_data)]
        
        return InlineKeyboardMarkup(
            cls.build_menu(
                keyboard_buttons, 
                n_cols=n_cols, 
                footer_buttons=footer_buttons
            )
        )

class CallbackDataBuilder:
    """回调数据构建器，简化回调数据生成"""
    
    @staticmethod
    def build(*parts):
        """
        构建回调数据
        
        参数:
            *parts: 回调数据部分
            
        返回:
            回调数据字符串
        """
        return '_'.join(str(part) for part in parts)
    
    @staticmethod
    def parse(data: str) -> List[str]:
        """
        解析回调数据
        
        参数:
            data: 回调数据字符串
            
        返回:
            回调数据部分列表
        """
        return data.split('_')
    
    @staticmethod
    def get_action(data: str) -> str:
        """
        获取回调数据中的操作
        
        参数:
            data: 回调数据字符串
            
        返回:
            操作名称
        """
        parts = data.split('_')
        if len(parts) >= 2:
            return parts[1]
        return ""
    
    @staticmethod
    def get_group_id(data: str) -> Optional[int]:
        """
        获取回调数据中的群组ID
        
        参数:
            data: 回调数据字符串
            
        返回:
            群组ID或None
        """
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                return int(parts[-1])
            except ValueError:
                pass
        return None
