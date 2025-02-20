import logging
from typing import Dict, Any
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from utils import validate_settings, format_error_message
from db import UserRole

logger = logging.getLogger(__name__)

class SettingsManager:
    def __init__(self, db):
        self.db = db
        self._temp_settings = {}
        self._pages = {}
        
    def get_current_page(self, group_id: int, section: str) -> int:
        """获取当前页码"""
        key = f"{group_id}_{section}"
        return self._pages.get(key, 1)
        
    def set_current_page(self, group_id: int, section: str, page: int):
        """设置当前页码"""
        key = f"{group_id}_{section}"
        self._pages[key] = page
        
    def start_setting(self, user_id: int, setting_type: str, group_id: int):
        """开始设置过程"""
        key = f"{user_id}_{setting_type}"
        self._temp_settings[key] = {
            'group_id': group_id,
            'step': 1,
            'data': {}
        }
        
    def get_setting_state(self, user_id: int, setting_type: str) -> Dict[str, Any]:
        """获取设置状态"""
        key = f"{user_id}_{setting_type}"
        return self._temp_settings.get(key, {})
        
    def update_setting_state(self, user_id: int, setting_type: str, data: Dict[str, Any]):
        """更新设置状态"""
        key = f"{user_id}_{setting_type}"
        if key in self._temp_settings:
            self._temp_settings[key]['data'].update(data)
            self._temp_settings[key]['step'] += 1
            
    def clear_setting_state(self, user_id: int, setting_type: str):
        """清除设置状态"""
        key = f"{user_id}_{setting_type}"
        if key in self._temp_settings:
            del self._temp_settings[key]

    async def show_group_selection(self, update: Update, context):
        """显示群组选择界面"""
        try:
            # 获取用户可管理的群组列表
            user_groups = await self.db.get_manageable_groups(update.effective_user.id)
            
            if not user_groups:
                await update.message.reply_text("❌ 没有可管理的群组")
                return
                
            # 创建群组选择键盘
            keyboard = []
            for group in user_groups:
                try:
                    group_info = await context.bot.get_chat(group['group_id'])
                    button_text = group_info.title or f"群组 {group['group_id']}"
                    keyboard.append([
                        InlineKeyboardButton(
                            button_text,
                            callback_data=f"settings_select_{group['group_id']}"
                        )
                    ])
                except Exception as e:
                    logger.error(f"Error getting group info: {e}")
                    continue
            
            await update.message.reply_text(
                "⚙️ 机器人设置\n"
                "请选择要管理的群组：",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error showing group selection: {e}")
            raise

    async def show_settings_menu(self, update: Update, context, group_id: int):
        """显示设置菜单"""
        try:
            query = update.callback_query
            
            keyboard = [
                [
                    InlineKeyboardButton(
                        "关键词管理",
                        callback_data=f"settings_keywords_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "轮播设置", 
                        callback_data=f"settings_broadcast_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "统计设置",
                        callback_data=f"settings_stats_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "返回群组选择",
                        callback_data="settings_groups"
                    )
                ]
            ]
            
            try:
                group_info = await context.bot.get_chat(group_id)
                group_name = group_info.title or f"群组 {group_id}"
            except Exception:
                group_name = f"群组 {group_id}"
            
            await query.edit_message_text(
                f"⚙️ {group_name} 的设置\n"
                "请选择要修改的设置项：",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error showing settings menu: {e}")
            raise

    async def handle_callback(self, update: Update, context):
        """处理设置回调"""
        query = update.callback_query
        data = query.data
        parts = data.split('_')
        action = parts[1]
        
        try:
            if action == "select":
                # 处理群组选择
                group_id = int(parts[2])
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                await self.show_settings_menu(update, context, group_id)
                
            elif action == "groups":
                # 返回群组选择界面
                await self.show_group_selection(update, context)
                
            elif action in ["keywords", "broadcast", "stats"]:
                # 处理具体设置项
                group_id = int(parts[2])
                # 委托给对应的管理器处理
                if action == "keywords":
                    await context.bot.keyword_manager.show_settings(update, context, group_id)
                elif action == "broadcast":
                    await context.bot.broadcast_manager.show_settings(update, context, group_id)
                elif action == "stats":
                    await context.bot.stats_manager.show_settings(update, context, group_id)
                
        except Exception as e:
            logger.error(f"Error handling settings callback: {e}")
            await query.edit_message_text(format_error_message(e))