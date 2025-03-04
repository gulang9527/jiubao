from datetime import datetime
from typing import Optional, Tuple, Dict, Any
import re
import pytz
import logging
from config import TIMEZONE
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

def validate_time_format(time_str: str) -> Optional[datetime]:
    """验证时间格式并转换为datetime对象"""
    try:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
        tz = pytz.timezone(TIMEZONE)
        return tz.localize(dt)
    except ValueError:
        return None

def validate_interval(interval_str: str) -> Optional[int]:
    """验证间隔时间格式"""
    try:
        interval = int(interval_str)
        if interval > 0:
            return interval
    except ValueError:
        pass
    return None

def format_file_size(size_bytes: int) -> str:
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

def validate_regex(pattern: str) -> bool:
    """验证正则表达式是否有效"""
    try:
        re.compile(pattern)
        return True
    except re.error:
        return False

def get_media_type(message) -> Optional[str]:
    """获取消息的媒体类型"""
    if message.photo:
        return 'photo'
    elif message.video:
        return 'video'
    elif message.document:
        return 'document'
    elif message.animation:
        return 'animation'
    elif message.audio:
        return 'audio'
    elif message.voice:
        return 'voice'
    elif message.video_note:
        return 'video_note'
    elif message.sticker:
        return 'sticker'
    return None

def format_duration(seconds: int) -> str:
    """格式化时间间隔"""
    if seconds < 60:
        return f"{seconds}秒"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}分钟"
    hours = minutes // 60
    minutes = minutes % 60
    if hours < 24:
        return f"{hours}小时{minutes}分钟" if minutes else f"{hours}小时"
    days = hours // 24
    hours = hours % 24
    if days < 30:
        return f"{days}天{hours}小时" if hours else f"{days}天"
    months = days // 30
    days = days % 30
    return f"{months}月{days}天" if days else f"{months}月"

def parse_command_args(text: str) -> Tuple[str, list]:
    """解析命令参数"""
    parts = text.split()
    command = parts[0].split('@')[0][1:]  # 移除 / 和机器人用户名
    args = parts[1:] if len(parts) > 1 else []
    return command, args

def escape_markdown(text: str) -> str:
    """转义Markdown特殊字符"""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{c}' if c in escape_chars else c for c in text)

def validate_settings(settings: Dict[str, Any]) -> Tuple[bool, str]:
    """验证设置是否有效"""
    from config import DEFAULT_SETTINGS
    
    try:
        # 验证最低字节数
        min_bytes = settings.get('min_bytes', DEFAULT_SETTINGS['min_bytes'])
        if not isinstance(min_bytes, int) or min_bytes < 0:
            return False, "最低字节数必须是非负整数"
            
        # 验证排行显示数量
        daily_rank_size = settings.get('daily_rank_size', DEFAULT_SETTINGS['daily_rank_size'])
        monthly_rank_size = settings.get('monthly_rank_size', DEFAULT_SETTINGS['monthly_rank_size'])
        if not isinstance(daily_rank_size, int) or daily_rank_size < 1:
            return False, "日排行显示数量必须是正整数"
        if not isinstance(monthly_rank_size, int) or monthly_rank_size < 1:
            return False, "月排行显示数量必须是正整数"
            
        # 验证其他布尔值设置
        count_media = settings.get('count_media', DEFAULT_SETTINGS['count_media'])
        if not isinstance(count_media, bool):
            return False, "count_media必须是布尔值"
            
        return True, "设置有效"
    except Exception as e:
        return False, f"设置验证出错：{str(e)}"

def format_error_message(error: Exception) -> str:
    """格式化错误消息"""
    error_type = type(error).__name__
    error_message = str(error)
    return f"❌ {error_type}: {error_message}"

def validate_delete_timeout(timeout: Optional[int] = None, message_type: Optional[str] = None) -> int:
    """
    验证并返回有效的删除超时时间
    :param timeout: 超时时间（秒）
    :param message_type: 消息类型，用于差异化超时
    :return: 有效的超时时间
    """
    from config import AUTO_DELETE_SETTINGS
    
    if not AUTO_DELETE_SETTINGS.get('enabled', False):
        return 0
    
    if timeout is None:
        timeouts = AUTO_DELETE_SETTINGS['timeouts']
        timeout = timeouts.get(message_type, timeouts['default']) if message_type else timeouts['default']
    
    timeout = max(AUTO_DELETE_SETTINGS['min_timeout'], min(timeout, AUTO_DELETE_SETTINGS['max_timeout']))
    return timeout

def is_auto_delete_exempt(user_role: str, command: Optional[str] = None) -> bool:
    """
    检查用户是否免除自动删除
    
    :param user_role: 用户角色
    :param command: 命令（可选）
    :return: 是否免除自动删除
    """
    from config import AUTO_DELETE_SETTINGS
    
    # 检查用户角色
    if user_role in AUTO_DELETE_SETTINGS.get('exempt_roles', []):
        return True
    
    # 检查命令前缀
    if command and any(
        command.startswith(prefix) 
        for prefix in AUTO_DELETE_SETTINGS.get('exempt_command_prefixes', [])
    ):
        return True
    
    return False

def get_message_metadata(message) -> Dict[str, Any]:
    """
    获取消息的元数据，用于自动删除判断
    
    :param message: 消息对象
    :return: 消息元数据字典
    """
    metadata = {
        'type': get_media_type(message) or 'text',
        'length': len(message.text or '') if message.text else 0,
        'contains_media': bool(message.photo or message.video or message.document)
    }
    return metadata

class CallbackDataBuilder:
    """回调数据构建器"""
    @staticmethod
    def build(*args) -> str:
        """
        构建回调数据
        
        参数:
        *args: 要拼接的回调数据部分
        
        返回:
        拼接后的回调数据字符串
        """
        return '_'.join(str(arg) for arg in args)
        
    @staticmethod
    def parse(data: str) -> list:
        """
        解析回调数据
        
        参数:
        data: 回调数据字符串
        
        返回:
        回调数据各部分组成的列表
        """
        return data.split('_')
        
    @staticmethod
    def get_action(data: str) -> str:
        """
        获取回调数据中的操作类型
        
        参数:
        data: 回调数据字符串
        
        返回:
        操作类型字符串
        """
        parts = data.split('_')
        if len(parts) >= 2:
            return parts[1]
        return ""
        
    @staticmethod
    def get_group_id(data: str) -> int:
        """
        获取回调数据中的群组ID
        
        参数:
        data: 回调数据字符串
        
        返回:
        群组ID (int)
        """
        parts = data.split('_')
        if len(parts) >= 3:
            try:
                return int(parts[-1])
            except ValueError:
                pass
        return 0

class KeyboardBuilder:
    """键盘构建器"""
    @staticmethod
    def build(buttons, rows=None):
        """
        构建内联键盘布局
        
        参数:
        buttons: 按钮列表或按钮行的列表
        rows: 每行按钮数量，如果不指定则每个按钮独占一行
        
        返回:
        InlineKeyboardMarkup对象
        """
        keyboard = []
        
        if not buttons:
            return InlineKeyboardMarkup(keyboard)
            
        # 如果传入的是已经分好行的按钮
        if isinstance(buttons[0], list):
            return InlineKeyboardMarkup(buttons)
            
        # 按照rows指定的数量分行
        if rows:
            for i in range(0, len(buttons), rows):
                row = buttons[i:i+rows]
                keyboard.append(row)
        else:
            # 默认每个按钮一行
            keyboard = [[button] for button in buttons]
            
        return InlineKeyboardMarkup(keyboard)
            
    @staticmethod
    def create_settings_keyboard(group_id: int, permissions: list) -> InlineKeyboardMarkup:
        """创建设置菜单键盘"""
        keyboard = []
        
        if 'stats' in permissions:
            keyboard.append([
                InlineKeyboardButton(
                    "📊 统计设置", 
                    callback_data=f"settings_stats_{group_id}"
                )
            ])
        
        if 'broadcast' in permissions:
            keyboard.append([
                InlineKeyboardButton(
                    "📢 轮播消息", 
                    callback_data=f"settings_broadcast_{group_id}"
                )
            ])
        
        if 'keywords' in permissions:
            keyboard.append([
                InlineKeyboardButton(
                    "🔑 关键词设置", 
                    callback_data=f"settings_keywords_{group_id}"
                )
            ])
            
        keyboard.append([
            InlineKeyboardButton(
                "🔙 返回群组列表", 
                callback_data="show_manageable_groups"
            )
        ])
        
        return InlineKeyboardMarkup(keyboard)
