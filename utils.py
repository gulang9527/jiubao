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

def validate_delete_timeout(
    timeout: Optional[int] = None, 
    message_type: Optional[str] = None
) -> int:
    """
    验证并返回有效的删除超时时间
    
    :param timeout: 超时时间（秒）
    :param message_type: 消息类型，可用于差异化超时
    :return: 有效的超时时间
    """
    from config import AUTO_DELETE_SETTINGS
    
    # 如果未启用自动删除，返回0（不删除）
    if not AUTO_DELETE_SETTINGS.get('enabled', False):
        return 0
    
    # 如果未提供超时时间，返回默认值
    if timeout is None:
        # 可以根据消息类型设置不同的默认超时
        type_timeouts = {
            'text': AUTO_DELETE_SETTINGS['default_timeout'],
            'photo': AUTO_DELETE_SETTINGS['default_timeout'] * 2,
            'video': AUTO_DELETE_SETTINGS['default_timeout'] * 3,
            'document': AUTO_DELETE_SETTINGS['default_timeout'] * 1.5
        }
        timeout = type_timeouts.get(
            message_type, 
            AUTO_DELETE_SETTINGS['default_timeout']
        )
    
    # 检查超时时间是否在允许范围内
    timeout = max(
        AUTO_DELETE_SETTINGS['min_timeout'], 
        min(timeout, AUTO_DELETE_SETTINGS['max_timeout'])
    )
    
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
    def build(action: str, *args) -> str:
        """构建回调数据"""
        return '_'.join([str(arg) for arg in [action, *args]])
    
    @staticmethod
    def parse(data: str) -> tuple:
        """解析回调数据"""
        parts = data.split('_')
        if len(parts) < 2:
            raise ValueError("Invalid callback data format")
        return parts[0], parts[1], parts[2:]

class KeyboardBuilder:
    """键盘构建器"""
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
