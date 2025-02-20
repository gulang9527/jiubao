from datetime import datetime
from typing import Optional, Tuple, Dict, Any
import re
import pytz
from config import TIMEZONE

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