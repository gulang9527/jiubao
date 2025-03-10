"""
时间处理工具，提供时间相关的实用函数
"""
import logging
import pytz
from datetime import datetime, timedelta
from typing import Optional, Union, Tuple

logger = logging.getLogger(__name__)

def validate_time_format(time_str: str) -> Optional[datetime]:
    """
    验证时间格式并转换为datetime对象
    
    参数:
        time_str: 时间字符串，格式为 YYYY-MM-DD HH:MM
        
    返回:
        时区本地化的datetime对象，如果格式不正确则返回None
    """
    try:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
        import config
        tz = pytz.timezone(config.TIMEZONE_STR)
        return tz.localize(dt)
    except ValueError as e:
        logger.debug(f"时间格式验证失败: {e}")
        return None
    except Exception as e:
        logger.error(f"时间处理出错: {e}")
        return None

def validate_interval(interval_str: str) -> Optional[int]:
    """
    验证间隔时间格式
    
    参数:
        interval_str: 间隔时间字符串（表示秒数）
        
    返回:
        有效的间隔时间（秒），如果无效则返回None
    """
    try:
        interval = int(interval_str)
        if interval > 0:
            return interval
        logger.debug(f"间隔时间必须大于0: {interval_str}")
    except ValueError:
        logger.debug(f"无效的间隔时间格式: {interval_str}")
    return None

def format_datetime(dt: Optional[datetime]) -> str:
    """
    格式化日期时间为用户友好格式
    
    参数:
        dt: datetime对象
        
    返回:
        格式化后的日期时间字符串
    """
    if not dt:
        return "未设置"
    try:
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        logger.error(f"格式化日期时间出错: {e}")
        return str(dt)

def format_date(dt: Optional[datetime]) -> str:
    """
    格式化日期为用户友好格式
    
    参数:
        dt: datetime对象
        
    返回:
        格式化后的日期字符串
    """
    if not dt:
        return "未设置"
    try:
        return dt.strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"格式化日期出错: {e}")
        return str(dt)

def format_time(dt: Optional[datetime]) -> str:
    """
    格式化时间为用户友好格式
    
    参数:
        dt: datetime对象
        
    返回:
        格式化后的时间字符串
    """
    if not dt:
        return "未设置"
    try:
        return dt.strftime("%H:%M:%S")
    except Exception as e:
        logger.error(f"格式化时间出错: {e}")
        return str(dt)

def format_duration(seconds: int) -> str:
    """
    格式化时间间隔为用户友好的形式
    
    参数:
        seconds: 时间间隔（秒）
        
    返回:
        格式化后的时间间隔字符串
    """
    try:
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
    except Exception as e:
        logger.error(f"格式化时间间隔出错: {e}")
        return f"{seconds}秒"

def get_datetime_range(range_type: str) -> Tuple[datetime, datetime]:
    """
    获取日期时间范围
    
    参数:
        range_type: 范围类型（'today', 'yesterday', 'this_week', 'last_week', 
                           'this_month', 'last_month', 'last_30_days'）
        
    返回:
        (开始时间, 结束时间)
    """
    now = datetime.now()
    
    if range_type == 'today':
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    elif range_type == 'yesterday':
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
    elif range_type == 'this_week':
        # 周一为一周的开始
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    elif range_type == 'last_week':
        # 上周一到上周日
        this_week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        start = this_week_start - timedelta(days=7)
        end = this_week_start - timedelta(microseconds=1)
    elif range_type == 'this_month':
        # 本月1日到现在
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now
    elif range_type == 'last_month':
        # 上月1日到上月最后一天
        if now.month == 1:
            start = now.replace(year=now.year-1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            start = now.replace(month=now.month-1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
    elif range_type == 'last_30_days':
        # 过去30天
        start = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    else:
        # 默认今天
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
        
    return start, end

def get_next_occurrence(hour: int, minute: int = 0) -> datetime:
    """
    获取下一个指定时间的发生时间
    
    参数:
        hour: 小时（0-23）
        minute: 分钟（0-59）
        
    返回:
        下一次发生的时间
    """
    now = datetime.now()
    next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # 如果当前时间已经过了今天的指定时间，则获取明天的时间
    if now > next_time:
        next_time += timedelta(days=1)
        
    return next_time

def is_within_timeframe(dt: datetime, start_hour: int, end_hour: int) -> bool:
    """
    检查时间是否在指定时间范围内
    
    参数:
        dt: 要检查的时间
        start_hour: 开始小时（0-23）
        end_hour: 结束小时（0-23）
        
    返回:
        是否在范围内
    """
    hour = dt.hour
    
    if start_hour <= end_hour:
        # 简单情况: 例如 9:00 - 17:00
        return start_hour <= hour < end_hour
    else:
        # 跨天情况: 例如 22:00 - 6:00
        return hour >= start_hour or hour < end_hour

def get_local_time(tzname: str = None) -> datetime:
    """
    获取指定时区的当前时间
    
    参数:
        tzname: 时区名称，默认使用配置中的时区
        
    返回:
        本地时间
    """
    try:
        if not tzname:
            import config
            tzname = config.TIMEZONE_STR
            
        tz = pytz.timezone(tzname)
        return datetime.now(tz)
    except Exception as e:
        logger.error(f"获取本地时间出错: {e}")
        return datetime.now()

def parse_date_string(date_str: str) -> Optional[datetime]:
    """
    解析日期字符串
    
    支持的格式:
    - YYYY-MM-DD
    - YYYY/MM/DD
    - DD.MM.YYYY
    - MM/DD/YYYY
    
    参数:
        date_str: 日期字符串
        
    返回:
        日期对象或None
    """
    formats = [
        "%Y-%m-%d",  # YYYY-MM-DD
        "%Y/%m/%d",  # YYYY/MM/DD
        "%d.%m.%Y",  # DD.MM.YYYY
        "%m/%d/%Y"   # MM/DD/YYYY
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
            
    logger.debug(f"无法解析日期字符串: {date_str}")
    return None

def parse_time_string(time_str: str) -> Optional[Tuple[int, int]]:
    """
    解析时间字符串
    
    支持的格式:
    - HH:MM
    - HH:MM:SS
    - HH点MM分
    
    参数:
        time_str: 时间字符串
        
    返回:
        (小时, 分钟)或None
    """
    try:
        # 尝试 HH:MM 或 HH:MM:SS 格式
        if ':' in time_str:
            parts = time_str.split(':')
            if len(parts) >= 2:
                hour = int(parts[0])
                minute = int(parts[1])
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    return hour, minute
                    
        # 尝试 HH点MM分 格式
        if '点' in time_str and '分' in time_str:
            hour_part = time_str.split('点')[0]
            minute_part = time_str.split('点')[1].split('分')[0]
            hour = int(hour_part)
            minute = int(minute_part) if minute_part else 0
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return hour, minute
                
    except (ValueError, IndexError):
        pass
        
    logger.debug(f"无法解析时间字符串: {time_str}")
    return None

def parse_datetime_string(datetime_str: str) -> Optional[datetime]:
    """
    解析日期时间字符串
    
    支持的格式:
    - YYYY-MM-DD HH:MM
    - YYYY/MM/DD HH:MM
    - YYYY-MM-DD HH:MM:SS
    - YYYY/MM/DD HH:MM:SS
    
    参数:
        datetime_str: 日期时间字符串
        
    返回:
        日期时间对象或None
    """
    formats = [
        "%Y-%m-%d %H:%M",      # YYYY-MM-DD HH:MM
        "%Y/%m/%d %H:%M",      # YYYY/MM/DD HH:MM
        "%Y-%m-%d %H:%M:%S",   # YYYY-MM-DD HH:MM:SS
        "%Y/%m/%d %H:%M:%S"    # YYYY/MM/DD HH:MM:SS
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
            
    logger.debug(f"无法解析日期时间字符串: {datetime_str}")
    return None

def get_date_string(dt: Optional[datetime] = None, fmt: str = "%Y-%m-%d") -> str:
    """
    获取日期字符串
    
    参数:
        dt: 日期时间对象，默认为当前日期
        fmt: 格式字符串
        
    返回:
        日期字符串
    """
    if not dt:
        dt = datetime.now()
        
    try:
        return dt.strftime(fmt)
    except Exception as e:
        logger.error(f"获取日期字符串出错: {e}")
        return ""
