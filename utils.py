import re
import logging
import pytz
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List, Union

from telegram import Message, InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)

#######################################
# 时间和日期处理函数
#######################################

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

#######################################
# 文件处理函数
#######################################

def format_file_size(size_bytes: int) -> str:
    """
    格式化文件大小为可读形式
    
    参数:
        size_bytes: 文件大小（字节）
        
    返回:
        格式化后的文件大小字符串
    """
    try:
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0
        size = float(size_bytes)
        
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
            
        return f"{size:.2f} {units[unit_index]}"
    except Exception as e:
        logger.error(f"格式化文件大小出错: {e}")
        return f"{size_bytes} B"

def validate_regex(pattern: str) -> bool:
    """
    验证正则表达式是否有效
    
    参数:
        pattern: 正则表达式模式
        
    返回:
        正则表达式是否有效
    """
    try:
        re.compile(pattern)
        return True
    except re.error as e:
        logger.debug(f"无效的正则表达式: {pattern}, 错误: {e}")
        return False
    except Exception as e:
        logger.error(f"验证正则表达式出错: {e}")
        return False

#######################################
# 消息处理函数
#######################################

def get_media_type(message: Message) -> Optional[str]:
    """
    获取消息的媒体类型
    
    参数:
        message: 消息对象
        
    返回:
        媒体类型字符串，如果没有媒体则返回None
    """
    if not message:
        return None
        
    try:
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
    except Exception as e:
        logger.error(f"获取媒体类型出错: {e}")
        return None

def get_file_id(message: Message) -> Optional[str]:
    """
    获取消息中媒体的file_id
    
    参数:
        message: 消息对象
        
    返回:
        媒体的file_id，如果没有媒体则返回None
    """
    if not message:
        return None
        
    try:
        if message.photo:
            # 照片是一个数组，取最后一个（最大尺寸）
            return message.photo[-1].file_id
        elif message.video:
            return message.video.file_id
        elif message.document:
            return message.document.file_id
        elif message.animation:
            return message.animation.file_id
        elif message.audio:
            return message.audio.file_id
        elif message.voice:
            return message.voice.file_id
        elif message.video_note:
            return message.video_note.file_id
        elif message.sticker:
            return message.sticker.file_id
        return None
    except Exception as e:
        logger.error(f"获取文件ID出错: {e}")
        return None

def get_message_metadata(message: Message) -> Dict[str, Any]:
    """
    获取消息的元数据，用于自动删除判断
    
    参数:
        message: 消息对象
        
    返回:
        消息元数据字典
    """
    if not message:
        return {'type': 'unknown', 'length': 0, 'contains_media': False}
        
    try:
        metadata = {
            'type': get_media_type(message) or 'text',
            'length': len(message.text or '') if message.text else 0,
            'contains_media': bool(message.photo or message.video or message.document or message.animation or 
                                 message.audio or message.voice or message.video_note or message.sticker)
        }
        return metadata
    except Exception as e:
        logger.error(f"获取消息元数据出错: {e}")
        return {'type': 'unknown', 'length': 0, 'contains_media': False}

def parse_command_args(text: str) -> Tuple[str, List[str]]:
    """
    解析命令参数
    
    参数:
        text: 完整命令文本
        
    返回:
        (命令名, 参数列表)
    """
    if not text:
        return "", []
        
    try:
        parts = text.split()
        command = parts[0].split('@')[0][1:] if parts[0].startswith('/') else ""
        args = parts[1:] if len(parts) > 1 else []
        return command, args
    except Exception as e:
        logger.error(f"解析命令参数出错: {e}")
        return "", []

def escape_markdown(text: str) -> str:
    """
    转义Markdown特殊字符
    
    参数:
        text: 要转义的文本
        
    返回:
        转义后的文本
    """
    if not text:
        return ""
        
    try:
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        return ''.join(f'\\{c}' if c in escape_chars else c for c in text)
    except Exception as e:
        logger.error(f"转义Markdown字符出错: {e}")
        return text

def format_error_message(error: Exception) -> str:
    """
    格式化错误消息
    
    参数:
        error: 异常对象
        
    返回:
        格式化后的错误消息
    """
    if not error:
        return "❌ 未知错误"
        
    try:
        error_type = type(error).__name__
        error_message = str(error)
        return f"❌ {error_type}: {error_message}"
    except Exception as e:
        logger.error(f"格式化错误消息出错: {e}")
        return "❌ 未知错误"

#######################################
# 设置与配置函数
#######################################

def validate_settings(settings: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证设置是否有效
    
    参数:
        settings: 设置字典
        
    返回:
        (是否有效, 错误消息)
    """
    try:
        import config
        
        # 验证最低字节数
        min_bytes = settings.get('min_bytes', config.DEFAULT_SETTINGS['min_bytes'])
        if not isinstance(min_bytes, int) or min_bytes < 0:
            return False, "最低字节数必须是非负整数"
            
        # 验证排行显示数量
        daily_rank_size = settings.get('daily_rank_size', config.DEFAULT_SETTINGS['daily_rank_size'])
        monthly_rank_size = settings.get('monthly_rank_size', config.DEFAULT_SETTINGS['monthly_rank_size'])
        if not isinstance(daily_rank_size, int) or daily_rank_size < 1:
            return False, "日排行显示数量必须是正整数"
        if not isinstance(monthly_rank_size, int) or monthly_rank_size < 1:
            return False, "月排行显示数量必须是正整数"
            
        # 验证其他布尔值设置
        count_media = settings.get('count_media', config.DEFAULT_SETTINGS['count_media'])
        if not isinstance(count_media, bool):
            return False, "count_media必须是布尔值"
            
        # 验证自动删除设置
        auto_delete = settings.get('auto_delete', False)
        if not isinstance(auto_delete, bool):
            return False, "auto_delete必须是布尔值"
            
        # 验证自动删除超时
        auto_delete_timeout = settings.get('auto_delete_timeout', config.AUTO_DELETE_SETTINGS['default_timeout'])
        if not isinstance(auto_delete_timeout, int) or auto_delete_timeout < 0:
            return False, "auto_delete_timeout必须是非负整数"
            
        return True, "设置有效"
    except Exception as e:
        logger.error(f"验证设置出错: {e}", exc_info=True)
        return False, f"设置验证出错：{str(e)}"

def validate_delete_timeout(timeout: Optional[int] = None, message_type: Optional[str] = None) -> int:
    """
    验证并返回有效的删除超时时间
    
    参数:
        timeout: 超时时间（秒）
        message_type: 消息类型，用于差异化超时
        
    返回:
        有效的超时时间
    """
    try:
        import config
        
        # 检查是否启用自动删除
        if not config.AUTO_DELETE_SETTINGS.get('enabled', False):
            return 0
        
        # 获取超时时间
        if timeout is None:
            timeouts = config.AUTO_DELETE_SETTINGS['timeouts']
            timeout = timeouts.get(message_type, timeouts['default']) if message_type else timeouts['default']
        
        # 确保超时时间在有效范围内
        min_timeout = config.AUTO_DELETE_SETTINGS['min_timeout']
        max_timeout = config.AUTO_DELETE_SETTINGS['max_timeout']
        return max(min_timeout, min(timeout, max_timeout))
    except Exception as e:
        logger.error(f"验证删除超时出错: {e}")
        # 返回默认值
        return 300  # 默认5分钟

def is_auto_delete_exempt(user_role: str, command: Optional[str] = None) -> bool:
    """
    检查用户是否免除自动删除
    
    参数:
        user_role: 用户角色
        command: 命令（可选）
        
    返回:
        是否免除自动删除
    """
    try:
        import config
        
        # 检查用户角色
        if user_role in config.AUTO_DELETE_SETTINGS.get('exempt_roles', []):
            return True
        
        # 检查命令前缀
        if command and any(
            command.startswith(prefix) 
            for prefix in config.AUTO_DELETE_SETTINGS.get('exempt_command_prefixes', [])
        ):
            return True
        
        return False
    except Exception as e:
        logger.error(f"检查自动删除豁免出错: {e}")
        return False

#######################################
# 键盘与界面函数
#######################################

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
        try:
            return '_'.join(str(arg) for arg in args)
        except Exception as e:
            logger.error(f"构建回调数据出错: {e}")
            return "error"
        
    @staticmethod
    def parse(data: str) -> List[str]:
        """
        解析回调数据
        
        参数:
            data: 回调数据字符串
        
        返回:
            回调数据各部分组成的列表
        """
        try:
            if not data:
                return []
            return data.split('_')
        except Exception as e:
            logger.error(f"解析回调数据出错: {e}")
            return []
        
    @staticmethod
    def get_action(data: str) -> str:
        """
        获取回调数据中的操作类型
        
        参数:
            data: 回调数据字符串
        
        返回:
            操作类型字符串
        """
        try:
            parts = data.split('_')
            if len(parts) >= 2:
                return parts[1]
            return ""
        except Exception as e:
            logger.error(f"获取回调操作类型出错: {e}")
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
        try:
            parts = data.split('_')
            if len(parts) >= 3:
                try:
                    return int(parts[-1])
                except ValueError:
                    logger.debug(f"回调数据最后一部分不是有效的群组ID: {parts[-1]}")
            return 0
        except Exception as e:
            logger.error(f"获取回调群组ID出错: {e}")
            return 0

class KeyboardBuilder:
    """键盘构建器"""
    
    @staticmethod
    def build(buttons: Union[List[InlineKeyboardButton], List[List[InlineKeyboardButton]]], 
              rows: Optional[int] = None) -> InlineKeyboardMarkup:
        """
        构建内联键盘布局
        
        参数:
            buttons: 按钮列表或按钮行的列表
            rows: 每行按钮数量，如果不指定则每个按钮独占一行
        
        返回:
            InlineKeyboardMarkup对象
        """
        try:
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
        except Exception as e:
            logger.error(f"构建键盘出错: {e}")
            # 返回空键盘
            return InlineKeyboardMarkup([])
            
    @staticmethod
    def create_settings_keyboard(group_id: int, permissions: List[str]) -> InlineKeyboardMarkup:
        """
        创建设置菜单键盘
        
        参数:
            group_id: 群组ID
            permissions: 权限列表
            
        返回:
            设置菜单键盘
        """
        try:
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
                
            # 添加开关设置按钮
            keyboard.append([
                InlineKeyboardButton(
                    "⚙️ 开关设置", 
                    callback_data=f"settings_switches_{group_id}"
                )
            ])
            
            # 添加自动删除设置按钮
            keyboard.append([
                InlineKeyboardButton(
                    "🗑️ 自动删除设置", 
                    callback_data=f"auto_delete_toggle_{group_id}"
                )
            ])
                
            keyboard.append([
                InlineKeyboardButton(
                    "🔙 返回群组列表", 
                    callback_data="show_manageable_groups"
                )
            ])
            
            return InlineKeyboardMarkup(keyboard)
        except Exception as e:
            logger.error(f"创建设置键盘出错: {e}")
            # 返回基本的返回按钮
            return InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 返回", callback_data="show_manageable_groups")
            ]])

class CommandHelper:
    """命令帮助类"""
    
    # 命令使用说明字典
    COMMAND_USAGE = {
        'start': {'usage': '/start', 'description': '启动机器人并查看功能列表', 'example': None, 'admin_only': False},
        'settings': {'usage': '/settings', 'description': '打开设置菜单', 'example': None, 'admin_only': True},
        'tongji': {'usage': '/tongji [页码]', 'description': '查看今日统计排行', 'example': '/tongji 2', 'admin_only': False},
        'tongji30': {'usage': '/tongji30 [页码]', 'description': '查看30日统计排行', 'example': '/tongji30 2', 'admin_only': False},
        'admingroups': {'usage': '/admingroups', 'description': '查看可管理的群组', 'example': None, 'admin_only': True},
        'easykeyword': {'usage': '/easykeyword', 'description': '添加关键词回复', 'example': None, 'admin_only': True},
        'easybroadcast': {'usage': '/easybroadcast', 'description': '添加轮播消息', 'example': None, 'admin_only': True},
        'addsuperadmin': {'usage': '/addsuperadmin <用户ID>', 'description': '添加超级管理员', 'example': '/addsuperadmin 123456789', 'admin_only': True},
        'delsuperadmin': {'usage': '/delsuperadmin <用户ID>', 'description': '删除超级管理员', 'example': '/delsuperadmin 123456789', 'admin_only': True},
        'addadmin': {'usage': '/addadmin <用户ID>', 'description': '添加管理员', 'example': '/addadmin 123456789', 'admin_only': True},
        'deladmin': {'usage': '/deladmin <用户ID>', 'description': '删除管理员', 'example': '/deladmin 123456789', 'admin_only': True},
        'authgroup': {'usage': '/authgroup <群组ID>', 'description': '授权群组', 'example': '/authgroup -100123456789', 'admin_only': True},
        'deauthgroup': {'usage': '/deauthgroup <群组ID>', 'description': '取消群组授权', 'example': '/deauthgroup -100123456789', 'admin_only': True},
        'checkconfig': {'usage': '/checkconfig', 'description': '检查当前配置', 'example': None, 'admin_only': True},
        'cancel': {'usage': '/cancel', 'description': '取消当前操作', 'example': None, 'admin_only': False}
    }
    
    @classmethod
    def get_usage(cls, command: str) -> Optional[Dict[str, Any]]:
        """
        获取命令使用说明
        
        参数:
            command: 命令名
            
        返回:
            命令使用说明字典或None
        """
        try:
            return cls.COMMAND_USAGE.get(command)
        except Exception as e:
            logger.error(f"获取命令使用说明出错: {e}")
            return None
        
    @classmethod
    def format_usage(cls, command: str) -> str:
        """
        格式化命令使用说明
        
        参数:
            command: 命令名
            
        返回:
            格式化后的命令使用说明
        """
        try:
            usage = cls.get_usage(command)
            if not usage:
                return "❌ 未知命令"
                
            text = [f"📝 命令: {command}", f"用法: {usage['usage']}", f"说明: {usage['description']}"]
            
            if usage['example']:
                text.append(f"示例: {usage['example']}")
                
            if usage['admin_only']:
                text.append("注意: 仅管理员可用")
                
            return "\n".join(text)
        except Exception as e:
            logger.error(f"格式化命令使用说明出错: {e}")
            return f"❌ 格式化命令 {command} 的使用说明时出错"
