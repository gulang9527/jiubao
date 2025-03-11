"""
消息处理工具，提供消息相关的实用函数
"""
import logging
import re
from typing import Optional, Dict, Any, Tuple, List

from telegram import Message

logger = logging.getLogger(__name__)

def get_media_type(message: Message) -> Optional[str]:
    """
    获取消息的媒体类型
    
    参数:
        message: 消息对象
        
    返回:
        媒体类型或None
    """
    if not message:
        logger.warning("获取媒体类型: 消息对象为空")
        return None
        
    try:
        logger.info(f"获取媒体类型: message.photo={bool(message.photo)}, "
                   f"message.video={bool(message.video)}, "
                   f"message.document={bool(message.document)}")
        
        if message.photo:
            logger.info(f"识别为photo类型, photo数组长度: {len(message.photo)}")
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
            
        logger.warning("未识别到任何媒体类型")
        return None
    except Exception as e:
        logger.error(f"获取媒体类型出错: {e}", exc_info=True)
        return None

def get_file_id(message: Message) -> Optional[str]:
    """
    获取消息的文件ID
    
    参数:
        message: 消息对象
        
    返回:
        文件ID或None
    """
    if not message:
        logger.warning("获取文件ID: 消息对象为空")
        return None
        
    try:
        if message.photo:
            # 照片是一个数组，取最后一个（最大尺寸）
            photo_sizes = len(message.photo)
            logger.info(f"获取photo文件ID, 共{photo_sizes}种尺寸")
            if photo_sizes > 0:
                file_id = message.photo[-1].file_id
                logger.info(f"获取到photo文件ID: {file_id}")
                return file_id
            else:
                logger.warning("photo数组为空")
                return None
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
            
        logger.warning("未能获取到文件ID")
        return None
    except Exception as e:
        logger.error(f"获取文件ID出错: {e}", exc_info=True)
        return None

def get_message_size(message: Message) -> int:
    """
    计算消息大小（字节）
    
    参数:
        message: 消息对象
        
    返回:
        消息大小
    """
    if not message:
        return 0
        
    try:
        size = len(message.text or '') if message.text else 0
        
        # 添加媒体大小
        if message.photo and message.photo:
            size += message.photo[-1].file_size
        elif message.video:
            size += message.video.file_size
        elif message.document:
            size += message.document.file_size
        elif message.audio:
            size += message.audio.file_size
        elif message.voice:
            size += message.voice.file_size
        elif message.video_note:
            size += message.video_note.file_size
        elif message.sticker:
            size += message.sticker.file_size
            
        return size
    except Exception as e:
        logger.error(f"计算消息大小出错: {e}")
        return 0

def format_message_preview(message: Message, max_length: int = 50) -> str:
    """
    格式化消息预览
    
    参数:
        message: 消息对象
        max_length: 最大长度
        
    返回:
        消息预览文本
    """
    if not message:
        return "[无效消息]"
        
    try:
        # 获取消息文本
        if message.text:
            text = message.text
        elif message.caption:
            text = message.caption
        else:
            # 获取媒体类型描述
            media_type = get_media_type(message)
            if media_type:
                media_texts = {
                    'photo': "[图片]",
                    'video': "[视频]",
                    'document': "[文件]",
                    'animation': "[动画]",
                    'audio': "[音频]",
                    'voice': "[语音]",
                    'video_note': "[视频留言]",
                    'sticker': "[贴纸]"
                }
                return media_texts.get(media_type, "[媒体]")
            return "[消息]"
            
        # 裁剪文本
        if len(text) > max_length:
            return text[:max_length] + "..."
        return text
    except Exception as e:
        logger.error(f"格式化消息预览出错: {e}")
        return "[消息]"

def extract_urls(text: str) -> List[str]:
    """
    从文本中提取URL
    
    参数:
        text: 文本
        
    返回:
        URL列表
    """
    if not text:
        return []
        
    try:
        # URL匹配模式
        url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
        return re.findall(url_pattern, text)
    except Exception as e:
        logger.error(f"提取URL出错: {e}")
        return []

def extract_user_mentions(text: str) -> List[str]:
    """
    从文本中提取用户提及
    
    参数:
        text: 文本
        
    返回:
        用户名列表
    """
    if not text:
        return []
        
    try:
        # 用户名匹配模式
        mention_pattern = r'@(\w+)'
        return re.findall(mention_pattern, text)
    except Exception as e:
        logger.error(f"提取用户提及出错: {e}")
        return []

def validate_delete_timeout(timeout: Optional[int] = None, message_type: Optional[str] = None) -> int:
    """
    验证自动删除超时时间
    
    参数:
        timeout: 超时时间（秒）
        message_type: 消息类型
        
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
        logger.error(f"验证删除超时时间出错: {e}")
        return 300  # 默认5分钟

def is_auto_delete_exempt(user_role: str, command: Optional[str] = None) -> bool:
    """
    检查是否免除自动删除
    
    参数:
        user_role: 用户角色
        command: 命令
        
    返回:
        是否免除
    """
    try:
        import config
        
        # 检查用户角色
        if user_role in config.AUTO_DELETE_SETTINGS.get('exempt_roles', []):
            return True
        
        # 检查命令前缀
        if command and any(command.startswith(prefix) for prefix in 
                          config.AUTO_DELETE_SETTINGS.get('exempt_command_prefixes', [])):
            return True
        
        return False
    except Exception as e:
        logger.error(f"检查自动删除豁免出错: {e}")
        return False

def escape_markdown(text: str) -> str:
    """
    转义Markdown特殊字符
    
    参数:
        text: 文本
        
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
        error: 异常
        
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
