from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

class ConfigValidationError(Exception):
    pass

def validate_telegram_token(token: Optional[str]) -> bool:
    """验证Telegram Token"""
    if not token:
        raise ConfigValidationError("TELEGRAM_TOKEN 未设置")
    if not isinstance(token, str):
        raise ConfigValidationError("TELEGRAM_TOKEN 必须是字符串")
    if len(token.split(':')) != 2:
        raise ConfigValidationError("TELEGRAM_TOKEN 格式无效")
    return True

def validate_mongodb_config(uri: str, db_name: str) -> bool:
    """验证MongoDB配置"""
    if not uri:
        raise ConfigValidationError("MONGODB_URI 未设置")
    if not db_name:
        raise ConfigValidationError("MONGODB_DB 未设置")
    return True

def validate_web_config(host: str, port: int) -> bool:
    """验证Web服务器配置"""
    if not isinstance(port, int):
        raise ConfigValidationError("WEB_PORT 必须是整数")
    if port <= 0 or port > 65535:
        raise ConfigValidationError("WEB_PORT 必须在 1-65535 之间")
    return True

def validate_superadmins(superadmins: List[int]) -> bool:
    """验证超级管理员配置"""
    if not isinstance(superadmins, list):
        raise ConfigValidationError("DEFAULT_SUPERADMINS 必须是列表")
    if not superadmins:
        raise ConfigValidationError("DEFAULT_SUPERADMINS 不能为空")
    if not all(isinstance(admin_id, int) for admin_id in superadmins):
        raise ConfigValidationError("所有超级管理员ID必须是整数")
    return True

def validate_settings(settings: Dict[str, Any]) -> bool:
    """验证默认设置"""
    required_fields = ['min_bytes', 'count_media', 'daily_rank_size', 'monthly_rank_size', 'cleanup_days']
    for field in required_fields:
        if field not in settings:
            raise ConfigValidationError(f"DEFAULT_SETTINGS 缺少必要字段: {field}")
    
    if settings['min_bytes'] < 0:
        raise ConfigValidationError("min_bytes 不能为负数")
    if settings['daily_rank_size'] <= 0:
        raise ConfigValidationError("daily_rank_size 必须大于0")
    if settings['monthly_rank_size'] <= 0:
        raise ConfigValidationError("monthly_rank_size 必须大于0")
    if settings['cleanup_days'] <= 0:
        raise ConfigValidationError("cleanup_days 必须大于0")
    return True

def validate_broadcast_settings(settings: Dict[str, Any]) -> bool:
    """验证轮播消息设置"""
    required_fields = ['min_interval', 'max_broadcasts', 'check_interval']
    for field in required_fields:
        if field not in settings:
            raise ConfigValidationError(f"BROADCAST_SETTINGS 缺少必要字段: {field}")
    
    if settings['min_interval'] <= 0:
        raise ConfigValidationError("min_interval 必须大于0")
    if settings['max_broadcasts'] <= 0:
        raise ConfigValidationError("max_broadcasts 必须大于0")
    if settings['check_interval'] <= 0:
        raise ConfigValidationError("check_interval 必须大于0")
    return True

def validate_config(config_module) -> bool:
    """验证所有配置"""
    try:
        validate_telegram_token(config_module.TELEGRAM_TOKEN)
        validate_mongodb_config(config_module.MONGODB_URI, config_module.MONGODB_DB)
        validate_web_config(config_module.WEB_HOST, config_module.WEB_PORT)
        validate_superadmins(config_module.DEFAULT_SUPERADMINS)
        validate_settings(config_module.DEFAULT_SETTINGS)
        validate_broadcast_settings(config_module.BROADCAST_SETTINGS)
        logger.info("配置验证通过")
        return True
    except ConfigValidationError as e:
        logger.error(f"配置验证失败: {e}")
        raise
