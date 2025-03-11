"""
配置验证器，用于检查配置文件是否有效
"""
import logging
from typing import Dict, Any, Type

logger = logging.getLogger(__name__)

class ConfigValidationError(Exception):
    """配置验证错误"""
    pass

def validate_config(config_module):
    """
    验证配置模块
    
    参数:
        config_module: 配置模块对象
        
    抛出:
        ConfigValidationError: 如果配置无效
    """
    try:
        # 获取必需配置项及其类型
        required_configs = getattr(config_module, 'validate_config_dict', lambda: {})()
        
        # 检查每个必需配置项
        for config_name, config_type in required_configs.items():
            if not hasattr(config_module, config_name):
                raise ConfigValidationError(f"缺少必需配置项: {config_name}")
                
            config_value = getattr(config_module, config_name)
            if not isinstance(config_value, config_type):
                raise ConfigValidationError(
                    f"配置项 {config_name} 类型错误: 预期 {config_type.__name__}, 实际 {type(config_value).__name__}"
                )
                
        # 验证TELEGRAM_TOKEN不能为空
        if not getattr(config_module, 'TELEGRAM_TOKEN', None):
            raise ConfigValidationError("TELEGRAM_TOKEN 不能为空")
            
        # 验证默认超级管理员列表不能为空
        if not getattr(config_module, 'DEFAULT_SUPERADMINS', []):
            raise ConfigValidationError("DEFAULT_SUPERADMINS 不能为空")
            
        # 验证自动删除设置
        auto_delete_settings = getattr(config_module, 'AUTO_DELETE_SETTINGS', {})
        if not isinstance(auto_delete_settings, dict):
            raise ConfigValidationError("AUTO_DELETE_SETTINGS 必须是字典")
            
        # 验证最小删除时间不能大于最大删除时间
        min_timeout = auto_delete_settings.get('min_timeout', 0)
        max_timeout = auto_delete_settings.get('max_timeout', 0)
        if min_timeout > max_timeout:
            raise ConfigValidationError(f"最小删除时间({min_timeout})不能大于最大删除时间({max_timeout})")
            
        # 验证默认删除时间在最小和最大值之间
        default_timeout = auto_delete_settings.get('default_timeout', 0)
        if default_timeout < min_timeout or default_timeout > max_timeout:
            raise ConfigValidationError(
                f"默认删除时间({default_timeout})必须在最小删除时间({min_timeout})和最大删除时间({max_timeout})之间"
            )
            
        # 验证轮播消息设置
        broadcast_settings = getattr(config_module, 'BROADCAST_SETTINGS', {})
        if not isinstance(broadcast_settings, dict):
            raise ConfigValidationError("BROADCAST_SETTINGS 必须是字典")
            
        # 验证最小轮播间隔
        min_interval = broadcast_settings.get('min_interval', 0)
        if min_interval < 1:  # 最小1分钟
            raise ConfigValidationError(f"最小轮播间隔({min_interval})不能小于1分钟")
            
        # 验证时区设置
        timezone_str = getattr(config_module, 'TIMEZONE_STR', None)
        if not timezone_str:
            raise ConfigValidationError("TIMEZONE_STR 不能为空")
            
        # 通过验证
        logger.info("配置验证通过")
        return True
        
    except Exception as e:
        if not isinstance(e, ConfigValidationError):
            logger.error(f"配置验证过程出错: {e}", exc_info=True)
            raise ConfigValidationError(f"配置验证过程出错: {str(e)}")
        else:
            logger.error(f"配置验证失败: {e}")
            raise

def validate_database_config(config):
    """
    验证数据库配置
    
    参数:
        config: 数据库配置字典
        
    抛出:
        ConfigValidationError: 如果配置无效
    """
    required_fields = ['uri', 'database']
    
    for field in required_fields:
        if field not in config:
            raise ConfigValidationError(f"数据库配置缺少必需字段: {field}")
            
    # 验证URI格式
    uri = config['uri']
    if not uri.startswith(('mongodb://', 'mongodb+srv://')):
        raise ConfigValidationError("数据库URI格式无效，必须以mongodb://或mongodb+srv://开头")
        
    return True

def validate_broadcast_config(config):
    """
    验证轮播消息配置
    
    参数:
        config: 轮播消息配置字典
        
    抛出:
        ConfigValidationError: 如果配置无效
    """
    # 验证必需字段
    required_fields = ['group_id', 'start_time', 'end_time', 'interval']
    
    for field in required_fields:
        if field not in config:
            raise ConfigValidationError(f"轮播消息配置缺少必需字段: {field}")
            
    # 验证间隔
    interval = config['interval']
    if interval < 60:  # 最小1分钟
        raise ConfigValidationError(f"轮播间隔({interval}秒)不能小于60秒")
        
    # 验证时间
    start_time = config['start_time']
    end_time = config['end_time']
    
    if end_time <= start_time:
        raise ConfigValidationError("结束时间必须晚于开始时间")
        
    return True
