"""
应用程序上下文管理器
用于解决循环引用问题，提供全局访问点
"""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# 全局应用程序上下文
_app_context = {
    'bot_instance': None,
    'auto_delete_manager': None,
    'recovery_manager': None,
    'settings_manager': None,
    'keyword_manager': None,
    'broadcast_manager': None,
    'stats_manager': None,
    'db': None
}

def register_bot_instance(bot_instance):
    """注册机器人实例"""
    _app_context['bot_instance'] = bot_instance
    logger.info("已注册机器人实例到应用程序上下文")
    
def get_bot_instance():
    """获取机器人实例"""
    return _app_context.get('bot_instance')
    
def register_auto_delete_manager(manager):
    """注册自动删除管理器"""
    _app_context['auto_delete_manager'] = manager
    logger.info("已注册自动删除管理器到应用程序上下文")
    
def get_auto_delete_manager():
    """获取自动删除管理器"""
    return _app_context.get('auto_delete_manager')
    
def register_recovery_manager(manager):
    """注册恢复管理器"""
    _app_context['recovery_manager'] = manager
    logger.info("已注册恢复管理器到应用程序上下文")
    
def get_recovery_manager():
    """获取恢复管理器"""
    return _app_context.get('recovery_manager')
    
def register_settings_manager(manager):
    """注册设置管理器"""
    _app_context['settings_manager'] = manager
    logger.info("已注册设置管理器到应用程序上下文")
    
def get_settings_manager():
    """获取设置管理器"""
    return _app_context.get('settings_manager')
    
def register_keyword_manager(manager):
    """注册关键词管理器"""
    _app_context['keyword_manager'] = manager
    logger.info("已注册关键词管理器到应用程序上下文")
    
def get_keyword_manager():
    """获取关键词管理器"""
    return _app_context.get('keyword_manager')
    
def register_broadcast_manager(manager):
    """注册轮播管理器"""
    _app_context['broadcast_manager'] = manager
    logger.info("已注册轮播管理器到应用程序上下文")
    
def get_broadcast_manager():
    """获取轮播管理器"""
    return _app_context.get('broadcast_manager')
    
def register_stats_manager(manager):
    """注册统计管理器"""
    _app_context['stats_manager'] = manager
    logger.info("已注册统计管理器到应用程序上下文")
    
def get_stats_manager():
    """获取统计管理器"""
    return _app_context.get('stats_manager')
    
def register_db(db):
    """注册数据库实例"""
    _app_context['db'] = db
    logger.info("已注册数据库实例到应用程序上下文")
    
def get_db():
    """获取数据库实例"""
    return _app_context.get('db')
    
def clear_context():
    """清除上下文"""
    for key in _app_context:
        _app_context[key] = None
    logger.info("已清除应用程序上下文")
