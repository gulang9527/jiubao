"""
命令帮助工具，提供命令使用说明的管理功能
"""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class CommandHelper:
    """命令帮助工具，管理命令使用说明"""
    
    # 命令使用说明字典
    COMMAND_USAGE = {
        'start': {
            'usage': '/start',
            'description': '启动机器人并查看功能列表',
            'example': None,
            'admin_only': False
        },
        'settings': {
            'usage': '/settings',
            'description': '打开设置菜单',
            'example': None,
            'admin_only': True
        },
        'tongji': {
            'usage': '/tongji [页码]',
            'description': '查看今日统计排行',
            'example': '/tongji 2',
            'admin_only': False
        },
        'tongji30': {
            'usage': '/tongji30 [页码]',
            'description': '查看30日统计排行',
            'example': '/tongji30 2',
            'admin_only': False
        },
        'admingroups': {
            'usage': '/admingroups',
            'description': '查看可管理的群组',
            'example': None,
            'admin_only': True
        },
        'easykeyword': {
            'usage': '/easykeyword',
            'description': '添加关键词回复',
            'example': None,
            'admin_only': True
        },
        'easybroadcast': {
            'usage': '/easybroadcast',
            'description': '添加轮播消息',
            'example': None,
            'admin_only': True
        },
        'addsuperadmin': {
            'usage': '/addsuperadmin <用户ID>',
            'description': '添加超级管理员',
            'example': '/addsuperadmin 123456789',
            'admin_only': True
        },
        'delsuperadmin': {
            'usage': '/delsuperadmin <用户ID>',
            'description': '删除超级管理员',
            'example': '/delsuperadmin 123456789',
            'admin_only': True
        },
        'addadmin': {
            'usage': '/addadmin <用户ID>',
            'description': '添加管理员',
            'example': '/addadmin 123456789',
            'admin_only': True
        },
        'deladmin': {
            'usage': '/deladmin <用户ID>',
            'description': '删除管理员',
            'example': '/deladmin 123456789',
            'admin_only': True
        },
        'authgroup': {
            'usage': '/authgroup <群组ID>',
            'description': '授权群组',
            'example': '/authgroup -100123456789',
            'admin_only': True
        },
        'deauthgroup': {
            'usage': '/deauthgroup <群组ID>',
            'description': '取消群组授权',
            'example': '/deauthgroup -100123456789',
            'admin_only': True
        },
        'checkconfig': {
            'usage': '/checkconfig',
            'description': '检查当前配置',
            'example': None,
            'admin_only': True
        },
        'cancel': {
            'usage': '/cancel',
            'description': '取消当前操作',
            'example': None,
            'admin_only': False
        }
    }

    @classmethod
    def get_usage(cls, command: str) -> Optional[Dict[str, Any]]:
        """
        获取命令使用说明
        
        参数:
            command: 命令名称
            
        返回:
            命令使用说明字典或None
        """
        return cls.COMMAND_USAGE.get(command)
    
    @classmethod
    def format_usage(cls, command: str) -> str:
        """
        格式化命令使用说明
        
        参数:
            command: 命令名称
            
        返回:
            格式化后的命令使用说明
        """
        usage = cls.get_usage(command)
        if not usage:
            return f"未知命令: {command}"
        
        lines = [
            f"📝 命令: {command}",
            f"用法: {usage['usage']}",
            f"说明: {usage['description']}"
        ]
        
        if usage['example']:
            lines.append(f"示例: {usage['example']}")
            
        if usage['admin_only']:
            lines.append("注意: 仅管理员可用")
            
        return "\n".join(lines)
    
    @classmethod
    def get_all_commands(cls, admin_only: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        获取所有命令列表
        
        参数:
            admin_only: 是否仅返回管理员命令
            
        返回:
            命令字典
        """
        if admin_only:
            return {cmd: info for cmd, info in cls.COMMAND_USAGE.items() if info['admin_only']}
        else:
            return cls.COMMAND_USAGE
    
    @classmethod
    def format_help_message(cls, admin: bool = False) -> str:
        """
        格式化帮助消息
        
        参数:
            admin: 是否为管理员
            
        返回:
            格式化后的帮助消息
        """
        # 基础命令
        basic_commands = []
        admin_commands = []
        superadmin_commands = []
        
        for cmd, info in cls.COMMAND_USAGE.items():
            # 基础命令
            if not info['admin_only']:
                basic_commands.append(f"/{cmd} - {info['description']}")
            # 管理员命令但不需要超级管理员权限
            elif 'super' not in cmd:
                admin_commands.append(f"/{cmd} - {info['description']}")
            # 超级管理员命令
            else:
                superadmin_commands.append(f"/{cmd} - {info['description']}")
                
        # 构建帮助消息
        message = "📋 可用命令列表：\n\n📌 基础命令：\n"
        message += "\n".join(basic_commands)
        
        # 如果是管理员，添加管理员命令
        if admin:
            message += "\n\n🔧 管理员命令：\n"
            message += "\n".join(admin_commands)
            
            # 超级管理员命令
            message += "\n\n👑 超级管理员命令：\n"
            message += "\n".join(superadmin_commands)
            
        message += "\n\n使用 /start 查看欢迎信息和功能介绍"
        
        return message
