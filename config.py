import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Telegram Bot Token
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN is not set in environment variables")

# MongoDB配置
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
MONGODB_DB = os.getenv('MONGODB_DB', 'telegram_bot')

# Web服务器配置
WEB_HOST = os.getenv('WEB_HOST', '0.0.0.0')
WEB_PORT = int(os.getenv('PORT', '8080'))

# 默认超级管理员列表
DEFAULT_SUPERADMINS = [
    358987879,  # 用户1
    502226686,  # 用户2
    883253093   # 用户3
]

DEFAULT_GROUPS = [
    {
        'group_id': -1001234567890,  # 替换为你的群组ID
        'permissions': ['keywords', 'stats', 'broadcast']
    },
    {
        'group_id': -1002438745546,  # 替换为你的群组ID
        'permissions': ['keywords', 'stats', 'broadcast']
    }
    # 可以添加更多群组
]

# 统计设置
DEFAULT_SETTINGS = {
    'min_bytes': 0,              # 默认最低字节数
    'count_media': False,        # 默认不统计多媒体
    'daily_rank_size': 15,       # 日排行显示数量
    'monthly_rank_size': 15,     # 月排行显示数量
    'cleanup_days': 30,          # 统计数据保留天数
}

# 轮播消息设置
BROADCAST_SETTINGS = {
    'min_interval': 300,         # 最小轮播间隔（秒）
    'max_broadcasts': 10,        # 每个群组最大轮播消息数
    'check_interval': 60,        # 轮播检查间隔（秒）
}

# 关键词设置
KEYWORD_SETTINGS = {
    'max_keywords': 100,         # 每个群组最大关键词数
    'max_pattern_length': 100,   # 关键词最大长度
    'max_response_length': 1000, # 回复最大长度
}

# 自动删除消息设置
AUTO_DELETE_SETTINGS = {
    'default_timeout': 300,      # 默认删除时间：5分钟
    'max_timeout': 86400,        # 最大删除时间：24小时
    'min_timeout': 10,           # 最小删除时间：10秒
    'enabled': True,             # 是否启用自动删除
    'exempt_roles': [            # 免除自动删除的用户角色
        'SUPERADMIN', 
        'ADMIN'
    ],
    'exempt_command_prefixes': [  # 免除自动删除的命令前缀
        '/start', 
        '/help', 
        '/settings'
    ]
}

# 消息类型配置
ALLOWED_MEDIA_TYPES = [
    'text',
    'photo',
    'video',
    'document'
]

# 权限配置
DEFAULT_PERMISSIONS = [
    'keywords',
    'stats',
    'broadcast'
]

# 时区设置
TIMEZONE = os.getenv('TIMEZONE', 'Asia/Shanghai')
