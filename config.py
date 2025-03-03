import os
from dotenv import load_dotenv
import pytz

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN is not set in environment variables")

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
MONGODB_DB = os.getenv('MONGODB_DB', 'telegram_bot')
WEB_HOST = os.getenv('WEB_HOST', '0.0.0.0')
WEB_PORT = int(os.getenv('PORT', '8080'))

DEFAULT_SUPERADMINS = [358987879, 502226686, 7713337585, 883253093]
DEFAULT_GROUPS = [{
    'group_id': -1001234567890,
    'permissions': ['keywords', 'stats', 'broadcast'],
    'feature_switches': {'keywords': True, 'stats': True, 'broadcast': True}
}]

TIMEZONE_STR = os.getenv('TIMEZONE', 'Asia/Shanghai')
TIMEZONE = pytz.timezone(TIMEZONE_STR)

DEFAULT_SETTINGS = {
    'min_bytes': 0,
    'count_media': False,
    'daily_rank_size': 15,
    'monthly_rank_size': 15,
    'cleanup_days': 30,
}

BROADCAST_SETTINGS = {
    'min_interval': 300,
    'max_broadcasts': 10,
    'check_interval': 60,
}

KEYWORD_SETTINGS = {
    'max_keywords': 100,
    'max_pattern_length': 100,
    'max_response_length': 1000,
}

AUTO_DELETE_SETTINGS = {
    'enabled': True,
    'default_timeout': 300,
    'max_timeout': 86400,
    'min_timeout': 10,
    'exempt_roles': ['SUPERADMIN', 'ADMIN'],
    'exempt_command_prefixes': ['/start', '/help', '/settings', '/tongji', '/tongji30'],
    'timeouts': {
        'broadcast': 3600,  # 轮播消息删除时间：1小时
        'keyword': 1800,    # 关键词响应删除时间：30分钟
        'ranking': 7200,    # 排行榜删除时间：2小时
        'default': 300      # 默认删除时间：5分钟
    }
}

ALLOWED_MEDIA_TYPES = ['text', 'photo', 'video', 'document']
DEFAULT_PERMISSIONS = ['keywords', 'stats', 'broadcast']
STATE_MANAGEMENT_SETTINGS = {
    'cleanup_interval': 60,
    'state_timeout': 300,
    'max_concurrent_states': 5
}
CALLBACK_SETTINGS = {
    'answer_timeout': 10,
    'max_callback_age': 3600,
}
ERROR_HANDLING_SETTINGS = {
    'max_retries': 3,
    'retry_delay': 1,
    'error_report_channel': None
}
KEEP_ALIVE_INTERVAL = 300
