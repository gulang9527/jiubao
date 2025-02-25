import os

class Config:
    """配置管理类"""
    def __init__(self):
        self.telegram_token = os.getenv('TELEGRAM_TOKEN')
        self.mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.mongodb_db_name = os.getenv('MONGODB_DB_NAME', 'telegram_bot')

    def get_telegram_token(self) -> str:
        """获取Telegram API令牌"""
        return self.telegram_token

    def get_mongodb_uri(self) -> str:
        """获取MongoDB连接URI"""
        return self.mongodb_uri

    def get_mongodb_db_name(self) -> str:
        """获取MongoDB数据库名称"""
        return self.mongodb_db_name