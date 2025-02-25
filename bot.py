import os
import logging
from typing import Optional, Dict, Any
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters
)
from middlewares.message_middleware import MessageMiddleware
from middlewares.error_middleware import ErrorMiddleware
from managers.settings_manager import SettingsManager
from managers.stats_manager import StatsManager
from managers.broadcast_manager import BroadcastManager
from managers.keyword_manager import KeywordManager
from utils.error_handler import ErrorHandler
from config import Config
from db import Database

class TelegramBot:
    def __init__(self):
        self.config = Config()
        self.db = None
        self.application = None
        self.running = False
        self.error_handler = ErrorHandler()
        self.settings_manager = None
        self.keyword_manager = None
        self.broadcast_manager = None
        self.stats_manager = None

    async def initialize(self):
        """初始化机器人"""
        try:
            # 初始化数据库
            self.db = Database()
            if not await self.db.connect():
                return False

            # 初始化管理器
            self.settings_manager = SettingsManager(self.db)
            self.keyword_manager = KeywordManager(self.db)
            self.broadcast_manager = BroadcastManager(self.db)
            self.stats_manager = StatsManager(self.db)

            # 创建Telegram应用
            self.application = Application.builder().token(self.config.get_telegram_token()).build()

            # 注册中间件
            message_middleware = MessageMiddleware()
            error_middleware = ErrorMiddleware(self.error_handler)
            self.application.post_init = message_middleware
            self.application.post_init = error_middleware

            # 注册处理器
            self._register_handlers()

            return True

        except Exception as e:
            logging.error(f"初始化失败: {e}")
            return False

    def _register_handlers(self):
        """注册处理器"""
        from handlers.command_handlers import command_handlers
        from handlers.callback_handlers import callback_handlers
        from handlers.message_handlers import message_handlers

        for handler in command_handlers():
            self.application.add_handler(handler)

        for handler in callback_handlers():
            self.application.add_handler(handler)

        for handler in message_handlers():
            self.application.add_handler(handler)

    async def start(self):
        """启动机器人"""
        try:
            if not self.application:
                return False

            await self.application.initialize()
            await self.application.start()
            self.running = True
            return True

        except Exception as e:
            logging.error(f"启动失败: {e}")
            return False

    async def stop(self):
        """停止机器人"""
        try:
            if self.application:
                await self.application.stop()
                await self.application.shutdown()
            self.running = False
            if self.db:
                await self.db.close()
        except Exception as e:
            logging.error(f"停止失败: {e}")