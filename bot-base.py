import os
import signal
import asyncio
import logging
from datetime import datetime
from aiohttp import web
from telegram.ext import Application, MessageHandler, filters

from db import Database, UserRole, GroupPermission
from bot_settings import SettingsManager
from bot_keywords import KeywordManager
from bot_broadcast import BroadcastManager
from bot_stats import StatsManager

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self):
        self.db = Database()
        self.application = None
        self.web_runner = None
        self.cleanup_task = None
        self.shutdown_event = asyncio.Event()
        self.running = False
        
        # 初始化各个管理器
        self.settings_manager = SettingsManager(self.db)
        self.keyword_manager = KeywordManager(self.db)
        self.broadcast_manager = BroadcastManager(self.db, self)
        self.stats_manager = StatsManager(self.db)
        
    async def initialize(self):
        """初始化机器人"""
        logger.info("Initializing bot...")
        
        # 初始化数据库
        await self.db.init_indexes()
        
        # 确保默认超级管理员存在
        from config import DEFAULT_SUPERADMINS
        for admin_id in DEFAULT_SUPERADMINS:
            await self.db.add_user({
                'user_id': admin_id,
                'role': UserRole.SUPERADMIN.value,
                'created_at': datetime.now().isoformat(),
                'created_by': None
            })
        
        # 初始化 Telegram 应用
        self.application = (
            Application.builder()
            .token(os.getenv('TELEGRAM_TOKEN'))
            .build()
        )
        
        # 启动轮播管理器
        await self.broadcast_manager.start()
        
        # 添加处理器
        self._add_handlers()
        
        # 启动后台任务
        self.cleanup_task = asyncio.create_task(self.cleanup_old_stats())
        
        logger.info("Bot initialization completed")
        
    async def start(self):
        """启动机器人"""
        try:
            logger.info("Starting services...")
            
            # 首先启动 web 服务器
            logger.info("Starting web server...")
            await self.setup_web_server()
            
            # 然后启动机器人
            logger.info("Starting Telegram bot...")
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            self.running = True
            logger.info("All services started successfully")
            
            # 保持运行直到收到停止信号
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error during startup: {e}")
            raise
        finally:
            logger.info("Initiating shutdown...")
            await self.shutdown()

    async def stop(self):
        """停止机器人"""
        logger.info("Stop signal received")
        self.running = False

    async def shutdown(self):
        """优雅关闭所有服务"""
        if not self.running:
            return

        logger.info("Initiating shutdown sequence...")
        
        # 设置关闭事件
        self.shutdown_event.set()
        
        # 取消清理任务
        if self.cleanup_task:
            logger.info("Cancelling cleanup task...")
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        # 停止轮播管理器
        await self.broadcast_manager.stop()
        
        # 关闭 Telegram 应用
        if self.application:
            logger.info("Shutting down Telegram bot...")
            try:
                if self.application.updater:
                    await self.application.updater.stop()
                await self.application.stop()
                await self.application.shutdown()
            except Exception as e:
                logger.error(f"Error during bot shutdown: {e}")
        
        # 关闭 Web 服务器
        if self.web_runner:
            logger.info("Shutting down web server...")
            try:
                await self.web_runner.cleanup()
            except Exception as e:
                logger.error(f"Error during web server shutdown: {e}")
        
        # 关闭数据库连接
        try:
            logger.info("Closing database connection...")
            self.db.close()
        except Exception as e:
            logger.error(f"Error during database shutdown: {e}")
        
        self.running = False
        logger.info("Shutdown completed")

    async def setup_web_server(self):
        """设置Web服务器"""
        try:
            # 创建应用
            app = web.Application()
            
            # 添加路由
            async def health_check(request):
                return web.Response(text="Bot is running", status=200)
            
            app.router.add_get('/', health_check)
            
            # 设置runner
            self.web_runner = web.AppRunner(app)
            await self.web_runner.setup()
            
            # 获取端口
            from config import WEB_PORT
            
            # 创建站点并启动
            site = web.TCPSite(self.web_runner, host='0.0.0.0', port=WEB_PORT)
            await site.start()
            
            logger.info(f"Web server started successfully on port {WEB_PORT}")
        except Exception as e:
            logger.error(f"Failed to start web server: {e}")
            raise

    async def is_superadmin(self, user_id: int) -> bool:
        """检查是否是超级管理员"""
        user = await self.db.get_user(user_id)
        return user and user['role'] == UserRole.SUPERADMIN.value
        
    async def is_admin(self, user_id: int) -> bool:
        """检查是否是管理员"""
        user = await self.db.get_user(user_id)
        return user and user['role'] in {UserRole.ADMIN.value, UserRole.SUPERADMIN.value}
        
    async def has_permission(self, group_id: int, permission: GroupPermission) -> bool:
        """检查群组权限"""
        group = await self.db.get_group(group_id)
        return group and permission.value in group.get('permissions', [])