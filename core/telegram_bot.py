"""
机器人主程序入口文件，处理初始化、启动、停止等操作
"""
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # 获取当前文件所在目录
parent_dir = os.path.dirname(current_dir)                # 获取父目录（项目根目录）
sys.path.insert(0, parent_dir)                           # 将项目根目录添加到Python路径

import signal
import asyncio
import logging
from aiohttp import web
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from telegram.error import BadRequest
from db.database import Database
from db.models import UserRole, GroupPermission
from core.callback_handler import CallbackHandler
from core.error_handler import ErrorHandler
from managers.settings_manager import SettingsManager
from managers.stats_manager import StatsManager
from managers.broadcast_manager import BroadcastManager
from managers.keyword_manager import KeywordManager
from utils.message_utils import validate_delete_timeout

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class TelegramBot:
    """
    Telegram机器人核心类，负责机器人的生命周期管理
    """
    def __init__(self):
        """初始化机器人实例"""
        self.db = None
        self.application = None
        self.web_app = None
        self.web_runner = None
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.cleanup_task = None
        
        # 各种管理器
        self.settings_manager = None
        self.keyword_manager = None
        self.broadcast_manager = None
        self.stats_manager = None
        self.error_handler = None
        self.callback_handler = None
        self.auto_delete_manager = None
        
    async def initialize(self):
        """初始化机器人"""
        try:
            # 验证配置
            from config_validator import validate_config, ConfigValidationError
            import config
            try:
                validate_config(config)
            except ConfigValidationError as e:
                logger.error(f"配置验证失败: {e}")
                return False
                
            logger.info("开始初始化机器人")
            
            # 加载配置
            from config import (
                TELEGRAM_TOKEN, MONGODB_URI, MONGODB_DB, DEFAULT_SUPERADMINS,
                DEFAULT_SETTINGS, BROADCAST_SETTINGS, KEYWORD_SETTINGS
            )
            
            # 连接数据库
            try:
                self.db = Database()
                if not await self.db.connect(MONGODB_URI, MONGODB_DB):
                    logger.error("数据库连接失败")
                    return False
            except Exception as e:
                logger.error(f"数据库连接错误: {e}", exc_info=True)
                return False
                
            # 初始化各个管理器
            self.error_handler = ErrorHandler(logger)
            self.callback_handler = CallbackHandler()
            
            self.settings_manager = SettingsManager(self.db)
            await self.settings_manager.start()
            
            self.keyword_manager = KeywordManager(self.db)
            
            # 注册内置关键词处理函数
            self.keyword_manager.register_built_in_handler('日排行', self._handle_daily_rank)
            self.keyword_manager.register_built_in_handler('月排行', self._handle_monthly_rank)
            
            self.stats_manager = StatsManager(self.db)
            self.broadcast_manager = BroadcastManager(self.db, self)

            # 初始化 自动删除管理器
            from managers.auto_delete_manager import AutoDeleteManager
            self.auto_delete_manager = AutoDeleteManager(self.db)
            logger.info("自动删除管理器已初始化")
            
            # 设置超级管理员
            for admin_id in DEFAULT_SUPERADMINS:
                await self.db.add_user({'user_id': admin_id, 'role': UserRole.SUPERADMIN.value})
                logger.info(f"已设置超级管理员: {admin_id}")
                
            # 设置默认群组
            default_groups = [{
                'group_id': -1001234567890,
                'permissions': [perm.value for perm in GroupPermission],
                'feature_switches': {'keywords': True, 'stats': True, 'broadcast': True}
            }]
            
            for group in default_groups:
                await self.db.add_group({
                    'group_id': group['group_id'],
                    'permissions': group['permissions'],
                    'settings': {'auto_delete': False, 'auto_delete_timeout': config.AUTO_DELETE_SETTINGS['default_timeout']},
                    'feature_switches': group['feature_switches']
                })
                logger.info(f"已设置群组权限: {group['group_id']}")
                
            # 设置Web服务和Webhook
            webhook_domain = os.getenv('WEBHOOK_DOMAIN', 'your-render-app-name.onrender.com')
            self.application = Application.builder().token(TELEGRAM_TOKEN).build()
            
            # 将bot实例存储在application的bot_data中，以便于在回调函数中访问
            self.application.bot_data['bot_instance'] = self
            
            # 注册处理函数
            from handlers import register_all_handlers
            register_all_handlers(self.application, self.callback_handler)
            
            # 设置Web应用
            self.web_app = web.Application()
            self.web_app.router.add_get('/', self.handle_healthcheck)
            self.web_app.router.add_get('/health', self.handle_healthcheck)
            
            # 设置Webhook
            webhook_url = f"https://{webhook_domain}/webhook/{TELEGRAM_TOKEN}"
            webhook_path = f"/webhook/{TELEGRAM_TOKEN}"
            self.web_app.router.add_post(webhook_path, self._handle_webhook)
            
            # 启动Web服务器
            self.web_runner = web.AppRunner(self.web_app)
            await self.web_runner.setup()
            from config import WEB_HOST, WEB_PORT
            site = web.TCPSite(self.web_runner, WEB_HOST, WEB_PORT)
            await site.start()
            logger.info(f"Web服务器已在 {WEB_HOST}:{WEB_PORT} 启动")
            
            # 设置Webhook
            await self.application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
            self.application.updater = None
            logger.info(f"Webhook已设置为 {webhook_url}")
            
            # 验证初始化
            if not await self.verify_initialization():
                logger.error("初始化验证失败")
                return False
                
            logger.info("机器人初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"机器人初始化失败: {e}", exc_info=True)
            return False
    
    async def verify_initialization(self):
        """验证初始化是否成功"""
        from config import DEFAULT_SUPERADMINS
        
        # 验证超级管理员
        for admin_id in DEFAULT_SUPERADMINS:
            user = await self.db.get_user(admin_id)
            if not user or user['role'] != UserRole.SUPERADMIN.value:
                logger.error(f"超级管理员 {admin_id} 初始化失败")
                return False
                
        # 验证群组
        groups = await self.db.find_all_groups()
        if not groups:
            logger.error("没有找到任何已授权的群组")
            return False
            
        logger.info("初始化验证成功")
        logger.info(f"超级管理员: {DEFAULT_SUPERADMINS}")
        logger.info(f"已授权群组: {[g['group_id'] for g in groups]}")
        return True
        
    @classmethod
    async def main(cls):
        """主入口方法"""
        bot = cls()
        if not await bot.initialize():
            logger.error("机器人初始化失败")
            return
            
        await bot.handle_signals()
        
        if not await bot.start():
            logger.error("机器人启动失败")
            return
            
        while bot.running:
            await asyncio.sleep(1)
            
    async def start(self):
        """启动机器人"""
        if not self.application:
            logger.error("机器人未初始化")
            return False
            
        # 启动应用
        await self.application.initialize()
        await self.application.start()
        self.running = True
        
        # 启动任务
        await self._start_broadcast_task()
        await self._start_cleanup_task()
        logger.info("机器人成功启动")
        return True
    
    async def stop(self):
        """停止机器人"""
        self.running = False
        
        # 设置关闭信号
        if self.shutdown_event:
            self.shutdown_event.set()
            
        # 停止设置管理器
        if self.settings_manager:
            await self.settings_manager.stop()
            
        # 取消清理任务
        if self.cleanup_task:
            self.cleanup_task.cancel()
            
        # 清理Web服务器
        if self.web_runner:
            await self.web_runner.cleanup()
            
        # 停止应用
        if self.application:
            try:
                if getattr(self.application, 'running', False):
                    await self.application.stop()
                    await self.application.shutdown()
            except Exception as e:
                logger.error(f"停止应用时出错: {e}")
                
        # 关闭数据库连接
        if self.db:
            try:
                await self.db.close()
            except Exception as e:
                logger.error(f"关闭数据库连接时出错: {e}")
                
    async def shutdown(self):
        """关闭机器人"""
        await self.stop()

    async def _start_broadcast_task(self):
        """启动广播任务"""
        while self.running:
            try:
                await self.broadcast_manager.process_broadcasts()
                # 每分钟检查一次
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"轮播任务出错: {e}")
                await asyncio.sleep(60)

    async def _start_cleanup_task(self):
        """启动清理任务"""
        async def cleanup_routine():
            while self.running:
                try:
                    from config import DEFAULT_SETTINGS
                    # 清理旧的统计数据
                    await self.db.cleanup_old_stats(days=DEFAULT_SETTINGS.get('cleanup_days', 30))
                    # 每天运行一次
                    await asyncio.sleep(24 * 60 * 60)
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
                    # 出错时一小时后重试
                    await asyncio.sleep(1 * 60 * 60)
                    
        self.cleanup_task = asyncio.create_task(cleanup_routine())
    
    async def handle_signals(self):
        """处理系统信号"""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_running_loop().add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.stop())
                )
            logger.info("信号处理器设置完成")
        except NotImplementedError:
            logger.warning("此平台不支持信号处理器")

    async def handle_healthcheck(self, request):
        """健康检查处理函数"""
        return web.Response(text="Healthy", status=200)

    async def _handle_webhook(self, request):
        """处理Webhook请求"""
        try:
            # 验证内容类型
            if request.content_type != 'application/json':
                logger.warning(f"收到无效的内容类型: {request.content_type}")
                return web.Response(status=415)
                
            # 解析更新数据
            update_data = await request.json()
            logger.info(f"收到webhook更新: {update_data}")
            
            # 创建更新对象
            update = Update.de_json(update_data, self.application.bot)
            if update:
                # 处理更新
                await self.application.process_update(update)
                logger.info("成功处理更新")
            else:
                logger.warning("收到无效的更新数据")
                
            return web.Response(status=200)
            
        except Exception as e:
            logger.error(f"处理webhook错误: {e}", exc_info=True)
            return web.Response(status=500)

    async def is_superadmin(self, user_id: int) -> bool:
        """检查用户是否为超级管理员"""
        user = await self.db.get_user(user_id)
        return user and user['role'] == UserRole.SUPERADMIN.value
        
    async def is_admin(self, user_id: int) -> bool:
        """检查用户是否为管理员"""
        user = await self.db.get_user(user_id)
        return user and user['role'] in {UserRole.ADMIN.value, UserRole.SUPERADMIN.value}
        
    async def has_permission(self, group_id: int, permission: GroupPermission) -> bool:
        """检查群组是否有特定权限"""
        group = await self.db.get_group(group_id)
        if group:
            switches = group.get('feature_switches', {'keywords': True, 'stats': True, 'broadcast': True})
            feature_name = permission.value
            return permission.value in group.get('permissions', []) and switches.get(feature_name, True)
        return False

    async def add_default_keywords(self, group_id: int):
            """
            为群组添加默认的关键词
            
            参数:
                group_id: 群组ID
            """
            logger.info(f"为群组 {group_id} 添加默认关键词")
            
            # 检查群组是否启用关键词功能
            if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
                logger.info(f"群组 {group_id} 未启用关键词功能，跳过添加默认关键词")
                return
            
            # 检查关键词是否已存在
            existing_keywords = await self.db.get_keywords(group_id)
            existing_patterns = [kw.get('pattern', '') for kw in existing_keywords]
            
            # 添加日排行关键词（无提示消息）
            if '日排行' not in existing_patterns:
                await self.db.add_keyword({
                    'group_id': group_id,
                    'pattern': '日排行',
                    'type': 'exact',
                    'response': '',  # 移除提示消息
                    'media': None,
                    'buttons': [],
                    'is_command': True,
                    'command': '/tongji'
                })
                logger.info(f"已为群组 {group_id} 添加'日排行'关键词")
            
            # 添加月排行关键词（无提示消息）
            if '月排行' not in existing_patterns:
                await self.db.add_keyword({
                    'group_id': group_id,
                    'pattern': '月排行',
                    'type': 'exact',
                    'response': '',  # 移除提示消息
                    'media': None,
                    'buttons': [],
                    'is_command': True,
                    'command': '/tongji30'
                })
                logger.info(f"已为群组 {group_id} 添加'月排行'关键词")
    
    async def _handle_daily_rank(self, message):
        """
        处理"日排行"关键词
        
        参数:
            message: 消息对象
            
        返回:
            命令ID或None
        """
        try:
            # 移除提示消息
            # await message.reply_text("正在查询今日排行...")
            
            # 执行tongji命令的逻辑
            from handlers.command_handlers import handle_rank_command
            
            # 创建一个简单的上下文
            from telegram.ext import ContextTypes
            context = ContextTypes.DEFAULT_TYPE(self.application)
            context.args = []
            
            # 创建一个假的Update对象，确保使用/tongji命令
            message_dict = message.to_dict()
            # 修改这里，确保使用正确的日排行命令
            message_dict['text'] = '/tongji'
            fake_message = message.__class__.de_json(message_dict, self.application.bot)
            
            fake_update = Update(
                update_id=message.message_id,
                message=fake_message
            )
            
            # 执行命令
            await handle_rank_command(fake_update, context)
            return "daily_rank_executed"
        except Exception as e:
            logger.error(f"处理日排行关键词出错: {e}", exc_info=True)
            return None
        
    async def _handle_monthly_rank(self, message):
        """
        处理"月排行"关键词
        
        参数:
            message: 消息对象
            
        返回:
            命令ID或None
        """
        try:
            # 移除提示消息
            # await message.reply_text("正在查询月度排行...")
            
            # 执行tongji30命令的逻辑
            from handlers.command_handlers import handle_rank_command
            
            # 创建一个简单的上下文
            from telegram.ext import ContextTypes
            context = ContextTypes.DEFAULT_TYPE(self.application)
            context.args = []
            
            # 创建一个假的Update对象，模拟tongji30命令
            # 需要修改message的text属性来模拟不同的命令
            message_dict = message.to_dict()
            message_dict['text'] = '/tongji30'
            fake_message = message.__class__.de_json(message_dict, self.application.bot)
            
            fake_update = Update(
                update_id=message.message_id,
                message=fake_message
            )
            
            # 执行命令
            await handle_rank_command(fake_update, context)
            return "monthly_rank_executed"
        except Exception as e:
            logger.error(f"处理月排行关键词出错: {e}", exc_info=True)
            return None

    async def _schedule_delete(self, message, timeout: int):
        """
        计划删除消息 - 兼容现有代码的接口
        
        参数:
            message: 要删除的消息
            timeout: 超时时间（秒）
        """
        if self.auto_delete_manager:
            # 使用自动删除管理器
            group_id = message.chat.id if message.chat else None
            await self.auto_delete_manager.schedule_delete(message, 'default', group_id, timeout)
        else:
            # 旧的实现方式，以防自动删除管理器不可用
            await asyncio.sleep(timeout)
            try:
                await message.delete()
            except Exception as e:
                logger.error(f"删除消息失败: {e}")

# 启动函数
if __name__ == '__main__':
    asyncio.run(TelegramBot.main())
