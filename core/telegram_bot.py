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
import time
import aiohttp
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
from config import (
    TELEGRAM_TOKEN, MONGODB_URI, MONGODB_DB, DEFAULT_SUPERADMINS,
    DEFAULT_SETTINGS, BROADCAST_SETTINGS, KEYWORD_SETTINGS
)

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
        
        # 时间校准管理器
        self.calibration_manager = None
        # 最后活动时间，用于检测系统休眠
        self.last_active_time = datetime.now()
        
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
                
                # 标准化轮播消息时间字段
                logger.info("开始标准化轮播消息时间字段...")
                await self.db.normalize_broadcast_datetimes()
                logger.info("标准化轮播消息时间字段完成")
            except Exception as e:
                logger.error(f"数据库连接错误: {e}", exc_info=True)
                return False

            # 获取初始化标志，检查机器人是否已经初始化过
            initialized = await self.db.get_system_flag("bot_initialized")
            apply_defaults = not initialized
            logger.info(f"机器人初始化状态: {'已初始化' if initialized else '首次初始化'}")
                
            # 初始化各个管理器
            self.error_handler = ErrorHandler(logger)
            self.callback_handler = CallbackHandler()
            
            self.settings_manager = SettingsManager(self.db)
            await self.settings_manager.start(apply_defaults_if_missing=apply_defaults)
            
            self.keyword_manager = KeywordManager(self.db, apply_defaults=apply_defaults)
            
            # 注册内置关键词处理函数
            self.keyword_manager.register_built_in_handler('日排行', self._handle_daily_rank)
            self.keyword_manager.register_built_in_handler('月排行', self._handle_monthly_rank)
            self.stats_manager = StatsManager(self.db)
            logger.info(f"StatsManager方法列表: {[method for method in dir(self.stats_manager) if not method.startswith('_')]}")
            
            # 初始化 自动删除管理器
            from managers.auto_delete_manager import AutoDeleteManager
            self.auto_delete_manager = AutoDeleteManager(self.db, apply_defaults=apply_defaults)
            logger.info("自动删除管理器已初始化")
            
            # 尝试初始化增强版轮播功能
            try:
                # 检查配置是否启用增强功能
                enable_enhanced = config.BROADCAST_SETTINGS.get('enable_enhanced_features', False)
                
                if enable_enhanced:
                    # 导入增强版轮播管理器
                    from managers.enhanced_broadcast_manager import EnhancedBroadcastManager
                    # 创建增强版轮播管理器
                    self.broadcast_manager = EnhancedBroadcastManager(self.db, self, apply_defaults=apply_defaults)
                    # ...其他初始化代码保持不变...
                else:
                    # 使用原始版轮播管理器
                    from managers.broadcast_manager import BroadcastManager
                    self.broadcast_manager = BroadcastManager(self.db, self, apply_defaults=apply_defaults)
                    logger.info("使用原始版轮播管理器")
            except Exception as e:
                logger.error(f"初始化增强版轮播功能出错: {e}", exc_info=True)
                # 使用原始版本的轮播管理器
                from managers.broadcast_manager import BroadcastManager
                self.broadcast_manager = BroadcastManager(self.db, self, apply_defaults=apply_defaults)
                logger.warning("使用原始版本的轮播管理器")
                        
            # 设置超级管理员
            for admin_id in DEFAULT_SUPERADMINS:
                await self.db.add_user({'user_id': admin_id, 'role': UserRole.SUPERADMIN.value})
                logger.info(f"已设置超级管理员: {admin_id}")
                
            # 初始化应用程序
            self.application = Application.builder().token(TELEGRAM_TOKEN).build()
            
            # 将bot实例存储在application的bot_data中，以便于在回调函数中访问
            self.application.bot_data['bot_instance'] = self
            
            # 注册处理函数
            from handlers import register_all_handlers
            register_all_handlers(self.application, self.callback_handler)
            
            # 初始化应用程序 - 移到这里，在设置webhook之前
            await self.application.initialize()
            
            # 设置Web应用
            self.web_app = web.Application()
            self.web_app.router.add_get('/', self.handle_healthcheck)
            self.web_app.router.add_get('/health', self.handle_healthcheck)
            
            # 设置Webhook
            webhook_domain = os.getenv('WEBHOOK_DOMAIN', 'your-render-app-name.onrender.com')
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

            # 初始化统计恢复系统
            from recovery.statistics_recovery import StatisticsRecoverySystem
            self.recovery_system = StatisticsRecoverySystem(self)
            logger.info("统计恢复系统已初始化")
            
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
        
        # 验证完成，设置初始化标志
        initialized = await self.db.get_system_flag("bot_initialized")
        if not initialized:
            await self.db.set_system_flag("bot_initialized", True)
            logger.info("已设置机器人初始化标志")
        
        # 修改群组验证逻辑 - 不再要求必须有群组
        groups = await self.db.find_all_groups()
        logger.info("初始化验证成功")
        logger.info(f"超级管理员: {DEFAULT_SUPERADMINS}")
        logger.info(f"已授权群组数量: {len(groups)}")
        return True
        
    @classmethod
    async def main(cls):
        """主入口方法"""
        try:
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
        except Exception as e:
            logger.critical(f"应用程序发生未捕获的异常: {e}", exc_info=True)
            # 记录所有正在运行的任务
            for task in asyncio.all_tasks():
                logger.critical(f"活动任务: {task.get_name()}, 已完成: {task.done()}, 已取消: {task.cancelled()}")
                if task.done() and not task.cancelled():
                    try:
                        exc = task.exception()
                        if exc:
                            logger.critical(f"任务异常: {exc}")
                    except asyncio.InvalidStateError:
                        pass
            
    async def start(self):
        """启动机器人"""
        if not self.application:
            logger.error("机器人未初始化")
            return False

        # 检查并恢复统计数据
        try:
            logger.info("开始检查是否需要恢复统计数据...")
            if hasattr(self, 'recovery_system'):
                await self.recovery_system.check_and_recover()
        except Exception as e:
            logger.error(f"检查恢复统计数据时出错: {e}", exc_info=True)
            # 错误不影响机器人启动
                    
        # 启动应用 - 已在初始化时进行，这里只需要调用start
        await self.application.start()
        self.running = True
        
        # 启动任务
        await self._start_broadcast_task()
        await self._start_cleanup_task()
        await self._start_ping_task()
        logger.info("机器人成功启动")
        return True
    
    async def stop(self, close_db=True):
        """停止机器人，可选择是否关闭数据库连接"""
        self.running = False
        
        # 设置关闭信号
        if self.shutdown_event:
            self.shutdown_event.set()
            
        # 停止设置管理器
        if self.settings_manager:
            logger.info("开始关闭设置管理器")
            await self.settings_manager.stop()
            
        # 取消清理任务
        if self.cleanup_task:
            logger.info("取消清理任务")
            self.cleanup_task.cancel()
    
        # 关闭自动删除管理器
        if self.auto_delete_manager:
            logger.info("开始关闭自动删除管理器")
            await self.auto_delete_manager.shutdown()

        # 关闭时间校准管理器
        if hasattr(self, 'calibration_manager') and self.calibration_manager:
            logger.info("开始关闭时间校准管理器")
            try:
                await self.calibration_manager.stop()
                logger.info("时间校准管理器已关闭")
            except Exception as e:
                logger.error(f"关闭时间校准管理器时出错: {e}", exc_info=True)
        
        # 修改关闭轮播管理器的部分:
        if self.broadcast_manager:
            logger.info("开始关闭轮播管理器")
            try:
                # 检查是否是增强版轮播管理器
                if hasattr(self.broadcast_manager, 'stop'):
                    await self.broadcast_manager.stop()
                else:
                    # 原始版本无需特殊关闭
                    pass
                logger.info("轮播管理器已关闭")
            except Exception as e:
                logger.error(f"关闭轮播管理器时出错: {e}", exc_info=True)
            
        # 清理Web服务器
        if self.web_runner:
            logger.info("开始清理Web服务器")
            await self.web_runner.cleanup()
            
        # 停止应用
        if self.application:
            try:
                logger.info("开始停止Telegram应用")
                if getattr(self.application, 'running', False):
                    await self.application.stop()
                    await self.application.shutdown()
                    logger.info("Telegram应用已成功关闭")
            except Exception as e:
                logger.error(f"停止应用时出错: {e}", exc_info=True)
                    
        # 条件性关闭数据库
        if close_db and self.db:
            try:
                logger.info("关闭数据库连接...")
                await self.db.close()
            except Exception as e:
                logger.error(f"关闭数据库连接时出错: {e}")
        else:
            logger.info("保持数据库连接活跃")
        
        logger.critical("应用程序关闭流程完成")
                
    async def shutdown(self):
        """关闭机器人"""
        await self.stop()

    async def _start_broadcast_task(self):
        """启动广播任务"""
        while self.running:
            try:
                # 更新最后活动时间
                self.last_active_time = datetime.now()
                
                # 处理广播
                if self.broadcast_manager:
                    if hasattr(self.broadcast_manager, 'process_broadcasts'):
                        await self.broadcast_manager.process_broadcasts()
                    
                # 每分钟检查一次
                await asyncio.sleep(60)
                
                # 检查时间偏移，可能的休眠后唤醒
                drift = self._check_time_drift()
                if drift > 120:  # 如果偏移超过2分钟
                    logger.warning(f"检测到系统可能休眠，时间偏移: {drift:.2f}秒")
                    # 如果使用增强版轮播管理器，它会自动处理
                    if hasattr(self.broadcast_manager, 'force_check'):
                        await self.broadcast_manager.force_check()
                
            except Exception as e:
                logger.error(f"轮播任务出错: {e}")
                await asyncio.sleep(60)

    def _check_time_drift(self):
        """
        检查时间偏移，用于检测系统休眠
        
        返回:
            时间偏移（秒）
        """
        current_time = datetime.now()
        time_diff = (current_time - self.last_active_time).total_seconds()
        expected_diff = 60  # 预期间隔时间
        
        # 计算时间偏移
        time_drift = time_diff - expected_diff
        
        # 更新最后活动时间
        self.last_active_time = current_time
        
        return time_drift

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

    async def _start_ping_task(self):
        """启动自我ping任务，防止Render休眠"""
        import aiohttp  # 确保在文件顶部有导入aiohttp
        
        while self.running:
            try:
                # 每10分钟访问一次自己的健康检查端点
                async with aiohttp.ClientSession() as session:
                    webhook_domain = os.getenv('WEBHOOK_DOMAIN', 'your-render-app-name.onrender.com')
                    url = f"https://{webhook_domain}/health"
                    async with session.get(url) as response:
                        logger.info(f"自我健康检查: {response.status}")
                
                # 每10分钟ping一次
                await asyncio.sleep(10 * 60)
            except Exception as e:
                logger.error(f"自我ping任务出错: {e}")
                await asyncio.sleep(5 * 60)  # 出错后5分钟再试
    
    async def handle_signals(self):
        """处理系统信号"""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                # 创建带有信号类型的闭包
                def create_signal_handler(signal_type):
                    return lambda: asyncio.create_task(self._handle_signal(signal_type))
                
                asyncio.get_running_loop().add_signal_handler(
                    sig,
                    create_signal_handler(sig)
                )
            logger.info("信号处理器设置完成")
        except NotImplementedError:
            logger.warning("此平台不支持信号处理器")
    
    async def _handle_signal(self, signal_type):
        """处理信号并进行优雅关闭"""
        signal_name = signal.Signals(signal_type).name if isinstance(signal_type, int) else str(signal_type)
        logger.critical(f"收到系统信号: {signal_name}，准备优雅关闭应用程序")
        
        # 设置标志位，停止接受新请求
        self.running = False
        
        # 完成当前正在处理的请求
        logger.info("等待当前请求完成...")
        await asyncio.sleep(2)
        
        # 然后执行正常关闭流程，但保持数据库连接
        logger.info("开始关闭应用程序，但保持数据库连接")
        await self.stop(close_db=False) 

    # 添加简单的限流机制
    _last_health_check_time = None
    _health_check_min_interval = 10.0  # 最小间隔1秒
    
    async def handle_healthcheck(self, request):
        """健康检查处理函数"""
        
        current_time = time.time()
        client_ip = request.remote
        user_agent = request.headers.get('User-Agent', 'Unknown')
        
        # 记录请求信息，但降低日志级别或减少日志输出
        if self._last_health_check_time is None or current_time - self._last_health_check_time > 60:
            logger.info(f"健康检查请求 - IP: {client_ip}, User-Agent: {user_agent}")
        
        # 更严格的限流逻辑
        if self._last_health_check_time is not None:
            time_diff = current_time - self._last_health_check_time
            if time_diff < self._health_check_min_interval:
                logger.debug(f"健康检查请求频率过高: {time_diff:.2f}秒，延迟响应")
                await asyncio.sleep(2.0)  # 更长的延迟时间
        
        self._last_health_check_time = current_time
        
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
            
            # 检查应用程序是否已初始化和启动
            if not self.running or not hasattr(self.application, '_initialized') or not self.application._initialized:
                logger.warning("应用程序尚未完全初始化，暂时无法处理更新")
                return web.Response(status=503, text="Bot not fully initialized yet")
            
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
        为群组添加必要的功能关键词
        
        参数:
            group_id: 群组ID
        """
        logger.info(f"为群组 {group_id} 添加基础功能关键词")
        
        # 检查群组是否启用关键词功能
        if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
            logger.info(f"群组 {group_id} 未启用关键词功能，跳过添加功能关键词")
            return
        
        # 检查关键词是否已存在
        existing_keywords = await self.db.get_keywords(group_id)
        existing_patterns = [kw.get('pattern', '') for kw in existing_keywords]
        
        # 添加日排行关键词
        if '日排行' not in existing_patterns:
            await self.db.add_keyword({
                'group_id': group_id,
                'pattern': '日排行',
                'type': 'exact',
                'response': '查询中...',  # 添加简单响应以满足验证
                'media': None,
                'buttons': [],
                'is_command': True,
                'command': '/tongji'
            })
            logger.info(f"已为群组 {group_id} 添加'日排行'关键词")
        
        # 添加月排行关键词
        if '月排行' not in existing_patterns:
            await self.db.add_keyword({
                'group_id': group_id,
                'pattern': '月排行',
                'type': 'exact',
                'response': '查询中...',  # 添加简单响应以满足验证
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
            # 执行tongji命令的逻辑
            from handlers.command_handlers import handle_rank_command
            
            # 创建一个简单的上下文
            from telegram.ext import ContextTypes
            context = ContextTypes.DEFAULT_TYPE(self.application)
            context.args = []
            
            # 创建一个假的Update对象，确保使用/tongji命令
            message_dict = message.to_dict()
            message_dict['text'] = '/tongji'
            fake_message = message.__class__.de_json(message_dict, self.application.bot)
            
            fake_update = Update(
                update_id=message.message_id,
                message=fake_message
            )
            
            # 执行命令
            await handle_rank_command(fake_update, context)
            # 返回'日排行'而不是'daily_rank_executed'，因为这是我们确定存在的关键词
            return "日排行"
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
            # 执行tongji30命令的逻辑
            from handlers.command_handlers import handle_rank_command
            
            # 创建一个简单的上下文
            from telegram.ext import ContextTypes
            context = ContextTypes.DEFAULT_TYPE(self.application)
            context.args = []
            
            # 创建一个假的Update对象，模拟tongji30命令
            message_dict = message.to_dict()
            message_dict['text'] = '/tongji30'
            fake_message = message.__class__.de_json(message_dict, self.application.bot)
            
            fake_update = Update(
                update_id=message.message_id,
                message=fake_message
            )
            
            # 执行命令
            await handle_rank_command(fake_update, context)
            # 返回'月排行'而不是'monthly_rank_executed'
            return "月排行"
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
