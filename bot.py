# 注意: 需要创建 config.py 文件，并定义以下变量:
# TELEGRAM_TOKEN - Telegram 机器人的 API 令牌
# MONGODB_URI - MongoDB 数据库的连接 URI
# MONGODB_DB - MongoDB 数据库名称
# DEFAULT_SUPERADMINS - 默认超级管理员的用户 ID 列表
# DEFAULT_SETTINGS - 默认机器人设置
# BROADCAST_SETTINGS - 轮播消息设置
# KEYWORD_SETTINGS - 关键词设置
# AUTO_DELETE_SETTINGS - 自动删除消息设置
# WEB_HOST - Web 服务器主机
# WEB_PORT - Web 服务器端口
import os
import json
import signal
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from bson import ObjectId

from aiohttp import web
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters
)
from dotenv import load_dotenv

from db import Database, UserRole, GroupPermission
from utils import (
    validate_time_format,
    validate_interval,
    format_file_size,
    validate_regex,
    get_media_type,
    format_duration,
    validate_delete_timeout,
    is_auto_delete_exempt,
    get_message_metadata,
    parse_command_args,
    escape_markdown
)
from config import (
    TELEGRAM_TOKEN, 
    MONGODB_URI, 
    MONGODB_DB, 
    DEFAULT_SUPERADMINS,
    DEFAULT_SETTINGS,
    BROADCAST_SETTINGS,
    KEYWORD_SETTINGS,
    AUTO_DELETE_SETTINGS,
    WEB_HOST,
    WEB_PORT
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

# 加载环境变量
load_dotenv()

class SettingsManager:
    def __init__(self, db):
        self.db = db
        self._temp_settings = {}
        self._pages = {}
        
    def get_current_page(self, group_id: int, section: str) -> int:
        """获取当前页码"""
        key = f"{group_id}_{section}"
        return self._pages.get(key, 1)
        
    def set_current_page(self, group_id: int, section: str, page: int):
        """设置当前页码"""
        key = f"{group_id}_{section}"
        self._pages[key] = page
        
    def start_setting(self, user_id: int, setting_type: str, group_id: int):
        """开始设置过程"""
        key = f"{user_id}_{setting_type}"
        self._temp_settings[key] = {
            'group_id': group_id,
            'step': 1,
            'data': {}
        }
        
    def get_setting_state(self, user_id: int, setting_type: str) -> dict:
        """获取设置状态"""
        key = f"{user_id}_{setting_type}"
        return self._temp_settings.get(key)
        
    def update_setting_state(self, user_id: int, setting_type: str, data: dict):
        """更新设置状态"""
        key = f"{user_id}_{setting_type}"
        if key in self._temp_settings:
            self._temp_settings[key]['data'].update(data)
            self._temp_settings[key]['step'] += 1
            
    def clear_setting_state(self, user_id: int, setting_type: str):
        """清除设置状态"""
        key = f"{user_id}_{setting_type}"
        if key in self._temp_settings:
            del self._temp_settings[key]

class StatsManager:
    def __init__(self, db):
        self.db = db

    async def add_message_stat(self, group_id: int, user_id: int, message: Message):
        """添加消息统计"""
        media_type = get_media_type(message)
        message_size = len(message.text or '') if message.text else 0
        
        if media_type and message.effective_attachment:
            try:
                file_size = getattr(message.effective_attachment, 'file_size', 0) or 0
                message_size += file_size
            except Exception:
                pass

        stat_data = {
            'group_id': group_id,
            'user_id': user_id,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_messages': 1,
            'total_size': message_size,
            'media_type': media_type
        }
        await self.db.add_message_stat(stat_data)

    async def get_daily_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取每日统计"""
        today = datetime.now().strftime('%Y-%m-%d')
        pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': today
            }},
            {'$group': {
                '_id': '$user_id',
                'total_messages': {'$sum': '$total_messages'},
                'total_size': {'$sum': '$total_size'}
            }},
            {'$sort': {'total_messages': -1}},
            {'$skip': (page - 1) * 15},
            {'$limit': 15}
        ]
        stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
        
        total_count_pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': today
            }},
            {'$group': {
                '_id': '$user_id'
            }},
            {'$count': 'total_users'}
        ]
        total_count_result = await self.db.db.message_stats.aggregate(total_count_pipeline).to_list(1)
        total_pages = (total_count_result[0]['total_users'] + 14) // 15 if total_count_result else 1
        
        return stats, total_pages

    async def get_monthly_stats(self, group_id: int, page: int = 1) -> Tuple[List[Dict], int]:
        """获取近30日统计"""
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': {'$gte': thirty_days_ago}
            }},
            {'$group': {
                '_id': '$user_id',
                'total_messages': {'$sum': '$total_messages'},
                'total_size': {'$sum': '$total_size'}
            }},
            {'$sort': {'total_messages': -1}},
            {'$skip': (page - 1) * 15},
            {'$limit': 15}
        ]
        stats = await self.db.db.message_stats.aggregate(pipeline).to_list(None)
        
        total_count_pipeline = [
            {'$match': {
                'group_id': group_id,
                'date': {'$gte': thirty_days_ago}
            }},
            {'$group': {
                '_id': '$user_id'
            }},
            {'$count': 'total_users'}
        ]
        total_count_result = await self.db.db.message_stats.aggregate(total_count_pipeline).to_list(1)
        total_pages = (total_count_result[0]['total_users'] + 14) // 15 if total_count_result else 1
        
        return stats, total_pages

class BroadcastManager:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot

class KeywordManager:
    def __init__(self, db):
        self.db = db
        self._built_in_keywords = {}
        
    def register_built_in_keyword(self, pattern: str, handler: callable):
        """注册内置关键词"""
        self._built_in_keywords[pattern] = handler
        
    async def match_keyword(self, group_id: int, text: str, message: Message) -> Optional[str]:
        """匹配关键词并返回回复"""
        # 首先检查内置关键词
        for pattern, handler in self._built_in_keywords.items():
            if text == pattern:
                return await handler(message)
        
        # 然后检查自定义关键词
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            try:
                import re
                if kw['type'] == 'regex':
                    pattern = re.compile(kw['pattern'])
                    if pattern.search(text):
                        return self._format_response(kw)
                else:  # exact match
                    if text == kw['pattern']:
                        return self._format_response(kw)
            except Exception as e:
                logger.error(f"Error matching keyword {kw['pattern']}: {e}")
                continue
        
        return None
        
    def _format_response(self, keyword: dict) -> str:
        """格式化关键词回复"""
        if keyword['response_type'] == 'text':
            return keyword['response']
        elif keyword['response_type'] in ['photo', 'video', 'document']:
            return f"__media__{keyword['response_type']}__{keyword['response']}"
        else:
            return "❌ 不支持的回复类型"
            
    async def get_keywords(self, group_id: int) -> List[Dict[str, Any]]:
        """获取群组的关键词列表"""
        return await self.db.get_keywords(group_id)

    async def get_keyword_by_id(self, group_id: int, keyword_id: str) -> Optional[Dict[str, Any]]:
        """通过ID获取关键词"""
        keywords = await self.get_keywords(group_id)
        for kw in keywords:
            if str(kw['_id']) == keyword_id:
                return kw
        return None

class TelegramBot:
    
    def __init__(self):
        self.db = None
        self.application = None
        self.web_app = None
        self.web_runner = None
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.cleanup_task = None
        self.settings_manager = None
        self.keyword_manager = None
        self.broadcast_manager = None
        self.stats_manager = None
        self.message_deletion_manager = None
           
    class MessageDeletionManager:
        """管理消息删除的类"""
        def __init__(self, bot):
            self.bot = bot
            self.deletion_tasks = {}

        async def schedule_message_deletion(
            self, 
            message: Message, 
            timeout: int, 
            delete_original: bool = False
        ):
            """调度消息删除"""
            if timeout <= 0:
                return
        
            task_key = f"delete_message_{message.chat.id}_{message.message_id}"
        
            async def delete_message_task():
                try:
                    await asyncio.sleep(timeout)
                
                    if delete_original and message.reply_to_message:
                        await message.reply_to_message.delete()
                
                    await message.delete()
                except Exception as e:
                    logger.warning(f"Error in message deletion: {e}")
                finally:
                    if task_key in self.deletion_tasks:
                        del self.deletion_tasks[task_key]
        
            task = asyncio.create_task(delete_message_task(), name=task_key)
            self.deletion_tasks[task_key] = task

        def cancel_deletion_task(self, message: Message):
            """取消特定消息的删除任务"""
            task_key = f"delete_message_{message.chat.id}_{message.message_id}"
            if task_key in self.deletion_tasks:
                task = self.deletion_tasks[task_key]
                task.cancel()
                del self.deletion_tasks[task_key]

    async def initialize(self):
        """初始化机器人"""
        try:
            logger.info("开始初始化机器人")
        
            # 初始化数据库
            self.db = Database()
            if not await self.db.connect(MONGODB_URI, MONGODB_DB):
                logger.error("数据库连接失败")
                return False
        
            # 初始化管理器
            self.settings_manager = SettingsManager(self.db)
            self.keyword_manager = KeywordManager(self.db)
            self.broadcast_manager = BroadcastManager(self.db, self)
            self.stats_manager = StatsManager(self.db)
            self.message_deletion_manager = self.MessageDeletionManager(self)
        
            # 强制更新所有默认超级管理员
            for admin_id in DEFAULT_SUPERADMINS:
                await self.db.add_user({
                    'user_id': admin_id,
                    'role': UserRole.SUPERADMIN.value
                })
                logger.info(f"已设置超级管理员: {admin_id}")
        
            # 初始化默认群组
            default_groups = [
                {
                    'group_id': -1001234567890,  # 替换为你的群组ID
                    'permissions': ['keywords', 'stats', 'broadcast']
                }
                # 可以添加更多群组
            ]
        
            for group in default_groups:
                await self.db.add_group({
                    'group_id': group['group_id'],
                    'permissions': group['permissions']
                })
                logger.info(f"已设置群组权限: {group['group_id']}")
        
            # 获取webhook域名
            webhook_domain = os.getenv('WEBHOOK_DOMAIN')
            if not webhook_domain:
                logger.warning("WEBHOOK_DOMAIN环境变量未设置，使用默认值")
                webhook_domain = 'your-render-app-name.onrender.com'
        
            # 创建Telegram Bot应用
            self.application = (
                Application.builder()
                .token(TELEGRAM_TOKEN)
                .build()
            )
        
            # 注册处理器
            await self._register_handlers()
        
            # 创建 web 应用并添加路由
            self.web_app = web.Application()
            self.web_app.router.add_get('/', self.handle_healthcheck)
            self.web_app.router.add_get('/health', self.handle_healthcheck)

            # 设置webhook路径并添加路由
            webhook_url = f"https://{webhook_domain}/webhook/{TELEGRAM_TOKEN}"
            webhook_path = f"/webhook/{TELEGRAM_TOKEN}"
            self.web_app.router.add_post(webhook_path, self._handle_webhook)

            # 设置web服务器
            self.web_runner = web.AppRunner(self.web_app)
            await self.web_runner.setup()

            site = web.TCPSite(self.web_runner, WEB_HOST, WEB_PORT)
            await site.start()
            logger.info(f"Web服务器已在 {WEB_HOST}:{WEB_PORT} 启动")

            # 配置webhook
            await self.application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )

            # 禁用轮询
            self.application.updater = None
        
            logger.info(f"Webhook已设置为 {webhook_url}")
        
            # 验证初始化
            if not await self.verify_initialization():
                logger.error("初始化验证失败")
                return False
            
            logger.info("机器人初始化完成")
            return True
        
        except Exception as e:
            logger.error(f"机器人初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
        
    async def verify_initialization(self):
        """验证初始化是否成功"""
        # 验证超级管理员
        for admin_id in DEFAULT_SUPERADMINS:
            user = await self.db.get_user(admin_id)
            if not user or user['role'] != UserRole.SUPERADMIN.value:
                logger.error(f"超级管理员 {admin_id} 初始化失败")
                return False
    
        # 验证群组权限
        groups = await self.db.find_all_groups()
        if not groups:
            logger.error("没有找到任何已授权的群组")
            return False
    
        logger.info("初始化验证成功")
        logger.info(f"超级管理员: {DEFAULT_SUPERADMINS}")
        logger.info(f"已授权群组: {[g['group_id'] for g in groups]}")
        return True

    async def main(cls):
        """主函数"""
        bot = None
        try:
            # 创建机器人实例
            bot = cls()
               
            # 初始化
            if not await bot.initialize():
                logger.error("机器人初始化失败")
                return
        
            # 设置信号处理
            await bot.handle_signals()
        
            # 启动机器人
            if not await bot.start():
                logger.error("机器人启动失败")
                return
        
            # 等待关闭
            while bot.running:
                await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"机器人启动失败: {e}")
            logger.error(traceback.format_exc())
        finally:
            if bot and hasattr(bot, 'stop'):
                await bot.stop()

    async def start(self):
        """启动机器人"""
        try:
            if not self.application:
                logger.error("机器人未初始化。初始化失败。")
                return False
        
            await self.application.initialize()
            await self.application.start()
            self.running = True
        
            # 启动轮播消息和清理任务
            await self._start_broadcast_task()
            await self._start_cleanup_task()
        
            logger.info("机器人成功启动")
            return True
    
        except Exception as e:
            logger.error(f"机器人启动失败: {e}")
            logger.error(traceback.format_exc())
            return False

    async def stop(self):
        """停止机器人"""
        try:
            self.running = False
            if self.shutdown_event:
                self.shutdown_event.set()
    
            # 停止清理任务
            if self.cleanup_task:
                self.cleanup_task.cancel()
    
            # 停止web服务器
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
                    
        except Exception as e:
            logger.error(f"停止机器人时出错: {e}")
     
    async def shutdown(self):
        """完全关闭机器人"""
        await self.stop()

    async def _start_broadcast_task(self):
        """启动轮播消息任务"""
        while self.running:
            try:
                # 获取所有需要发送的轮播消息
                now = datetime.now()
                broadcasts = await self.db.db.broadcasts.find({
                    'start_time': {'$lte': now},
                    'end_time': {'$gt': now},
                    '$or': [
                        {'last_broadcast': {'$exists': False}},
                        # 使用聚合管道或其他查询方法来处理间隔时间
                        {'last_broadcast': {'$lt': now}}  # 简化条件，稍后在代码中过滤
                    ]
                }).to_list(None)

                # 然后在获取到 broadcasts 后进行过滤
                filtered_broadcasts = []
                for broadcast in broadcasts:
                    if 'last_broadcast' not in broadcast or broadcast['last_broadcast'] <= now - timedelta(seconds=broadcast['interval']):
                        filtered_broadcasts.append(broadcast)

                broadcasts = filtered_broadcasts

                for broadcast in broadcasts:
                    try:
                        # 发送轮播消息
                        if broadcast['content_type'] == 'text':
                            await self.application.bot.send_message(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'photo':
                            await self.application.bot.send_photo(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'video':
                            await self.application.bot.send_video(broadcast['group_id'], broadcast['content'])
                        elif broadcast['content_type'] == 'document':
                            await self.application.bot.send_document(broadcast['group_id'], broadcast['content'])

                        # 更新最后发送时间
                        await self.db.db.broadcasts.update_one(
                            {'_id': broadcast['_id']},
                            {'$set': {'last_broadcast': now}}
                        )
                    except Exception as e:
                        logger.error(f"发送轮播消息时出错: {e}")

                # 等待一分钟后再次检查
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"轮播任务出错: {e}")
                await asyncio.sleep(60)  # 如果出错，等待1分钟后重试

    async def _start_cleanup_task(self):
        """启动数据清理任务"""
        async def cleanup_routine():
            while self.running:
                try:
                    await self.db.cleanup_old_stats(
                        days=DEFAULT_SETTINGS.get('cleanup_days', 30)
                    )
                    await asyncio.sleep(24 * 60 * 60)  # 每24小时运行一次
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
                    await asyncio.sleep(1 * 60 * 60)  # 如果出错，等待1小时后重试
    
        self.cleanup_task = asyncio.create_task(cleanup_routine())
    
    async def handle_signals(self):
        """设置信号处理器"""
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_running_loop().add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.stop())
                )
            logger.info("Signal handlers set up")
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")

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

    async def handle_healthcheck(self, request):
        """处理健康检查请求"""
        return web.Response(text="Healthy", status=200)

    async def _handle_webhook(self, request):
        """处理Telegram webhook请求"""
        try:
            # 检查请求内容类型
            if request.content_type != 'application/json':
                logger.warning(f"收到无效的内容类型: {request.content_type}")
                return web.Response(status=415, text="Unsupported Media Type")
            
            update_data = await request.json()
            if not update_data:
                logger.warning("收到空的更新数据")
                return web.Response(status=400, text="Empty Update")
            
            update = Update.de_json(update_data, self.application.bot)
        
            if update:
                await self.application.process_update(update)
            else:
                logger.warning("收到无效的更新")
        
            return web.Response(status=200)
    
        except json.JSONDecodeError:
            logger.error("Webhook请求中的JSON无效")
            return web.Response(status=400, text="Invalid JSON")
    
        except Exception as e:
            logger.error(f"处理Webhook时出错: {e}")
            logger.error(traceback.format_exc())
            return web.Response(status=500, text="Internal Server Error")

    async def _register_handlers(self):
        """注册各种事件处理器"""
        # 普通命令（所有用户可用）
        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("tongji", self._handle_rank_command))
        self.application.add_handler(CommandHandler("tongji30", self._handle_rank_command))
        
        # 管理员命令
        self.application.add_handler(CommandHandler("settings", self._handle_settings))
        self.application.add_handler(CommandHandler("admingroups", self._handle_admin_groups))
        
        # 超级管理员命令
        self.application.add_handler(CommandHandler("addsuperadmin", self._handle_add_superadmin))
        self.application.add_handler(CommandHandler("delsuperadmin", self._handle_del_superadmin))
        self.application.add_handler(CommandHandler("addadmin", self._handle_add_admin))
        self.application.add_handler(CommandHandler("deladmin", self._handle_del_admin))
        self.application.add_handler(CommandHandler("authgroup", self._handle_auth_group))
        self.application.add_handler(CommandHandler("deauthgroup", self._handle_deauth_group))

         # 新增配置检查命令
        self.application.add_handler(CommandHandler("checkconfig", self._handle_check_config))
        
        # 消息处理器
        self.application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            self._handle_message
        ))
        
        # 回调查询处理器
        self.application.add_handler(CallbackQueryHandler(
            self._handle_settings_callback, 
            pattern=r'^settings_'
        ))
        self.application.add_handler(CallbackQueryHandler(
            self._handle_keyword_callback, 
            pattern=r'^keyword_'
        ))
        self.application.add_handler(CallbackQueryHandler(
            self._handle_keyword_response_type_callback, 
            pattern=r'^keyword_response_'
        ))
        self.application.add_handler(CallbackQueryHandler(
            self._handle_broadcast_callback, 
            pattern=r'^broadcast_'
        ))
        self.application.add_handler(CallbackQueryHandler(
            self._handle_stats_edit_callback, 
            pattern=r'^stats_'
        ))

    async def _handle_start(self, update: Update, context):
        """处理 start 命令"""
        if not update.effective_user or not update.message:
            return

        user_id = update.effective_user.id
        is_superadmin = await self.is_superadmin(user_id)
        is_admin = await self.is_admin(user_id)

        welcome_text = (
            f"👋 你好 {update.effective_user.first_name}！\n\n"
            "我是啤酒群酒保，主要功能包括：\n"
            "• 关键词自动回复\n"
            "• 消息统计\n"
            "• 轮播消息\n\n"
            "基础命令：\n"
            "🔧 /settings - 配置机器人\n"
            "📊 /tongji - 查看今日统计\n"
            "📈 /tongji30 - 查看30日统计\n"
        )

        if is_admin:
            admin_commands = (
                "\n管理员命令：\n"
                "👥 /admingroups - 查看可管理的群组\n"
                "⚙️ /settings - 群组设置管理\n"
            )
            welcome_text += admin_commands

        if is_superadmin:
            superadmin_commands = (
                "\n超级管理员命令：\n"
                "➕ /addsuperadmin <用户ID> - 添加超级管理员\n"
                "➖ /delsuperadmin <用户ID> - 删除超级管理员\n"
                "👤 /addadmin <用户ID> - 添加管理员\n"
                "🚫 /deladmin <用户ID> - 删除管理员\n"
                "✅ /authgroup <群组ID> <权限1> [权限2] ... - 授权群组\n"
                "❌ /deauthgroup <群组ID> - 取消群组授权\n"
                "🔍 /checkconfig - 检查当前配置\n"
            )
            welcome_text += superadmin_commands

        welcome_text += "\n如需帮助，请联系管理员。"
    
        await update.message.reply_text(welcome_text)

    async def _handle_settings(self, update: Update, context):
        """处理设置命令"""
        try:
            # 获取用户可管理的群组
            manageable_groups = await self.db.get_manageable_groups(update.effective_user.id)
            
            if not manageable_groups:
                await update.message.reply_text("❌ 你没有权限管理任何群组")
                return
                
            # 创建群组选择键盘
            keyboard = []
            for group in manageable_groups:
                try:
                    group_info = await context.bot.get_chat(group['group_id'])
                    group_name = group_info.title or f"群组 {group['group_id']}"
                except Exception:
                    group_name = f"群组 {group['group_id']}"
                
                keyboard.append([
                    InlineKeyboardButton(
                        group_name, 
                        callback_data=f"settings_select_{group['group_id']}"
                    )
                ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "请选择要管理的群组：", 
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"设置命令处理错误: {e}")
            await update.message.reply_text("❌ 处理设置命令时出错")

    async def _handle_settings_callback(self, update: Update, context):
        """处理设置回调"""
        query = update.callback_query
        await query.answer()

        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
            group_id = int(parts[2])

            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return

            if action == "select":
                # 显示设置菜单
                keyboard = []
                
                # 检查各功能权限并添加对应按钮
                if await self.has_permission(group_id, GroupPermission.STATS):
                    keyboard.append([
                        InlineKeyboardButton(
                            "📊 统计设置", 
                            callback_data=f"settings_stats_{group_id}"
                        )
                    ])
                    
                if await self.has_permission(group_id, GroupPermission.BROADCAST):
                    keyboard.append([
                        InlineKeyboardButton(
                             "📢 轮播消息", 
                            callback_data=f"settings_broadcast_{group_id}"
                        )
                    ])
                    
                if await self.has_permission(group_id, GroupPermission.KEYWORDS):
                    keyboard.append([
                        InlineKeyboardButton(
                            "🔑 关键词设置", 
                            callback_data=f"settings_keywords_{group_id}"
                        )
                    ])

                await query.edit_message_text(
                    f"群组 {group_id} 的设置菜单\n"
                    "请选择要管理的功能：",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            else:
                # 处理具体设置分区
                section = action  # stats, broadcast, keywords
                await self._handle_settings_section(query, context, group_id, section)

        except Exception as e:
            logger.error(f"处理设置回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理设置操作时出错")

    async def _handle_settings_section(self, query, context, group_id: int, section: str):
        """处理设置分区显示"""
        try:
            if section == "stats":
                # 获取当前群组的统计设置
                settings = await self.db.get_group_settings(group_id)
                
                # 创建设置展示和修改的键盘
                keyboard = [
                    [
                        InlineKeyboardButton(
                            f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", 
                            callback_data=f"stats_edit_min_bytes_{group_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            f"统计多媒体: {'是' if settings.get('count_media', False) else '否'}", 
                            callback_data=f"stats_edit_toggle_media_{group_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            f"日排行显示数量: {settings.get('daily_rank_size', 15)}", 
                            callback_data=f"stats_edit_daily_rank_{group_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            f"月排行显示数量: {settings.get('monthly_rank_size', 15)}", 
                            callback_data=f"stats_edit_monthly_rank_{group_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "返回设置菜单", 
                            callback_data=f"settings_select_{group_id}"
                        )
                    ]
                ]
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    f"群组 {group_id} 的统计设置",
                    reply_markup=reply_markup
                )
                
            elif section == "broadcast":
                # 获取轮播消息列表
                broadcasts = await self.db.db.broadcasts.find({
                    'group_id': group_id
                }).to_list(None)
                
                keyboard = []
                for bc in broadcasts:
                    # 截取消息预览
                    preview = (bc['content'][:20] + '...') if len(str(bc['content'])) > 20 else str(bc['content'])
                    keyboard.append([
                        InlineKeyboardButton(
                            f"📢 {bc['content_type']}: {preview}", 
                            callback_data=f"broadcast_detail_{group_id}_{bc['_id']}"
                        )
                    ])
                
                keyboard.append([
                    InlineKeyboardButton(
                        "➕ 添加轮播消息", 
                        callback_data=f"broadcast_add_{group_id}"
                    )
                ])
                
                keyboard.append([
                    InlineKeyboardButton(
                        "返回设置菜单", 
                        callback_data=f"settings_select_{group_id}"
                    )
                ])
                
                await query.edit_message_text(
                    f"群组 {group_id} 的轮播消息设置",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
            elif section == "keywords":
                # 获取关键词列表
                keywords = await self.db.get_keywords(group_id)
                
                keyboard = []
                for kw in keywords:
                    keyword_text = kw['pattern'][:20] + '...' if len(kw['pattern']) > 20 else kw['pattern']
                    keyboard.append([
                        InlineKeyboardButton(
                            f"🔑 {keyword_text}", 
                            callback_data=f"keyword_detail_{group_id}_{kw['_id']}"
                        )
                    ])
                
                keyboard.append([
                    InlineKeyboardButton(
                        "➕ 添加关键词", 
                        callback_data=f"keyword_add_{group_id}"
                    )
                ])
                
                keyboard.append([
                    InlineKeyboardButton(
                        "返回设置菜单", 
                        callback_data=f"settings_select_{group_id}"
                    )
                ])
                
                await query.edit_message_text(
                    f"群组 {group_id} 的关键词设置",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
        except Exception as e:
            logger.error(f"处理设置分区显示错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 显示设置分区时出错")


    async def _handle_rank_command(self, update: Update, context):
        """处理统计命令（tongji/tongji30）"""
        if not update.effective_chat or not update.effective_user or not update.message:
            return
            
        try:
            command = update.message.text.split('@')[0][1:]  # 移除 / 和机器人用户名
            group_id = update.effective_chat.id
            
            # 检查权限
            if not await self.has_permission(group_id, GroupPermission.STATS):
                await update.message.reply_text("❌ 此群组未启用统计功能")
                return
                
            # 获取页码
            page = 1
            if context.args:
                try:
                    page = int(context.args[0])
                    if page < 1:
                        raise ValueError
                except ValueError:
                    await update.message.reply_text("❌ 无效的页码")
                    return

            # 获取统计数据
            if command == "tongji":
                stats, total_pages = await self.stats_manager.get_daily_stats(group_id, page)
                title = "📊 今日发言排行"
            else:  # tongji30
                stats, total_pages = await self.stats_manager.get_monthly_stats(group_id, page)
                title = "📊 近30天发言排行"
                
            if not stats:
                await update.message.reply_text("📊 暂无统计数据")
                return
                
            # 生成排行榜文本
            text = f"{title}\n\n"
            settings = await self.db.get_group_settings(group_id)
            min_bytes = settings.get('min_bytes', 0)
            
            for i, stat in enumerate(stats, start=(page-1)*15+1):
                try:
                    user = await context.bot.get_chat_member(group_id, stat['_id'])
                    name = user.user.full_name or user.user.username or f"用户{stat['_id']}"
                except Exception:
                    name = f"用户{stat['_id']}"
                
                text += f"{i}. {name}\n"
                text += f"   消息数: {stat['total_messages']}\n"
                text += f"   总字节: {format_file_size(stat['total_size'])}\n\n"
            
            if min_bytes > 0:
                text += f"\n注：仅统计大于 {format_file_size(min_bytes)} 的消息"
            
            # 添加分页信息
            text += f"\n\n第 {page}/{total_pages} 页"
            if total_pages > 1:
                text += f"\n使用 /{command} <页码> 查看其他页"
            
            keyboard = self._create_navigation_keyboard(
                page, 
                total_pages, 
                f"{'today' if command == 'tongji' else 'monthly'}_{group_id}"
            )
            
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )
            
        except Exception as e:
            logger.error(f"处理排行命令错误: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 获取排行榜时出错")

    async def _handle_broadcast_callback(self, update: Update, context):
        """处理轮播消息回调"""
        query = update.callback_query
        await query.answer()
    
        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
            group_id = int(parts[2])
        
            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return
            
            if not await self.has_permission(group_id, GroupPermission.BROADCAST):
                await query.edit_message_text("❌ 此群组未启用轮播功能")
                return
            
            if action == "add":
                # 选择消息类型
                keyboard = [
                    [
                        InlineKeyboardButton("文本", callback_data=f"broadcast_type_text_{group_id}"),
                        InlineKeyboardButton("图片", callback_data=f"broadcast_type_photo_{group_id}")
                    ],
                    [
                        InlineKeyboardButton("视频", callback_data=f"broadcast_type_video_{group_id}"),
                        InlineKeyboardButton("文件", callback_data=f"broadcast_type_document_{group_id}")
                    ],
                    [
                        InlineKeyboardButton("取消", callback_data=f"settings_broadcast_{group_id}")
                    ]
                ]
            
                await query.edit_message_text(
                    "请选择轮播消息类型：",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif action == "type":
                # 处理消息类型选择
                content_type = parts[2]
            
                # 开始添加轮播消息流程
                self.settings_manager.start_setting(
                    update.effective_user.id,
                    'broadcast',
                    group_id
                )
            
                self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'broadcast',
                    {'content_type': content_type}
                )
            
                type_prompts = {
                    'text': '文本内容',
                    'photo': '图片',
                    'video': '视频',
                    'document': '文件'
                }
            
                await query.edit_message_text(
                    f"请发送要轮播的{type_prompts[content_type]}：\n\n"
                    f"发送 /cancel 取消"
                )
            
            elif action == "detail":
                # 显示轮播消息详情
                broadcast_id = ObjectId(parts[3])
                broadcast = await self.db.db.broadcasts.find_one({
                    '_id': broadcast_id,
                    'group_id': group_id
                })
            
                if not broadcast:
                    await query.edit_message_text("❌ 轮播消息不存在")
                    return
                
                text = "📢 轮播消息详情：\n\n"
                text += f"类型：{broadcast['content_type']}\n"
                text += f"开始时间：{broadcast['start_time'].strftime('%Y-%m-%d %H:%M')}\n"
                text += f"结束时间：{broadcast['end_time'].strftime('%Y-%m-%d %H:%M')}\n"
                text += f"间隔：{format_duration(broadcast['interval'])}\n"
            
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "🗑️ 删除轮播消息",
                            callback_data=f"broadcast_delete_{group_id}_{broadcast_id}"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "返回轮播列表",
                            callback_data=f"settings_broadcast_{group_id}"
                        )
                    ]
                ]
            
                await query.edit_message_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif action == "delete":
                # 删除轮播消息
                broadcast_id = ObjectId(parts[3])
                await self.db.db.broadcasts.delete_one({
                    '_id': broadcast_id,
                    'group_id': group_id
                })
            
                await self._handle_settings_section(
                    query,
                    context,
                    group_id,
                    "broadcast"
                )

            elif action == "interval":
                # 处理间隔时间选择
                interval = int(parts[3])
                user_id = update.effective_user.id
            
                # 获取设置状态
                setting_state = self.settings_manager.get_setting_state(user_id, 'broadcast')
                if not setting_state:
                    await query.edit_message_text("❌ 设置会话已过期，请重新开始")
                    return
                
                # 设置时间
                now = datetime.now()
                start_time = now
                end_time = now + timedelta(days=30)  # 默认30天
            
                # 添加轮播消息
                try:
                    content = setting_state['data']['content']
                    content_type = setting_state['data']['content_type']
                
                    await self.db.db.broadcasts.insert_one({
                        'group_id': group_id,
                        'content_type': content_type,
                        'content': content,
                        'start_time': start_time,
                        'end_time': end_time,
                        'interval': interval
                    })
                
                    await query.edit_message_text(
                        "✅ 轮播消息添加成功！\n\n"
                        f"• 类型：{content_type}\n"
                        f"• 间隔：{format_duration(interval)}\n"
                        f"• 有效期：30天"
                    )
                
                    # 清除设置状态
                    self.settings_manager.clear_setting_state(user_id, 'broadcast')
                
                except Exception as e:
                    logger.error(f"添加轮播消息错误: {e}")
                    await query.edit_message_text("❌ 添加轮播消息时出错")
            
        except Exception as e:
            logger.error(f"处理轮播消息回调错误: {e}")
            await query.edit_message_text("❌ 处理轮播消息操作时出错")

    async def _handle_stats_edit_callback(self, update: Update, context):
        """处理统计设置编辑回调"""
        query = update.callback_query
        await query.answer()
    
        try:
            data = query.data
            parts = data.split('_')
            setting_type = parts[2]  # min_bytes, toggle_media 等
            group_id = int(parts[3])
        
            # 验证权限
            if not await self.db.can_manage_group(update.effective_user.id, group_id):
                await query.edit_message_text("❌ 无权限管理此群组")
                return
            
            if not await self.has_permission(group_id, GroupPermission.STATS):
                await query.edit_message_text("❌ 此群组未启用统计功能")
                return
            
            settings = await self.db.get_group_settings(group_id)
        
            if setting_type == "toggle_media":
                # 切换是否统计多媒体
                current_value = settings.get('count_media', False)
                settings['count_media'] = not current_value
                await self.db.update_group_settings(group_id, settings)
            
                # 刷新统计设置页面
                await self._show_stats_settings(query, group_id, settings)
            
            elif setting_type in ['min_bytes', 'daily_rank', 'monthly_rank']:
                # 进入编辑模式
                self.settings_manager.start_setting(
                    update.effective_user.id,
                    f'stats_{setting_type}',
                    group_id
                )
            
                setting_names = {
                    'min_bytes': '最小统计字节数',
                    'daily_rank': '日排行显示数量',
                    'monthly_rank': '月排行显示数量'
                }
            
                current_value = settings.get(setting_type, 0)
                await query.edit_message_text(
                    f"当前{setting_names[setting_type]}：{current_value}\n"
                    f"请输入新的值：\n\n"
                    f"发送 /cancel 取消"
                )
            
        except Exception as e:
            logger.error(f"处理统计设置编辑回调错误: {e}")
            await query.edit_message_text("❌ 处理统计设置编辑时出错")

    async def _show_stats_settings(self, query, group_id: int, settings: dict):
        """显示统计设置页面"""
        keyboard = [
            [
                InlineKeyboardButton(
                    f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", 
                    callback_data=f"stats_edit_min_bytes_{group_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    f"统计多媒体: {'是' if settings.get('count_media', False) else '否'}", 
                    callback_data=f"stats_edit_toggle_media_{group_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    f"日排行显示数量: {settings.get('daily_rank_size', 15)}", 
                    callback_data=f"stats_edit_daily_rank_{group_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    f"月排行显示数量: {settings.get('monthly_rank_size', 15)}", 
                    callback_data=f"stats_edit_monthly_rank_{group_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "返回设置菜单", 
                    callback_data=f"settings_select_{group_id}"
                )
            ]
        ]
    
        await query.edit_message_text(
            f"群组 {group_id} 的统计设置",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def _process_stats_setting(self, update: Update, context, stats_state, setting_type):
        """处理统计设置编辑"""
        try:
            group_id = stats_state['group_id']
            
            # 获取用户输入的值
            try:
                value = int(update.message.text)
                if value < 0:
                    raise ValueError("值不能为负")
            except ValueError:
                await update.message.reply_text("❌ 请输入一个有效的数字")
                return
                
            # 根据设置类型更新相应的值
            tips = await self.update_stats_setting(group_id, setting_type, value)
            await update.message.reply_text(f"✅ {tips}")
            
            # 清除设置状态
            self.settings_manager.clear_setting_state(update.effective_user.id, setting_type)
            
        except Exception as e:
            logger.error(f"处理统计设置错误: {e}")
            await update.message.reply_text("❌ 更新设置时出错")

    def _create_navigation_keyboard(
            self,
            current_page: int,
            total_pages: int,
            base_callback: str
        ) -> List[List[InlineKeyboardButton]]:
            """创建分页导航键盘"""
            keyboard = []
            nav_row = []
        
            if current_page > 1:
                nav_row.append(
                    InlineKeyboardButton(
                        "◀️ 上一页",
                        callback_data=f"{base_callback}_{current_page-1}"
                    )
                )
            
            if current_page < total_pages:
                nav_row.append(
                    InlineKeyboardButton(
                        "下一页 ▶️",
                        callback_data=f"{base_callback}_{current_page+1}"
                    )
                )
            
            if nav_row:
                keyboard.append(nav_row)
            
            return keyboard

    async def update_stats_setting(self, group_id: int, setting_type: str, value: int):
        """更新统计设置"""
        settings = await self.db.get_group_settings(group_id)
        if setting_type == 'stats_min_bytes':
            settings['min_bytes'] = value
            tips = f"最小统计字节数已设置为 {value} 字节"
        elif setting_type == 'stats_daily_rank':
            settings['daily_rank_size'] = value
            tips = f"日排行显示数量已设置为 {value}"
        elif setting_type == 'stats_monthly_rank':
            settings['monthly_rank_size'] = value
            tips = f"月排行显示数量已设置为 {value}"
        await self.db.update_group_settings(group_id, settings)
        return tips

    async def _handle_message(self, update: Update, context):
        """处理消息"""
        if not update.effective_chat or not update.effective_user or not update.message:
            return
        
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        try:
            # 检查是否正在进行关键词添加流程
            setting_state = self.settings_manager.get_setting_state(user_id, 'keyword')
            if setting_state and setting_state['group_id'] == chat_id:
                await self._process_keyword_adding(update, context, setting_state)
                return
                
            # 检查是否正在进行轮播消息添加流程
            broadcast_state = self.settings_manager.get_setting_state(user_id, 'broadcast')
            if broadcast_state and broadcast_state['group_id'] == chat_id:
                await self._process_broadcast_adding(update, context, broadcast_state)
                return
                
            # 检查是否正在进行统计设置编辑
            for setting_type in ['stats_min_bytes', 'stats_daily_rank', 'stats_monthly_rank']:
                stats_state = self.settings_manager.get_setting_state(user_id, setting_type)
                if stats_state and stats_state['group_id'] == chat_id:
                    await self._process_stats_setting(update, context, stats_state, setting_type)
                    return
                    
            # 处理关键词匹配
            if await self.has_permission(chat_id, GroupPermission.KEYWORDS):
                if update.message.text:
                    # 尝试匹配关键词
                    response = await self.keyword_manager.match_keyword(
                        chat_id,
                        update.message.text,
                        update.message
                    )
                    if response:
                        await self._handle_keyword_response(chat_id, response, context, update.message)
            
            # 处理消息统计
            if await self.has_permission(chat_id, GroupPermission.STATS):
                await self.stats_manager.add_message_stat(chat_id, user_id, update.message)
                
        except Exception as e:
            logger.error(f"处理消息错误: {e}")
            logger.error(traceback.format_exc())

    async def _handle_admin_groups(self, update: Update, context):
        """处理管理员群组管理命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是管理员
        if not await self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ 只有管理员可以使用此命令")
            return
            
        try:
            # 获取可管理的群组
            groups = await self.db.get_manageable_groups(update.effective_user.id)
            
            if not groups:
                await update.message.reply_text("📝 你目前没有可管理的群组")
                return
                
            # 生成群组列表
            text = "📝 你可以管理的群组：\n\n"
            for group in groups:
                try:
                    group_info = await context.bot.get_chat(group['group_id'])
                    group_name = group_info.title
                except Exception:
                    group_name = f"群组 {group['group_id']}"
                    
                text += f"• {group_name}\n"
                text += f"  ID: {group['group_id']}\n"
                text += f"  权限: {', '.join(group.get('permissions', []))}\n\n"
                
            await update.message.reply_text(text)
            
        except Exception as e:
            logger.error(f"列出管理员群组错误: {e}")
            await update.message.reply_text("❌ 获取群组列表时出错")

    async def _handle_add_admin(self, update: Update, context):
        """处理添加管理员命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以添加管理员")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/addadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            
            # 检查用户是否已经是管理员
            user = await self.db.get_user(user_id)
            if user and user['role'] in [UserRole.ADMIN.value, UserRole.SUPERADMIN.value]:
                await update.message.reply_text("❌ 该用户已经是管理员")
                return
                
            # 添加管理员
            await self.db.add_user({
                'user_id': user_id,
                'role': UserRole.ADMIN.value
            })
            
            await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为管理员")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"添加管理员错误: {e}")
            await update.message.reply_text("❌ 添加管理员时出错")

    async def _handle_del_admin(self, update: Update, context):
        """处理删除管理员命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除管理员")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deladmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            
            # 检查不能删除超级管理员
            user = await self.db.get_user(user_id)
            if not user:
                await update.message.reply_text("❌ 该用户不是管理员")
                return
                
            if user['role'] == UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 不能删除超级管理员")
                return
                
            # 删除管理员
            await self.db.remove_user(user_id)
            
            await update.message.reply_text(f"✅ 已删除管理员 {user_id}")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"删除管理员错误: {e}")
            await update.message.reply_text("❌ 删除管理员时出错")

    async def _handle_add_superadmin(self, update: Update, context):
        """处理添加超级管理员命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以添加超级管理员")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/addsuperadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            
            # 检查用户是否已经是超级管理员
            user = await self.db.get_user(user_id)
            if user and user['role'] == UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 该用户已经是超级管理员")
                return
                
            # 添加超级管理员
            await self.db.add_user({
                'user_id': user_id,
                'role': UserRole.SUPERADMIN.value
            })
            
            await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为超级管理员")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"添加超级管理员错误: {e}")
            await update.message.reply_text("❌ 添加超级管理员时出错")

    async def _handle_del_superadmin(self, update: Update, context):
        """处理删除超级管理员命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以删除超级管理员")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/delsuperadmin <用户ID>")
            return
            
        try:
            user_id = int(context.args[0])
            
            # 不能删除自己
            if user_id == update.effective_user.id:
                await update.message.reply_text("❌ 不能删除自己的超级管理员权限")
                return
            
            # 检查用户是否是超级管理员
            user = await self.db.get_user(user_id)
            if not user or user['role'] != UserRole.SUPERADMIN.value:
                await update.message.reply_text("❌ 该用户不是超级管理员")
                return
                
            # 删除超级管理员
            await self.db.remove_user(user_id)
            
            await update.message.reply_text(f"✅ 已删除超级管理员 {user_id}")
            
        except ValueError:
            await update.message.reply_text("❌ 用户ID必须是数字")
        except Exception as e:
            logger.error(f"删除超级管理员错误: {e}")
            await update.message.reply_text("❌ 删除超级管理员时出错")

    async def _handle_check_config(self, update: Update, context):
        """处理检查配置命令"""
        if not update.effective_user:
            return
        
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以查看配置")
            return
        
        try:
            # 获取超级管理员列表
            superadmins = await self.db.get_users_by_role(UserRole.SUPERADMIN.value)
            superadmin_ids = [user['user_id'] for user in superadmins]
        
            # 获取群组列表
            groups = await self.db.find_all_groups()
        
            # 构建配置信息
            config_text = "🔧 当前配置信息：\n\n"
            config_text += "👥 超级管理员：\n"
            for admin_id in superadmin_ids:
                config_text += f"• {admin_id}\n"
            
            config_text += "\n📋 已授权群组：\n"
            for group in groups:
                config_text += f"• 群组 {group['group_id']}\n"
                config_text += f"  权限: {', '.join(group.get('permissions', []))}\n"
        
            await update.message.reply_text(config_text)
        
        except Exception as e:
            logger.error(f"检查配置出错: {e}")
            await update.message.reply_text("❌ 获取配置信息时出错")

    async def _handle_auth_group(self, update: Update, context):
        """处理授权群组命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以授权群组")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text(
                "❌ 请使用正确的格式：\n"
                "/authgroup <群组ID>"
            )
            return
            
        try:
            group_id = int(context.args[0])
            
            # 获取群组信息
            try:
                group_info = await context.bot.get_chat(group_id)
                group_name = group_info.title
            except Exception:
                await update.message.reply_text("❌ 无法获取群组信息，请确保机器人已加入该群组")
                return
            
            # 设置全部权限
            all_permissions = ['keywords', 'stats', 'broadcast']
        
            # 更新群组权限
            await self.db.add_group({
                'group_id': group_id,
                'permissions': all_permissions
            })
            
            await update.message.reply_text(
                f"✅ 已授权群组\n"
                f"群组：{group_name}\n"
                f"ID：{group_id}\n"
                f"已启用全部功能"
            )
            
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字")
        except Exception as e:
            logger.error(f"授权群组错误: {e}")
            await update.message.reply_text("❌ 授权群组时出错")

    async def _handle_deauth_group(self, update: Update, context):
        """处理解除群组授权命令"""
        if not update.effective_user or not update.message:
            return
            
        # 检查是否是超级管理员
        if not await self.is_superadmin(update.effective_user.id):
            await update.message.reply_text("❌ 只有超级管理员可以解除群组授权")
            return
            
        # 检查命令格式
        if not context.args:
            await update.message.reply_text("❌ 请使用正确的格式：/deauthgroup <群组ID>")
            return
            
        try:
            group_id = int(context.args[0])
            
            # 检查群组是否已授权
            group = await self.db.get_group(group_id)
            if not group:
                await update.message.reply_text("❌ 该群组未授权")
                return
            
            # 删除群组
            await self.db.remove_group(group_id)
            
            await update.message.reply_text(f"✅ 已解除群组 {group_id} 的所有授权")
            
        except ValueError:
            await update.message.reply_text("❌ 群组ID必须是数字")
        except Exception as e:
            logger.error(f"解除群组授权错误: {e}")
            await update.message.reply_text("❌ 解除群组授权时出错")

    async def _handle_keyword_callback(self, update: Update, context):
        """处理关键词回调"""
        query = update.callback_query
        await query.answer()
    
        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
        
            if action == "add":
                # 处理添加关键词
                group_id = int(parts[2])
            
                # 验证权限
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                
                # 检查群组权限
                if not await self.has_permission(group_id, GroupPermission.KEYWORDS):
                    await query.edit_message_text("❌ 此群组未启用关键词功能")
                    return
                
                # 选择关键词类型
                keyboard = [
                    [
                        InlineKeyboardButton("精确匹配", callback_data=f"keyword_type_exact_{group_id}"),
                        InlineKeyboardButton("正则匹配", callback_data=f"keyword_type_regex_{group_id}")
                    ],
                    [
                        InlineKeyboardButton("返回", callback_data=f"settings_keywords_{group_id}")
                    ]
                ]
            
                await query.edit_message_text(
                    "请选择关键词匹配类型：\n"
                    "• 精确匹配：完全相同才触发\n"
                    "• 正则匹配：支持正则表达式",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif action == "type":
                # 处理类型选择
                match_type = parts[2]  # exact 或 regex
                group_id = int(parts[3])
            
                # 开始添加关键词流程
                self.settings_manager.start_setting(
                    update.effective_user.id,
                    'keyword',
                    group_id
                )
            
                # 保存匹配类型
                self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'keyword',
                    {'match_type': match_type}
                )
            
                # 提示输入关键词
                max_length = 500  # 增加字符限制
                await query.edit_message_text(
                    f"请发送要添加的关键词：\n"
                    f"• 最大长度{max_length}字符\n"
                    f"• 当前模式：{'精确匹配' if match_type == 'exact' else '正则匹配'}\n"
                    f"• 发送 /cancel 取消"
                )
                
            elif action == "detail":
                # 处理关键词详情
                group_id = int(parts[2])
                keyword_id = parts[3]
                
                # 验证权限
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                # 获取关键词信息
                keyword = await self.keyword_manager.get_keyword_by_id(group_id, keyword_id)
                if not keyword:
                    await query.edit_message_text("❌ 关键词不存在")
                    return
                    
                # 创建删除按钮
                keyboard = [[
                    InlineKeyboardButton(
                        "🗑️ 删除关键词",
                        callback_data=f"keyword_delete_{group_id}_{keyword_id}"
                    )
                ], [
                    InlineKeyboardButton(
                        "返回关键词列表",
                        callback_data=f"settings_keywords_{group_id}"
                    )
                ]]
                
                # 显示关键词详情
                text = "📝 关键词详情：\n\n"
                text += f"模式：{keyword['pattern']}\n"
                text += f"类型：{'正则表达式' if keyword['type'] == 'regex' else '精确匹配'}\n"
                text += f"响应类型：{keyword['response_type']}"
                
                await query.edit_message_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                
            elif action == "delete":
                # 处理删除关键词
                group_id = int(parts[2])
                keyword_id = parts[3]
                
                # 验证权限
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                # 删除关键词
                await self.db.remove_keyword(group_id, keyword_id)
                
                # 更新消息
                await self._handle_settings_section(
                    query,
                    context,
                    group_id,
                    "keywords"
                )
                
        except Exception as e:
            logger.error(f"处理关键词回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理关键词操作时出错")

    async def handle_keyword_response(
            self, 
            chat_id: int, 
            response: str, 
            context, 
            original_message: Optional[Message] = None
        ) -> Optional[Message]:
            """处理关键词响应，并可能进行自动删除
        
            :param chat_id: 聊天ID
            :param response: 响应内容
            :param context: 机器人上下文
            :param original_message: 原始消息
            :return: 发送的消息
            """
            sent_message = None
        
            if response.startswith('__media__'):
                # 处理媒体响应
                _, media_type, file_id = response.split('__')
            
                # 根据媒体类型发送消息
                media_methods = {
                    'photo': context.bot.send_photo,
                    'video': context.bot.send_video,
                    'document': context.bot.send_document
                }
            
                if media_type in media_methods:
                    sent_message = await media_methods[media_type](chat_id, file_id)
            else:
                # 处理文本响应
                sent_message = await context.bot.send_message(chat_id, response)
        
            # 如果成功发送消息，进行自动删除
            if sent_message:
                # 获取原始消息的元数据（如果有）
                metadata = get_message_metadata(original_message) if original_message else {}
            
                # 计算删除超时时间
                timeout = validate_delete_timeout(
                    message_type=metadata.get('type')
                )
            
                # 调度消息删除
                await self.message_deletion_manager.schedule_message_deletion(
                    sent_message, 
                    timeout
                )
        
            return sent_message

    async def _process_keyword_adding(self, update: Update, context, setting_state):
        """处理关键词添加流程的各个步骤"""
        try:
            step = setting_state['step']
            group_id = setting_state['group_id']
            match_type = setting_state['data'].get('match_type')
    
            if step == 1:
                # 获取关键词
                pattern = update.message.text
                max_length = 500  # 增加字符限制
            
                if len(pattern) > max_length:
                    await update.message.reply_text(f"❌ 关键词过长，请不要超过 {max_length} 个字符")
                    return
                
                # 如果是正则，验证正则表达式
                if match_type == 'regex':
                    if not validate_regex(pattern):
                        await update.message.reply_text("❌ 无效的正则表达式格式")
                        return
                    
                setting_state['data']['pattern'] = pattern
            
                # 选择响应类型
                keyboard = [
                    [
                        InlineKeyboardButton("文本", callback_data="keyword_response_text"),
                        InlineKeyboardButton("图片", callback_data="keyword_response_photo")
                    ],
                    [
                        InlineKeyboardButton("视频", callback_data="keyword_response_video"),
                        InlineKeyboardButton("文件", callback_data="keyword_response_document")
                    ],
                    [
                        InlineKeyboardButton("取消", callback_data=f"settings_keywords_{group_id}")
                    ]
                ]
            
                await update.message.reply_text(
                    "请选择关键词响应的类型：",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif step == 2:
                    # 获取关键词响应
                    response_type = setting_state['data'].get('response_type')
                
                    if response_type == 'text':
                        response = update.message.text
                        if len(response) > KEYWORD_SETTINGS['max_response_length']:
                            await update.message.reply_text(f"❌ 响应内容过长，请不要超过 {KEYWORD_SETTINGS['max_response_length']} 个字符")
                            return
                        file_id = response
                    elif response_type in ['photo', 'video', 'document']:
                        media_methods = {
                            'photo': lambda m: m.photo[-1].file_id if m.photo else None,
                            'video': lambda m: m.video.file_id if m.video else None,
                            'document': lambda m: m.document.file_id if m.document else None
                        }
                    
                        file_id = media_methods[response_type](update.message)
                    
                        if not file_id:
                            await update.message.reply_text(f"❌ 请发送一个{response_type}")
                            return
                    else:
                        await update.message.reply_text("❌ 未知的响应类型")
                        return
                
                    # 检查关键词数量是否超过限制
                    keywords = await self.db.get_keywords(group_id)
                    if len(keywords) >= KEYWORD_SETTINGS['max_keywords']:
                        await update.message.reply_text(f"❌ 关键词数量已达到上限 {KEYWORD_SETTINGS['max_keywords']} 个")
                        return
                
                    # 添加关键词
                    await self.db.add_keyword({
                        'group_id': group_id,
                        'pattern': setting_state['data']['pattern'],
                        'type': setting_state['data']['type'],
                        'response': file_id,
                        'response_type': response_type
                    })
                
                    await update.message.reply_text("✅ 关键词添加成功！")
                
                    # 清除设置状态
                    self.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
        
        except Exception as e:
            logger.error(f"处理关键词添加错误: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 添加关键词时出错")

    async def _process_broadcast_adding(self, update: Update, context, setting_state):
        """处理轮播消息添加流程"""
        try:
            step = setting_state['step']
            group_id = setting_state['group_id']
            content_type = setting_state['data'].get('content_type')
        
            if step == 1:
                # 获取消息内容
                if content_type == 'text':
                    content = update.message.text
                elif content_type == 'photo':
                    content = update.message.photo[-1].file_id if update.message.photo else None
                elif content_type == 'video':
                    content = update.message.video.file_id if update.message.video else None
                elif content_type == 'document':
                    content = update.message.document.file_id if update.message.document else None
                else:
                    await update.message.reply_text("❌ 不支持的消息类型")
                    return
                
                if not content:
                    await update.message.reply_text(f"❌ 请发送正确的{content_type}内容")
                    return
            
                setting_state['data']['content'] = content
            
                # 询问轮播时间设置
                await update.message.reply_text(
                    "请设置轮播时间：\n"
                    "格式：开始时间 结束时间 间隔(秒)\n"
                    "例如：2024-02-22 08:00 2024-03-22 20:00 3600\n"
                    "发送 /cancel 取消"
                )
                self.settings_manager.update_setting_state(
                    update.effective_user.id,
                    'broadcast',
                    {'content': content}
                )
            
            elif step == 2:
                # 处理时间设置
                try:
                    parts = update.message.text.split()
                    if len(parts) != 5:
                        raise ValueError("参数数量不正确")
                    
                    start_time = validate_time_format(f"{parts[0]} {parts[1]}")
                    end_time = validate_time_format(f"{parts[2]} {parts[3]}")
                    interval = validate_interval(parts[4])
                
                    if not all([start_time, end_time, interval]):
                        raise ValueError("时间格式无效")
                    
                    if start_time >= end_time:
                        raise ValueError("结束时间必须晚于开始时间")
                    
                    if interval < BROADCAST_SETTINGS['min_interval']:
                        raise ValueError(f"间隔时间不能小于{format_duration(BROADCAST_SETTINGS['min_interval'])}")

                    # 检查轮播消息数量限制
                    broadcasts = await self.db.db.broadcasts.count_documents({'group_id': group_id})
                    if broadcasts >= BROADCAST_SETTINGS['max_broadcasts']:
                        await update.message.reply_text(
                            f"❌ 轮播消息数量已达到上限 {BROADCAST_SETTINGS['max_broadcasts']} 条"
                        )
                        return
                    
                    # 添加轮播消息
                    await self.db.db.broadcasts.insert_one({
                        'group_id': group_id,
                        'content_type': content_type,
                        'content': setting_state['data']['content'],
                        'start_time': start_time,
                        'end_time': end_time,
                        'interval': interval
                    })
                    
                    await update.message.reply_text("✅ 轮播消息添加成功！")
                    
                    # 清除设置状态
                    self.settings_manager.clear_setting_state(update.effective_user.id, 'broadcast')
                    
                except ValueError as e:
                    await update.message.reply_text(f"❌ {str(e)}")
                    return
                    
        except Exception as e:
            logger.error(f"处理轮播消息添加错误: {e}")
            logger.error(traceback.format_exc())
            await update.message.reply_text("❌ 添加轮播消息时出错")

    async def _handle_keyword_response_type_callback(self, update: Update, context):
        """处理关键词响应类型的回调"""
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            response_type = data.split('_')[-1]  # 获取响应类型
            
            # 获取当前设置状态
            setting_state = self.settings_manager.get_setting_state(
                update.effective_user.id,
                'keyword'
            )
            
            if not setting_state:
                await query.edit_message_text("❌ 设置会话已过期，请重新开始")
                return
                
            # 更新设置状态
            setting_state['data']['response_type'] = response_type
            
            # 根据响应类型提示用户
            if response_type == 'text':
                prompt = "请发送关键词的文本回复内容："
            elif response_type == 'photo':
                prompt = "请发送关键词要回复的图片："
            elif response_type == 'video':
                prompt = "请发送关键词要回复的视频："
            elif response_type == 'document':
                prompt = "请发送关键词要回复的文件："
            else:
                await query.edit_message_text("❌ 不支持的响应类型")
                return
                
            await query.edit_message_text(
                f"{prompt}\n"
                "发送 /cancel 取消"
            )
            
            self.settings_manager.update_setting_state(
                update.effective_user.id,
                'keyword',
                {'response_type': response_type}
            )
            
        except Exception as e:
            logger.error(f"处理关键词响应类型回调错误: {e}")
            logger.error(traceback.format_exc())
            await query.edit_message_text("❌ 处理响应类型选择时出错")

            self.settings_manager.update_setting_state(
                update.effective_user.id,
                'broadcast',
                {'content_type': content_type}
            )
            
            if content_type == 'text':
                prompt = "请发送轮播消息的文本内容："
            elif content_type == 'photo':
                prompt = "请发送要轮播的图片："
            elif content_type == 'video':
                prompt = "请发送要轮播的视频："
            elif content_type == 'document':
                prompt = "请发送要轮播的文件："
            else:
                await query.edit_message_text("❌ 不支持的消息类型")
                return
            
            await query.edit_message_text(
                f"{prompt}\n"
                "发送 /cancel 取消"
            )

def async_main():
    """异步主入口点"""
    try:
        asyncio.run(TelegramBot.main(TelegramBot))
    except KeyboardInterrupt:
        logger.info("机器人被用户停止")
    except Exception as e:
        logger.error(f"机器人停止，错误原因: {e}")
        logger.error(traceback.format_exc())
        raise

if __name__ == '__main__':
    async_main()
