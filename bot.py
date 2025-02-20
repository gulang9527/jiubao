import os
import signal
import asyncio
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, List, Dict, Any
import re
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
    format_duration
)

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

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
        return await self.db.keywords.find({
            'group_id': group_id
        }).to_list(None)

class BroadcastManager:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot
        self.running = False
        self.task = None
        
    async def start(self):
        """启动轮播任务"""
        if self.running:
            return
            
        self.running = True
        self.task = asyncio.create_task(self._broadcast_loop())
        logger.info("Broadcast manager started")
        
    async def stop(self):
        """停止轮播任务"""
        if not self.running:
            return
            
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("Broadcast manager stopped")
        
    async def _broadcast_loop(self):
        """轮播消息循环"""
        while self.running:
            try:
                # 获取所有活跃的轮播消息
                now = datetime.now()
                broadcasts = await self.db.broadcasts.find({
                    'start_time': {'$lte': now.isoformat()},
                    'end_time': {'$gt': now.isoformat()}
                }).to_list(None)
                
                for bc in broadcasts:
                    try:
                        # 检查是否需要发送
                        last_broadcast = bc.get('last_broadcast')
                        if last_broadcast:
                            last_time = datetime.fromisoformat(last_broadcast)
                            if (now - last_time).total_seconds() < bc['interval']:
                                continue
                        
                        # 发送消息
                        await self._send_broadcast(bc)
                        
                        # 更新发送时间
                        await self.db.broadcasts.update_one(
                            {'_id': bc['_id']},
                            {'$set': {'last_broadcast': now.isoformat()}}
                        )
                        
                    except Exception as e:
                        logger.error(f"Error sending broadcast {bc['_id']}: {e}")
                
                # 清理过期的轮播消息
                await self.db.broadcasts.delete_many({
                    'end_time': {'$lte': now.isoformat()}
                })
                
            except Exception as e:
                logger.error(f"Error in broadcast loop: {e}")
                
            await asyncio.sleep(60)  # 每分钟检查一次
            
    async def _send_broadcast(self, broadcast):
        """发送轮播消息"""
        try:
            content_type = broadcast['content_type']
            content = broadcast['content']
            group_id = broadcast['group_id']
            
            if content_type == 'text':
                await self.bot.send_message(group_id, content)
            elif content_type == 'photo':
                await self.bot.send_photo(group_id, content)
            elif content_type == 'video':
                await self.bot.send_video(group_id, content)
            elif content_type == 'document':
                await self.bot.send_document(group_id, content)
            else:
                logger.error(f"Unknown content type: {content_type}")
                
        except Exception as e:
            logger.error(f"Error sending broadcast: {e}")

class StatsManager:
    def __init__(self, db):
        self.db = db
        
    async def get_daily_stats(self, group_id: int, page: int = 1) -> tuple:
        """获取日排行统计"""
        settings = await self.db.get_group_settings(group_id)
        limit = settings.get('daily_rank_size', 15)
        skip = (page - 1) * limit
        
        pipeline = [
            {
                '$match': {
                    'group_id': group_id,
                    'date': datetime.now().strftime('%Y-%m-%d')
                }
            },
            {
                '$group': {
                    '_id': '$user_id',
                    'total_messages': {'$sum': 1},
                    'total_size': {'$sum': '$size'}
                }
            },
            {'$sort': {'total_size': -1}},
            {'$skip': skip},
            {'$limit': limit}
        ]
        
        results = await self.db.message_stats.aggregate(pipeline).to_list(None)
        
        # 获取总页数
        total_users = len(await self.db.message_stats.distinct('user_id', {
            'group_id': group_id,
            'date': datetime.now().strftime('%Y-%m-%d')
        }))
        total_pages = (total_users + limit - 1) // limit
        
        return results, total_pages

    async def get_monthly_stats(self, group_id: int, page: int = 1) -> tuple:
        """获取月排行统计"""
        settings = await self.db.get_group_settings(group_id)
        limit = settings.get('monthly_rank_size', 15)
        skip = (page - 1) * limit
        
        # 获取30天前的日期
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        pipeline = [
            {
                '$match': {
                    'group_id': group_id,
                    'date': {'$gte': thirty_days_ago}
                }
            },
            {
                '$group': {
                    '_id': '$user_id',
                    'total_messages': {'$sum': 1},
                    'total_size': {'$sum': '$size'}
                }
            },
            {'$sort': {'total_size': -1}},
            {'$skip': skip},
            {'$limit': limit}
        ]
        
        results = await self.db.message_stats.aggregate(pipeline).to_list(None)
        
        # 获取总页数
        total_users = len(await self.db.message_stats.distinct('user_id', {
            'group_id': group_id,
            'date': {'$gte': thirty_days_ago}
        }))
        total_pages = (total_users + limit - 1) // limit
        
        return results, total_pages

    async def add_message_stat(self, group_id: int, user_id: int, message: Message):
        """添加消息统计"""
        settings = await self.db.get_group_settings(group_id)
        min_bytes = settings.get('min_bytes', 0)
        count_media = settings.get('count_media', False)
        
        # 计算消息大小
        if message.text:
            size = len(message.text.encode('utf-8'))
        elif count_media:
            if message.photo:
                size = message.photo[-1].file_size
            elif message.video:
                size = message.video.file_size
            elif message.document:
                size = message.document.file_size
            else:
                return  # 不统计其他类型的消息
        else:
            return  # 不统计多媒体消息
            
        # 检查最低字节数
        if size < min_bytes:
            return
            
        # 添加统计
        await self.db.add_message_stat({
            'group_id': group_id,
            'user_id': user_id,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'size': size,
            'created_at': datetime.now().isoformat()
        })

class TelegramBot:
    def __init__(self):
        self.db = Database()
        self.application = None
        self.web_runner = None
        self.cleanup_task = None
        self.shutdown_event = asyncio.Event()
        self.running = False
        
        # 初始化管理器
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
        for admin_id in [358987879, 502226686, 883253093]:
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
        
    def _add_handlers(self):
        """添加命令处理器"""
        # 消息处理器
        self.application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message)
        )
        
        # 基本命令
        self.application.add_handler(
            CommandHandler("settings", self._handle_settings)
        )
        
        # 统计命令
        self.application.add_handler(
            CommandHandler(["tongji", "tongji30"], self._handle_rank_command)
        )
        
        # 回调查询
        self.application.add_handler(
            CallbackQueryHandler(self._handle_settings_callback, pattern="^settings_")
        )
        self.application.add_handler(
            CallbackQueryHandler(self._handle_keyword_callback, pattern="^keyword_")
        )
        self.application.add_handler(
            CallbackQueryHandler(self._handle_broadcast_callback, pattern="^broadcast_")
        )
        self.application.add_handler(
            CallbackQueryHandler(self._handle_rank_callback, pattern="^rank_")
        )
        
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
        
        # 关闭轮播管理器
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
            port = int(os.getenv("PORT", "8080"))
            
            # 创建站点并启动
            site = web.TCPSite(self.web_runner, host='0.0.0.0', port=port)
            await site.start()
            
            logger.info(f"Web server started successfully on port {port}")
        except Exception as e:
            logger.error(f"Failed to start web server: {e}")
            raise

    async def _handle_message(self, update: Update, context):
        """处理消息"""
        if not update.effective_chat or not update.effective_user or not update.message:
            return
            
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        try:
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
                        if response.startswith('__media__'):
                            # 处理媒体响应
                            _, media_type, file_id = response.split('__')
                            if media_type == 'photo':
                                await context.bot.send_photo(chat_id, file_id)
                            elif media_type == 'video':
                                await context.bot.send_video(chat_id, file_id)
                            elif media_type == 'document':
                                await context.bot.send_document(chat_id, file_id)
                        else:
                            # 处理文本响应
                            await update.message.reply_text(response)
            
            # 处理消息统计
            if await self.has_permission(chat_id, GroupPermission.STATS):
                await self.stats_manager.add_message_stat(chat_id, user_id, update.message)
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            
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
            settings = await self.settings_manager.get_settings(group_id)
            min_bytes = settings.get('min_bytes', 0)
            
            for i, stat in enumerate(stats, start=(page-1)*15+1):
                try:
                    user = await context.bot.get_chat_member(group_id, stat['_id'])
                    name = user.user.full_name
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
            
            await update.message.reply_text(text)
            
        except Exception as e:
            logger.error(f"Error handling rank command: {e}")
            await update.message.reply_text("❌ 获取排行榜时出错")

    async def _handle_settings_callback(self, update: Update, context):
        """处理设置回调"""
        query = update.callback_query
        await query.answer()
        
        try:
            data = query.data
            parts = data.split('_')
            action = parts[1]
            
            if action == "select":
                # 处理群组选择
                group_id = int(parts[2])
                if not await self.db.can_manage_group(update.effective_user.id, group_id):
                    await query.edit_message_text("❌ 无权限管理此群组")
                    return
                    
                # 显示设置菜单
                await self._show_settings_menu(query, context, group_id)
                
            elif action in ["keywords", "broadcast", "stats"]:
                # 处理具体设置项
                group_id = int(parts[2])
                await self._handle_settings_section(query, context, group_id, action)
                
        except Exception as e:
            logger.error(f"Error handling settings callback: {e}")
            await query.edit_message_text("❌ 处理设置回调时出错")
            
    async def _show_settings_menu(self, query, context, group_id: int):
        """显示设置菜单"""
        try:
            keyboard = [
                [
                    InlineKeyboardButton(
                        "关键词管理",
                        callback_data=f"settings_keywords_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "轮播设置",
                        callback_data=f"settings_broadcast_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "统计设置",
                        callback_data=f"settings_stats_{group_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        "返回群组选择",
                        callback_data="settings_groups"
                    )
                ]
            ]
            
            try:
                group_info = await context.bot.get_chat(group_id)
                group_name = group_info.title or f"群组 {group_id}"
            except Exception:
                group_name = f"群组 {group_id}"
            
            await query.edit_message_text(
                f"⚙️ {group_name} 的设置\n"
                "请选择要修改的设置项：",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Error showing settings menu: {e}")
            await query.edit_message_text("❌ 显示设置菜单时出错")

    async def cleanup_old_stats(self):
        """定期清理旧统计数据"""
        try:
            while not self.shutdown_event.is_set():
                try:
                    await self.db.cleanup_old_stats(days=30)
                    logger.info("Cleaned up old stats")
                    await asyncio.sleep(86400)  # 每天清理一次
                except Exception as e:
                    logger.error(f"Error cleaning up old stats: {e}")
                    await asyncio.sleep(3600)  # 出错后等待1小时再试
        except asyncio.CancelledError:
            logger.info("Cleanup task cancelled")
            
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

async def handle_signals(bot):
    """处理系统信号"""
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(
                sig,
                lambda: asyncio.create_task(bot.stop())
            )
        logger.info("Signal handlers set up")
    except NotImplementedError:
        # Windows 不支持 add_signal_handler
        logger.warning("Signal handlers not supported on this platform")

async def main():
    """主函数"""
    bot = None
    try:
        # 创建机器人实例
        bot = TelegramBot()
               
        # 初始化
        await bot.initialize()
        
        # 设置信号处理
        await handle_signals(bot)
        
        # 启动机器人
        await bot.start()
        
    except Exception as e:
        logger.error(f"Bot startup failed: {e}")
        if bot:
            await bot.stop()
        raise
    finally:
        if bot and bot.running:
            await bot.shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot stopped due to error: {e}")
        raise
