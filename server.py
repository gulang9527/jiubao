import os
import asyncio
import logging
from aiohttp import web
from bot import TelegramBot

# 配置日志
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# 创建机器人实例
bot = TelegramBot()

async def health_check(request):
    """简单的健康检查端点"""
    return web.Response(text="Bot is running")

async def start_bot():
    """初始化并启动机器人"""
    success = await bot.initialize()
    if success:
        logger.info("Bot initialized successfully")
        await bot.start()
        logger.info("Bot started successfully")
        return True
    else:
        logger.error("Failed to initialize bot")
        return False

async def stop_bot():
    """停止机器人"""
    if bot.running:
        await bot.stop()
        logger.info("Bot stopped")

async def start_server():
    """启动Web服务器"""
    # 从环境变量获取端口
    port = int(os.environ.get("PORT", 8080))
    
    # 创建Web应用
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    
    # 启动机器人
    bot_started = await start_bot()
    if not bot_started:
        logger.error("Could not start the bot. Exiting.")
        return
    
    # 配置关闭时的清理工作
    async def cleanup_background_tasks(app):
        await stop_bot()
    
    app.on_cleanup.append(cleanup_background_tasks)
    
    # 启动Web服务器
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    logger.info(f"Server started on port {port}")
    
    # 保持服务器运行
    while True:
        await asyncio.sleep(3600)  # 休眠一小时

if __name__ == "__main__":
    try:
        asyncio.run(start_server())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error in main process: {e}")
