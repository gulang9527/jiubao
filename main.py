import os
import asyncio
import logging
from dotenv import load_dotenv
from bot import TelegramBot

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def main():
    """主函数"""
    # 检查必要的环境变量
    if not os.getenv('TELEGRAM_TOKEN'):
        logger.error("Missing TELEGRAM_TOKEN environment variable")
        return
        
    if not os.getenv('MONGODB_URI'):
        logger.error("Missing MONGODB_URI environment variable")
        return

    # 创建并启动机器人
    bot = TelegramBot()
    try:
        await bot.initialize()
        await bot.start()
    except Exception as e:
        logger.error(f"Error running bot: {e}")
    finally:
        await bot.shutdown()

if __name__ == '__main__':
    asyncio.run(main())