import os
import logging
from typing import Dict, Any, List, Optional, Union
from telegram import Bot
from telegram.error import TelegramError
from db import Database
from datetime import datetime

class BroadcastManager:
    """广播管理类"""
    def __init__(self, db: Database, bot: Bot):
        self.db = db
        self.bot = bot
        self.max_retries = 3  # 消息发送最大重试次数
        self.retry_delay = 5  # 重试间隔（秒）

    async def send_message_to_group(
        self, 
        group_id: int, 
        message: Union[str, Dict[str, Any]]
    ) -> int:
        """
        向指定群组发送消息。

        Args:
            group_id: 群组ID
            message: 消息内容，可以是字符串或包含多媒体信息的字典

        Returns:
            发送成功的消息数量
        """
        try:
            # 检查群组是否存在
            group = await self.db.get_group(group_id)
            if not group:
                logging.warning(f"Group {group_id} not found.")
                return 0

            # 发送消息
            if isinstance(message, str):
                await self.bot.send_message(group_id, message)
            elif isinstance(message, dict):
                # 处理多媒体消息
                media_type = message.get("type")
                media_id = message.get("id")
                if media_type and media_id:
                    if media_type == "photo":
                        await self.bot.send_photo(group_id, media_id)
                    elif media_type == "video":
                        await self.bot.send_video(group_id, media_id)
                    elif media_type == "document":
                        await self.bot.send_document(group_id, media_id)
                    else:
                        logging.error(f"Unsupported media type: {media_type}")
                        return 0
                else:
                    logging.error("Invalid media format.")
                    return 0
            else:
                logging.error("Invalid message format.")
                return 0

            return 1

        except TelegramError as e:
            logging.error(f"Failed to send message to group {group_id}: {e}")
            return 0

    async def send_message_to_users(
        self, 
        user_ids: List[int], 
        message: Union[str, Dict[str, Any]]
    ) -> int:
        """
        向多个用户发送消息。

        Args:
            user_ids: 用户ID列表
            message: 消息内容，可以是字符串或包含多媒体信息的字典

        Returns:
            发送成功的消息数量
        """
        success_count = 0

        for user_id in user_ids:
            try:
                # 检查用户是否被封禁
                if await self.db.is_user_banned(user_id):
                    continue

                # 发送消息
                if isinstance(message, str):
                    await self.bot.send_message(user_id, message)
                elif isinstance(message, dict):
                    # 处理多媒体消息
                    media_type = message.get("type")
                    media_id = message.get("id")
                    if media_type and media_id:
                        if media_type == "photo":
                            await self.bot.send_photo(user_id, media_id)
                        elif media_type == "video":
                            await self.bot.send_video(user_id, media_id)
                        elif media_type == "document":
                            await self.bot.send_document(user_id, media_id)
                        else:
                            logging.error(f"Unsupported media type: {media_type}")
                            continue
                    else:
                        logging.error("Invalid media format.")
                        continue
                else:
                    logging.error("Invalid message format.")
                    continue

                success_count += 1

            except TelegramError as e:
                logging.error(f"Failed to send message to user {user_id}: {e}")
                continue

        return success_count

    async def broadcast_message(
        self, 
        message: Union[str, Dict[str, Any]], 
        group_ids: Optional[List[int]] = None, 
        user_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        向指定群组和用户广播消息。

        Args:
            message: 消息内容，可以是字符串或包含多媒体信息的字典
            group_ids: 目标群组ID列表（可选）
            user_ids: 目标用户ID列表（可选）

        Returns:
            包含广播结果的字典，格式为：
            {
                "total_sent": int,  # 总共发送的消息数量
                "success": int,     # 发送成功的消息数量
                "failed": int       # 发送失败的消息数量
            }
        """
        start_time = datetime.now()
        total_sent = 0
        success = 0
        failed = 0

        # 处理群组广播
        if group_ids:
            for group_id in group_ids:
                sent = await self.send_message_to_group(group_id, message)
                total_sent += 1
                success += sent
                failed += 1 if sent == 0 else 0

        # 处理用户广播
        if user_ids:
            sent = await self.send_message_to_users(user_ids, message)
            total_sent += len(user_ids)
            success += sent
            failed += len(user_ids) - sent

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return {
            "total_sent": total_sent,
            "success": success,
            "failed": failed,
            "duration": duration
        }

    async def send_broadcast_report(
        self, 
        chat_id: int, 
        result: Dict[str, Any]
    ) -> bool:
        """
        发送广播报告。

        Args:
            chat_id: 接收报告的聊天ID
            result: 广播结果字典

        Returns:
            是否成功发送报告
        """
        try:
            report = (
                f"广播报告\n"
                f"总发送量: {result['total_sent']}\n"
                f"成功: {result['success']}\n"
                f"失败: {result['failed']}\n"
                f"耗时: {result['duration']:.2f} 秒"
            )
            await self.bot.send_message(chat_id, report)
            return True
        except TelegramError as e:
            logging.error(f"Failed to send broadcast report: {e}")
            return False