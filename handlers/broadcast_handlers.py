"""
轮播消息处理函数，处理轮播消息相关操作
"""
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

from utils.decorators import handle_callback_errors
from utils.message_utils import get_media_type, get_file_id
from utils.time_utils import validate_time_format, format_datetime, format_duration
from db.models import GroupPermission

logger = logging.getLogger(__name__)

#######################################
# 回调处理函数
#######################################

@handle_callback_errors
async def handle_broadcast_form_callback(update: Update, context: CallbackContext, data: str):
    """
    处理轮播消息表单回调
    
    参数:
        update: 更新对象
        context: 上下文对象
        data: 回调数据
    """
    query = update.callback_query
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 立即应答回调查询
    await query.answer()
    
    # 解析回调数据
    parts = data.split('_')
    logger.info(f"处理轮播消息表单回调: {parts}")

    if len(parts) < 2:
        logger.error(f"轮播消息回调数据格式错误: {data}")
        await query.edit_message_text("❌ 无效的操作")
        return

    # 根据前缀判断
    prefix = parts[0]
    if prefix != "bcform":
        logger.error(f"非轮播消息回调数据: {data}")
        await query.edit_message_text("❌ 无效的操作")
        return

    # 首先直接检查常见的简单操作
    if len(parts) == 2 and parts[1] in ["submit", "cancel", "preview"]:
        action = parts[1]
        logger.info(f"检测到简单操作: {action}")
    # 特殊处理select_group的情况
    elif len(parts) >= 4 and parts[1] == "select" and parts[2] == "group":
        action = "select_group"
        group_id = int(parts[3])
        logger.info(f"检测到选择群组操作，群组ID: {group_id}")
    # 处理两部分的回调数据
    elif len(parts) == 2:
        action = parts[1]
        logger.info(f"检测到基本操作: {action}")
    elif len(parts) >= 3:
        # 首先检查接收类型操作，避免被后续逻辑覆盖
        if parts[1] in ["content", "media", "buttons", "interval", "time"] and parts[2] == "received":
            action = f"{parts[1]}_received"
            logger.info(f"检测到接收操作: {action}")
        elif parts[1] == "end" and parts[2] == "time" and parts[3] == "received":
            action = "end_time_received"
            logger.info(f"检测到接收操作: end_time_received")
        # 然后检查添加操作
        elif parts[1] == "add" and parts[2] in ["text", "media", "button", "content"]:
            action = f"add_{parts[2]}"
            logger.info(f"检测到添加操作: {action}")
        # 再检查设置操作
        elif parts[1] == "set" and parts[2] in ["schedule", "repeat", "start", "end"]:
            if parts[2] == "start" and len(parts) > 3 and parts[3] == "time":
                action = "set_start_time"
            elif parts[2] == "end" and len(parts) > 3 and parts[3] == "time":
                action = "set_end_time"
            else:
                action = f"set_{parts[2]}"
            logger.info(f"检测到设置操作: {action}")
        # 最后，如果以上条件都不满足，才考虑使用默认的 parts[2]
        else:
            action = parts[2]
    else:
        logger.error(f"轮播消息回调数据格式错误: {data}")
        await query.edit_message_text("❌ 无效的操作")
        return
        
    logger.info(f"轮播消息表单操作: {action}")
    
    form_data = context.user_data.get('broadcast_form', {})
    logger.info(f"当前轮播消息表单数据: {form_data}")
    
    # 处理不同的表单操作
    logger.info(f"开始处理操作: {action}")
    
    if action == "cancel":
        logger.info("执行取消操作")
        # 取消操作
        if 'broadcast_form' in context.user_data:
            del context.user_data['broadcast_form']
        if 'waiting_for' in context.user_data:
            del context.user_data['waiting_for']
        await query.edit_message_text("✅ 已取消轮播消息添加")
        
    elif action == "select_group":
        logger.info(f"执行选择群组操作，群组ID: {group_id}")
        # 选择群组
        # 启动添加流程
        await start_broadcast_form(update, context, group_id)
        
    elif action == "add_content":
        logger.info("执行添加内容操作")
        # 显示内容添加选项
        await show_broadcast_content_options(update, context)
        
    elif action == "add_text":
        logger.info("执行添加文本操作")
        # 添加文本内容
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        await query.edit_message_text(
            "请发送轮播消息的文本内容:\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'broadcast_text'
        
    elif action == "add_media":
        logger.info("执行添加媒体操作")
        # 添加媒体内容
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        await query.edit_message_text(
            "请发送要添加的媒体:\n"
            "• 图片\n"
            "• 视频\n"
            "• 文件\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'broadcast_media'
        
    elif action == "add_button":
        logger.info("执行添加按钮操作")
        # 添加按钮
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        await query.edit_message_text(
            "请发送按钮信息，每行一个按钮，格式灵活:\n\n"
            "文字 网址\n"
            "文字-网址\n"
            "文字,网址\n"
            "文字|网址\n\n"
            "例如:\n"
            "访问官网 https://example.com\n"
            "联系我们 https://t.me/username\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'broadcast_buttons'
        
    elif action == "set_schedule":
        logger.info("执行设置计划操作")
        # 设置轮播计划
        await show_schedule_options(update, context)
        
    elif action == "set_repeat":
        logger.info("执行设置重复操作")
        # 设置重复选项
        if len(parts) >= 4:
            repeat_type = parts[3]
            logger.info(f"设置重复类型: {repeat_type}")
            form_data['repeat_type'] = repeat_type
            context.user_data['broadcast_form'] = form_data
            
            # 根据不同的重复类型设置默认间隔
            if repeat_type == 'once':
                form_data['repeat_interval'] = 0
            elif repeat_type == 'hourly':
                form_data['repeat_interval'] = 60  # 默认间隔60分钟
            elif repeat_type == 'daily':
                form_data['repeat_interval'] = 24  # 默认间隔24小时
            elif repeat_type == 'custom':
                # 提示用户设置自定义间隔
                keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
                await query.edit_message_text(
                    "请设置自定义重复间隔（分钟）:\n"
                    "例如: 30（表示每30分钟发送一次）\n\n"
                    "发送完后请点击下方出现的「继续」按钮",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                context.user_data['waiting_for'] = 'broadcast_interval'
                logger.info("等待用户输入自定义重复间隔")
                return
                
            # 显示发送时间选项
            await show_start_time_options(update, context)
        else:
            logger.warning("无效的重复类型设置")
            await query.edit_message_text("❌ 无效的重复类型")

        context.user_data['waiting_for'] = 'broadcast_start_time'

                
    elif action == "set_start_time":
        logger.info("执行设置开始时间操作")
        # 设置开始时间
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        await query.edit_message_text(
            "请设置轮播消息的首次发送时间:\n\n"
            "支持多种格式:\n"
            "• YYYY-MM-DD HH:MM:SS (例如: 2023-12-31 12:30:00)\n"
            "• YYYY/MM/DD HH:MM (例如: 2023/12/31 12:30)\n"
            "• MM-DD HH:MM (例如: 12-31 12:30, 使用当前年份)\n"
            "• HH:MM (例如: 12:30, 使用当天)\n"
            "• +分钟 (例如: +30, 表示30分钟后)\n"
            "• now 或 立即 (表示立即开始)\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    elif action in ["content_received", "media_received", "buttons_received", "time_received", "end_time_received"]:
        logger.info(f"执行数据接收操作: {action}")
        # 已收到各类数据，显示表单选项
        await show_broadcast_options(update, context)

    elif action == "interval_received":
        logger.info("执行接收间隔操作，显示开始时间选项")
        # 显示开始时间选项
        await show_start_time_options(update, context)
        
    elif action == "preview":
        logger.info("执行预览操作")
        # 预览轮播消息
        await preview_broadcast_content(update, context)
        
    elif action == "submit":
        logger.info("执行提交操作")
        # 提交轮播消息
        await submit_broadcast_form(update, context)
        
    elif action == "set_end_time":
        logger.info("执行设置结束时间操作")
        # 设置结束时间
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        await query.edit_message_text(
            "请设置轮播消息的结束时间:\n\n"
            "支持多种格式:\n"
            "• YYYY-MM-DD HH:MM:SS (例如: 2023-12-31 12:30:00)\n"
            "• YYYY/MM/DD HH:MM (例如: 2023/12/31 12:30)\n"
            "• MM-DD HH:MM (例如: 12-31 12:30, 使用当前年份)\n"
            "• HH:MM (例如: 12:30, 使用当天)\n"
            "• +天数 (例如: +30, 表示30天后)\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'broadcast_end_time'
    
    else:
        logger.warning(f"未知的轮播消息表单操作: {action}")
        await query.edit_message_text("❌ 未知操作")
        
async def submit_broadcast_form(update: Update, context: CallbackContext):
    """
    提交轮播消息表单
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    logger.info("提交轮播消息表单")
    form_data = context.user_data.get('broadcast_form', {})
    logger.info(f"提交的表单数据: {form_data}")
    
    # 验证必要字段
    has_content = bool(form_data.get('text') or form_data.get('media') or form_data.get('buttons'))
    if not has_content:
        await update.callback_query.answer("❌ 请至少添加一项内容")
        await show_broadcast_options(update, context)
        return
    
    # 验证计划设置
    if not form_data.get('start_time'):
        await update.callback_query.answer("❌ 请设置发送计划")
        await show_broadcast_options(update, context)
        return
    
    # 构建轮播消息数据
    broadcast_data = {
        'group_id': form_data['group_id'],
        'text': form_data.get('text', ''),
        'media': form_data.get('media'),
        'buttons': form_data.get('buttons', []),
        'repeat_type': form_data.get('repeat_type', 'once'),
        'repeat_interval': form_data.get('repeat_interval', 0)
    }
    
    # 处理开始时间
    start_time_str = form_data.get('start_time')
    if start_time_str and start_time_str.lower() != 'now':
        try:
            # 验证时间格式
            start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
            broadcast_data['start_time'] = start_time
        except ValueError:
            await update.callback_query.answer("❌ 时间格式不正确")
            await show_broadcast_options(update, context)
            return
    else:
        # 立即开始
        broadcast_data['start_time'] = datetime.now()

    # 处理结束时间
    if form_data.get('repeat_type') == 'once':
        # 单次发送时，结束时间与开始时间相同
        broadcast_data['end_time'] = broadcast_data['start_time']
    else:
        # 重复发送时，使用设置的结束时间或者默认30天
        if form_data.get('end_time'):
            try:
                end_time = datetime.strptime(form_data.get('end_time'), '%Y-%m-%d %H:%M:%S')
                broadcast_data['end_time'] = end_time
            except ValueError:
                # 如果结束时间格式错误，使用默认的30天
                end_time = broadcast_data['start_time'] + timedelta(days=30)
                broadcast_data['end_time'] = end_time
                logger.info(f"结束时间格式错误，设置默认结束时间: {end_time}")
        else:
            # 如果未设置结束时间，使用默认的30天
            end_time = broadcast_data['start_time'] + timedelta(days=30)
            broadcast_data['end_time'] = end_time
            logger.info(f"设置默认结束时间: {end_time}")
    
    # 添加轮播消息
    bot_instance = context.application.bot_data.get('bot_instance')
    try:
        await bot_instance.db.add_broadcast(broadcast_data)
        
        # 清理表单数据
        if 'broadcast_form' in context.user_data:
            del context.user_data['broadcast_form']
        if 'waiting_for' in context.user_data:
            del context.user_data['waiting_for']
        
        # 确定重复类型文本
        repeat_text = "单次发送"
        if broadcast_data['repeat_type'] == 'hourly':
            repeat_text = "每小时发送"
        elif broadcast_data['repeat_type'] == 'daily':
            repeat_text = "每天发送"
        elif broadcast_data['repeat_type'] == 'custom':
            repeat_text = f"每 {broadcast_data['repeat_interval']} 分钟发送"
        
        # 显示成功消息
        await update.callback_query.edit_message_text(
            "✅ 轮播消息添加成功！\n\n"
            f"重复类型: {repeat_text}\n"
            f"开始时间: {format_datetime(broadcast_data['start_time'])}\n"
            f"结束时间: {format_datetime(broadcast_data['end_time'])}"
        )
    except Exception as e:
        logger.error(f"添加轮播消息错误: {e}")
        await update.callback_query.answer("❌ 添加轮播消息失败")
        await update.callback_query.edit_message_text(
            f"❌ 添加轮播消息失败: {str(e)}\n\n"
            "请重试或联系管理员"
        )

#######################################
# 表单输入处理
#######################################

async def handle_broadcast_form_input(update: Update, context: CallbackContext, input_type: str) -> bool:
    """
    处理轮播消息表单输入
    
    参数:
        update: 更新对象
        context: 上下文对象
        input_type: 输入类型
        
    返回:
        是否处理了输入
    """
    message = update.effective_message
    form_data = context.user_data.get('broadcast_form', {})
    user_id = update.effective_user.id
    
    if not form_data:
        logger.warning(f"用户 {user_id} 处于轮播输入模式但无表单数据")
        await message.reply_text("❌ 轮播表单数据丢失，请重新开始")
        context.user_data.pop('waiting_for', None)
        return True
        
    # 根据输入类型处理
    if input_type == 'broadcast_text':
        # 接收轮播消息文本
        text = message.text
        if not text or len(text) > 1000:
            await message.reply_text("❌ 文本长度必须在1-1000字符之间")
            return True
            
        # 存储文本
        form_data['text'] = text
        context.user_data['broadcast_form'] = form_data
        context.user_data.pop('waiting_for', None)
        
        # 提供继续按钮
        keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_content_received")]]
        await message.reply_text(
            f"✅ 已设置轮播文本\n\n点击「继续」进行下一步",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    elif input_type == 'broadcast_media':
        # 接收轮播媒体
        media_type = get_media_type(message)
        if not media_type:
            await message.reply_text("❌ 请发送图片、视频或文件")
            return True
            
        # 存储媒体信息
        file_id = get_file_id(message)
        if not file_id:
            await message.reply_text("❌ 无法获取媒体文件ID")
            return True
            
        form_data['media'] = {'type': media_type, 'file_id': file_id}
        context.user_data['broadcast_form'] = form_data
        context.user_data.pop('waiting_for', None)
        
        # 提供继续按钮
        keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_media_received")]]
        await message.reply_text(
            f"✅ 已设置{media_type}媒体\n\n点击「继续」进行下一步",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    elif input_type == 'broadcast_buttons':
        # 接收按钮配置
        lines = message.text.strip().split('\n')
        buttons = []
        error_lines = []
    
        for i, line in enumerate(lines, 1):
            if not line.strip():
                continue
        
            # 尝试多种分隔符
            button_found = False
            for separator in ['|', ' ', '-', ',']:
                if separator in line:
                    parts = line.split(separator, 1)  # 只分割一次，以防URL中包含分隔符
                    text, url = parts[0].strip(), parts[1].strip()
                
                    # 检查URL格式
                    if text and url and (url.startswith(('http://', 'https://', 't.me/'))):
                        buttons.append({'text': text, 'url': url})
                        button_found = True
                        break
        
            if not button_found:
                error_lines.append(i)
    
        if error_lines:
            await message.reply_text(
                f"❌ 第 {', '.join(map(str, error_lines))} 行格式不正确\n"
                "请使用以下格式之一，每行一个按钮:\n"
                "• 按钮文字|网址\n"
                "• 按钮文字 网址\n"
                "• 按钮文字-网址\n"
                "• 按钮文字,网址\n"
                "例如: 访问官网 https://example.com"
            )
            return True
        
        if not buttons:
            await message.reply_text("❌ 未能解析任何有效按钮")
            return True
        
        if len(buttons) > 10:
            await message.reply_text("❌ 按钮数量不能超过10个")
            return True
        
        # 存储按钮配置
        form_data['buttons'] = buttons
        context.user_data['broadcast_form'] = form_data
        context.user_data.pop('waiting_for', None)
    
        # 提供继续按钮
        keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_buttons_received")]]
        await message.reply_text(
            f"✅ 已设置 {len(buttons)} 个按钮\n\n点击「继续」进行下一步",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    elif input_type == 'broadcast_interval':
        # 接收自定义重复间隔
        try:
            interval = int(message.text)
            if interval < 5 or interval > 10080:  # 5分钟到1周(10080分钟)
                await message.reply_text("❌ 重复间隔必须在5-10080分钟之间")
                return True
                
            # 存储自定义间隔
            form_data['repeat_interval'] = interval
            context.user_data['broadcast_form'] = form_data
            context.user_data.pop('waiting_for', None)
            
            # 显示开始时间选项
            keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_interval_received")]]
            await message.reply_text(
                f"✅ 已设置重复间隔: {interval} 分钟\n\n点击「继续」进行下一步",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return True
            
        except ValueError:
            await message.reply_text("❌ 请输入一个有效的数字")
            return True
            
    elif input_type == 'broadcast_start_time':
        # 接收开始时间
        start_time_str = message.text.strip()
        now = datetime.now()
    
        # 处理现在开始的情况
        if start_time_str.lower() in ['now', '立即', '现在']:
            # 设置为当前时间
            start_time = now
            form_data['start_time'] = start_time.strftime('%Y-%m-%d %H:%M:%S')
            context.user_data['broadcast_form'] = form_data
            context.user_data.pop('waiting_for', None)
        
            # 提供继续按钮
            keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_time_received")]]
            await message.reply_text(
                f"✅ 已设置开始时间: 立即开始\n\n点击「继续」进行下一步",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return True
    
        # 处理相对时间（+分钟）
        if start_time_str.startswith('+'):
            try:
                minutes = int(start_time_str[1:])
                if minutes <= 0:
                    await message.reply_text("❌ 分钟数必须大于0")
                    return True
            
                start_time = now + timedelta(minutes=minutes)
                form_data['start_time'] = start_time.strftime('%Y-%m-%d %H:%M:%S')
                context.user_data['broadcast_form'] = form_data
                context.user_data.pop('waiting_for', None)
            
                keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_time_received")]]
                await message.reply_text(
                    f"✅ 已设置开始时间: {format_datetime(start_time)}（{minutes}分钟后）\n\n点击「继续」进行下一步",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return True
            except ValueError:
                await message.reply_text("❌ +后面必须是有效的分钟数")
                return True
    
        # 尝试多种时间格式
        try:
            # 尝试完整格式 YYYY-MM-DD HH:MM:SS
            if re.match(r'^\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}$', start_time_str):
                start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
            # 尝试 YYYY-MM-DD HH:MM 格式
            elif re.match(r'^\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}$', start_time_str):
                start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M')
            # 尝试 YYYY/MM/DD HH:MM 格式
            elif re.match(r'^\d{4}/\d{1,2}/\d{1,2} \d{1,2}:\d{1,2}$', start_time_str):
                start_time = datetime.strptime(start_time_str, '%Y/%m/%d %H:%M')
            # 尝试 MM-DD HH:MM 格式（使用当前年份）
            elif re.match(r'^\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}$', start_time_str):
                start_time = datetime.strptime(f"{now.year}-{start_time_str}", '%Y-%m-%d %H:%M')
            # 尝试 HH:MM 格式（使用当天）
            elif re.match(r'^\d{1,2}:\d{1,2}$', start_time_str):
                hour, minute = map(int, start_time_str.split(':'))
                start_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                # 如果时间已过，则设为明天
                if start_time <= now:
                    start_time = start_time + timedelta(days=1)
            else:
                await message.reply_text(
                    "❌ 时间格式不正确，请使用以下格式之一:\n"
                    "• YYYY-MM-DD HH:MM:SS (例如: 2023-12-31 12:30:00)\n"
                    "• YYYY-MM-DD HH:MM (例如: 2023-12-31 12:30)\n"
                    "• YYYY/MM/DD HH:MM (例如: 2023/12/31 12:30)\n"
                    "• MM-DD HH:MM (例如: 12-31 12:30, 使用当前年份)\n"
                    "• HH:MM (例如: 12:30, 使用当天或明天)\n"
                    "• +分钟 (例如: +30, 表示30分钟后)\n"
                    "• now 或 立即 (表示立即开始)"
                )
                return True
        
            # 检查是否是未来时间
            if start_time <= now and not (re.match(r'^\d{1,2}:\d{1,2}$', start_time_str)):
                await message.reply_text("❌ 开始时间必须是未来时间")
                return True
        
            # 存储开始时间
            form_data['start_time'] = start_time.strftime('%Y-%m-%d %H:%M:%S')
            context.user_data['broadcast_form'] = form_data
            context.user_data.pop('waiting_for', None)
        
            # 提供继续按钮
            keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_time_received")]]
            await message.reply_text(
                f"✅ 已设置开始时间: {format_datetime(start_time)}\n\n点击「继续」进行下一步",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return True
    
        except ValueError as e:
            logger.error(f"时间解析错误: {str(e)}")
            await message.reply_text("❌ 无法解析时间，请检查格式")
            return True

    elif input_type == 'broadcast_end_time':
        # 接收结束时间
        end_time_str = message.text.strip()
        now = datetime.now()
        start_time_str = form_data.get('start_time')
        
        # 如果start_time存在且不是'now'，则解析开始时间
        if start_time_str and start_time_str.lower() != 'now':
            try:
                start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                start_time = datetime.now()
        else:
            start_time = datetime.now()
    
        # 处理相对时间（+天数）
        if end_time_str.startswith('+'):
            try:
                days = int(end_time_str[1:])
                if days <= 0:
                    await message.reply_text("❌ 天数必须大于0")
                    return True
                
                end_time = start_time + timedelta(days=days)
                form_data['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
                context.user_data['broadcast_form'] = form_data
                context.user_data.pop('waiting_for', None)
                
                keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_end_time_received")]]
                await message.reply_text(
                    f"✅ 已设置结束时间: {format_datetime(end_time)}（开始后{days}天）\n\n点击「继续」进行下一步",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return True
            except ValueError:
                await message.reply_text("❌ +后面必须是有效的天数")
                return True
    
        # 尝试多种时间格式
        try:
            # 尝试完整格式 YYYY-MM-DD HH:MM:SS
            if re.match(r'^\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}$', end_time_str):
                end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
            # 尝试 YYYY-MM-DD HH:MM 格式
            elif re.match(r'^\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}$', end_time_str):
                end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M')
            # 尝试 YYYY/MM/DD HH:MM 格式
            elif re.match(r'^\d{4}/\d{1,2}/\d{1,2} \d{1,2}:\d{1,2}$', end_time_str):
                end_time = datetime.strptime(end_time_str, '%Y/%m/%d %H:%M')
            # 尝试 MM-DD HH:MM 格式（使用当前年份）
            elif re.match(r'^\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}$', end_time_str):
                end_time = datetime.strptime(f"{now.year}-{end_time_str}", '%Y-%m-%d %H:%M')
            # 尝试 HH:MM 格式（使用当天）
            elif re.match(r'^\d{1,2}:\d{1,2}$', end_time_str):
                hour, minute = map(int, end_time_str.split(':'))
                end_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                # 如果时间已过，则设为明天
                if end_time <= now:
                    end_time = end_time + timedelta(days=1)
            else:
                await message.reply_text(
                    "❌ 时间格式不正确，请使用以下格式之一:\n"
                    "• YYYY-MM-DD HH:MM:SS (例如: 2023-12-31 12:30:00)\n"
                    "• YYYY-MM-DD HH:MM (例如: 2023-12-31 12:30)\n"
                    "• YYYY/MM/DD HH:MM (例如: 2023/12/31 12:30)\n"
                    "• MM-DD HH:MM (例如: 12-31 12:30, 使用当前年份)\n"
                    "• HH:MM (例如: 12:30, 使用当天或明天)\n"
                    "• +天数 (例如: +30, 表示30天后)"
                )
                return True
            
            # 检查是否晚于开始时间
            if end_time <= start_time:
                await message.reply_text("❌ 结束时间必须晚于开始时间")
                return True
            
            # 存储结束时间
            form_data['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
            context.user_data['broadcast_form'] = form_data
            context.user_data.pop('waiting_for', None)
            
            # 提供继续按钮
            keyboard = [[InlineKeyboardButton("继续", callback_data="bcform_end_time_received")]]
            await message.reply_text(
                f"✅ 已设置结束时间: {format_datetime(end_time)}\n\n点击「继续」进行下一步",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return True
        
        except ValueError as e:
            logger.error(f"时间解析错误: {str(e)}")
            await message.reply_text("❌ 无法解析时间，请检查格式")
            return True

#######################################
# 表单功能函数
#######################################

async def start_broadcast_form(update: Update, context: CallbackContext, group_id: int):
    """
    启动轮播消息表单流程
    
    参数:
        update: 更新对象
        context: 上下文对象
        group_id: 群组ID
    """
    try:
        logger.info(f"启动轮播消息表单流程，群组ID: {group_id}")
        # 获取bot实例
        bot_instance = context.application.bot_data.get('bot_instance')
        if not bot_instance:
            logger.error("获取bot实例失败")
            if update.callback_query:
                await update.callback_query.edit_message_text("❌ 系统错误，无法获取bot实例")
            else:
                await update.message.reply_text("❌ 系统错误，无法获取bot实例")
            return
            
        user_id = update.effective_user.id
        logger.info(f"用户ID: {user_id}, 开始处理轮播消息表单")
        
        # 清理旧的设置管理器状态
        active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
        logger.info(f"用户 {user_id} 的活动设置状态: {active_settings}")
    
        # 清理轮播相关的所有状态
        if 'broadcast' in active_settings:
            await bot_instance.settings_manager.clear_setting_state(user_id, 'broadcast')
            logger.info(f"已清理用户 {user_id} 的旧轮播设置状态")
    
        # 清理context.user_data中的旧表单数据
        for key in list(context.user_data.keys()):
            if key.startswith('broadcast_') or key == 'waiting_for':
                del context.user_data[key]
                logger.info(f"已清理用户数据中的键: {key}")
    
        # 初始化新的表单数据
        context.user_data['broadcast_form'] = {
            'group_id': group_id,
            'text': '',
            'media': None,
            'buttons': [],
            'repeat_type': 'once',    # 默认只发送一次
            'repeat_interval': 0,     # 默认间隔（分钟）
            'start_time': None,       # 开始时间
            'end_time': None          # 结束时间
        }
        logger.info(f"已为用户 {user_id} 初始化新的轮播消息表单数据")
    
        # 显示内容添加选项
        await show_broadcast_content_options(update, context)

    except Exception as e:
        logger.error(f"启动轮播消息表单流程出错: {e}", exc_info=True)
        if update.callback_query:
            await update.callback_query.edit_message_text(f"❌ 启动轮播消息表单出错: {str(e)}")
        else:
            await update.message.reply_text(f"❌ 启动轮播消息表单出错: {str(e)}")
        return

async def show_broadcast_content_options(update: Update, context: CallbackContext):
    """
    显示轮播消息内容选项
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    # 构建选项按钮
    keyboard = [
        [InlineKeyboardButton("📝 添加文本", callback_data="bcform_add_text")],
        [InlineKeyboardButton("🖼️ 添加媒体", callback_data="bcform_add_media")],
        [InlineKeyboardButton("🔘 添加按钮", callback_data="bcform_add_button")],
        [InlineKeyboardButton("❌ 取消", callback_data="bcform_cancel")]
    ]
    
    # 根据情境使用不同的发送方式
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "📢 轮播消息添加向导\n\n请选择要添加的内容类型：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            "📢 轮播消息添加向导\n\n请选择要添加的内容类型：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def show_broadcast_options(update: Update, context: CallbackContext):
    """
    显示轮播消息设置选项
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    logger.info("显示轮播消息设置选项")
    form_data = context.user_data.get('broadcast_form', {})
    logger.info(f"当前轮播消息表单数据: {form_data}")
    
    # 构建当前状态摘要
    summary = "📢 轮播消息添加向导\n\n"
    summary += f"• 内容: {'✅ 已设置文本' if form_data.get('text') else '❌ 未设置文本'}\n"
    summary += f"• 媒体: {'✅ 已设置' if form_data.get('media') else '❌ 未设置'}\n"
    summary += f"• 按钮: {len(form_data.get('buttons', []))} 个\n"
    
    # 显示计划信息
    if form_data.get('repeat_type'):
        repeat_type = form_data.get('repeat_type')
        if repeat_type == 'once':
            summary += "• 发送类型: 单次发送\n"
        elif repeat_type == 'hourly':
            summary += "• 发送类型: 每小时发送\n"
        elif repeat_type == 'daily':
            summary += "• 发送类型: 每日发送\n"
        elif repeat_type == 'custom':
            interval = form_data.get('repeat_interval', 0)
            summary += f"• 发送类型: 自定义（每{interval}分钟）\n"
    
    # 显示开始时间
    if form_data.get('start_time'):
        start_time = form_data.get('start_time')
        if start_time.lower() == 'now':
            summary += "• 开始时间: 立即开始\n"
        else:
            try:
                dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                summary += f"• 开始时间: {format_datetime(dt)}\n"
            except ValueError:
                summary += f"• 开始时间: {start_time}\n"
    else:
        summary += "• 开始时间: ❌ 未设置\n"

    # 显示结束时间
    if form_data.get('end_time'):
        end_time = form_data.get('end_time')
        try:
            dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            summary += f"• 结束时间: {format_datetime(dt)}\n"
        except ValueError:
            summary += f"• 结束时间: {end_time}\n"
    else:
        if form_data.get('repeat_type') != 'once':
            summary += "• 结束时间: ❌ 未设置（将使用默认的30天）\n"
    
    summary += "\n请选择要添加或修改的内容:"
    
    # 构建操作按钮
    keyboard = [
        [InlineKeyboardButton("📝 添加/修改文本", callback_data=f"bcform_add_text")],
        [InlineKeyboardButton("🖼️ 添加/修改媒体", callback_data=f"bcform_add_media")],
        [InlineKeyboardButton("🔘 添加/修改按钮", callback_data=f"bcform_add_button")],
        [InlineKeyboardButton("⏰ 设置计划", callback_data=f"bcform_set_schedule")],
    ]
    
    # 如果不是单次发送，添加结束时间设置按钮
    if form_data.get('repeat_type') and form_data.get('repeat_type') != 'once':
        keyboard.append([InlineKeyboardButton("🏁 设置结束时间", callback_data=f"bcform_set_end_time")])
    
    keyboard.extend([
        [InlineKeyboardButton("👁️ 预览效果", callback_data=f"bcform_preview")],
        [InlineKeyboardButton("✅ 提交", callback_data=f"bcform_submit")],
        [InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]
    ])
    # 检查是否至少有一项内容和计划设置
    has_content = bool(form_data.get('text') or form_data.get('media') or form_data.get('buttons'))
    has_schedule = bool(form_data.get('start_time'))
    
    if not has_content:
        summary += "\n\n⚠️ 请至少添加一项内容(文本/媒体/按钮)"
    if not has_schedule:
        summary += "\n\n⚠️ 请设置发送计划"
    
    # 显示表单选项
    await update.callback_query.edit_message_text(
        summary,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_schedule_options(update: Update, context: CallbackContext):
    """
    显示轮播计划选项
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    # 构建重复类型选择按钮
    keyboard = [
        [InlineKeyboardButton("单次发送", callback_data="bcform_set_repeat_once")],
        [InlineKeyboardButton("每小时发送", callback_data="bcform_set_repeat_hourly")],
        [InlineKeyboardButton("每天发送", callback_data="bcform_set_repeat_daily")],
        [InlineKeyboardButton("自定义间隔", callback_data="bcform_set_repeat_custom")],
        [InlineKeyboardButton("返回", callback_data="bcform_content_received")],
        [InlineKeyboardButton("❌ 取消", callback_data="bcform_cancel")]
    ]
    
    await update.callback_query.edit_message_text(
        "📢 设置轮播计划\n\n请选择轮播消息的重复类型：",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_start_time_options(update: Update, context: CallbackContext):
    """
    显示开始时间选项
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    # 显示时间设置选项
    keyboard = [
        [InlineKeyboardButton("立即开始", callback_data="bcform_set_start_time")],
        [InlineKeyboardButton("设置未来时间", callback_data="bcform_set_start_time")],
        [InlineKeyboardButton("返回", callback_data="bcform_set_schedule")],
        [InlineKeyboardButton("❌ 取消", callback_data="bcform_cancel")]
    ]
    
    await update.callback_query.edit_message_text(
        "📢 设置开始时间\n\n请选择轮播消息的开始时间：",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def preview_broadcast_content(update: Update, context: CallbackContext):
    """
    预览轮播消息内容
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    logger.info("预览轮播消息内容")
    form_data = context.user_data.get('broadcast_form', {})
    logger.info(f"预览的轮播消息表单数据: {form_data}")
    
    # 获取内容数据
    text = form_data.get('text', '')
    media = form_data.get('media')
    buttons = form_data.get('buttons', [])
    
    # 创建按钮键盘(如果有)
    reply_markup = None
    if buttons:
        keyboard = []
        for button in buttons:
            keyboard.append([InlineKeyboardButton(button['text'], url=button['url'])])
        reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 发送预览消息
    try:
        if media and media.get('type'):
            if media['type'] == 'photo':
                await update.callback_query.message.reply_photo(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'video':
                await update.callback_query.message.reply_video(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'document':
                await update.callback_query.message.reply_document(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            else:
                await update.callback_query.message.reply_document(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
        elif text or buttons:
            await update.callback_query.message.reply_text(
                text or "轮播消息内容",
                reply_markup=reply_markup
            )
        else:
            await update.callback_query.answer("没有预览内容")
            await show_broadcast_options(update, context)
            return
    except Exception as e:
        logger.error(f"预览生成错误: {e}")
        await update.callback_query.answer(f"预览生成失败: {str(e)}")
        await show_broadcast_options(update, context)
        return
    
    # 返回表单选项
    keyboard = [
        [InlineKeyboardButton("🔙 返回", callback_data=f"bcform_content_received")]
    ]
    await update.callback_query.edit_message_text(
        "👆 上方为轮播消息内容预览\n\n点击「返回」继续编辑",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
