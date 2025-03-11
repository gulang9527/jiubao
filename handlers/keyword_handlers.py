"""
关键词处理函数，处理关键词相关操作
"""
import logging
from typing import Dict, Any, Optional, List

from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

from utils.decorators import handle_callback_errors
from utils.message_utils import get_media_type, get_file_id
from db.models import GroupPermission

logger = logging.getLogger(__name__)

#######################################
# 回调处理函数
#######################################

@handle_callback_errors
async def handle_keyword_form_callback(update: Update, context: CallbackContext, data: str):
    """
    处理关键词表单回调
    
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
    logger.info(f"处理关键词表单回调: {parts}")

    # 验证基本格式
    if len(parts) < 2 or parts[0] != "kwform":
        logger.error(f"关键词回调数据格式错误: {data}")
        await query.edit_message_text("❌ 无效的操作")
        return

    # 提取操作和参数
    # 格式: kwform_操作_参数1_参数2...
    action = parts[1]
    params = parts[2:] if len(parts) > 2 else []
    
    logger.info(f"关键词表单操作: {action}, 参数: {params}")
    form_data = context.user_data.get('keyword_form', {})
    logger.info(f"当前关键词表单数据: {form_data}")

    # 处理不同的表单操作
    if action == "cancel":
        # 取消操作
        if 'keyword_form' in context.user_data:
            del context.user_data['keyword_form']
        if 'waiting_for' in context.user_data:
            del context.user_data['waiting_for']
        await query.edit_message_text("✅ 已取消关键词添加")
        
    elif action == "select" and len(params) >= 2 and params[0] == "group":
        # 选择群组: kwform_select_group_123
        try:
            group_id = int(params[1])
            await start_keyword_form(update, context, group_id)
        except (ValueError, IndexError):
            logger.error(f"无效的群组ID: {params[1] if len(params) > 1 else 'missing'}")
            await query.edit_message_text("❌ 无效的群组ID")
        
    elif action == "type" and len(params) >= 1:
        # 选择匹配类型: kwform_type_exact 或 kwform_type_regex
        match_type = params[0]
        if match_type not in ["exact", "regex"]:
            logger.error(f"未提供有效的匹配类型: {match_type}")
            await query.edit_message_text("❌ 无效的匹配类型")
            return
            
        form_data['match_type'] = match_type
        context.user_data['keyword_form'] = form_data
        
        # 提示输入关键词
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data="kwform_cancel")]]
        await query.edit_message_text(
            f"已选择: {'精确匹配' if match_type == 'exact' else '正则匹配'}\n\n"
            "请发送关键词内容: \n"
            f"({'支持正则表达式' if match_type == 'regex' else '精确匹配文字'})\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        # 设置等待输入状态
        context.user_data['waiting_for'] = 'keyword_pattern'
        
    elif action == "pattern" and len(params) >= 1 and params[0] == "received":
        # 已收到关键词模式: kwform_pattern_received
        await show_keyword_response_options(update, context)
        
    elif action == "edit" and len(params) >= 1 and params[0] == "pattern":
        # 修改关键词模式: kwform_edit_pattern
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data="kwform_cancel")]]
        await query.edit_message_text(
            f"当前关键词: {form_data.get('pattern', '')}\n\n"
            "请发送新的关键词内容:\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'keyword_pattern'
        
    elif action == "add" and len(params) >= 1:
        # 添加各种回复: kwform_add_text, kwform_add_media, kwform_add_button
        add_type = params[0]
        
        if add_type == "text":
            # 添加文本响应
            keyboard = [[InlineKeyboardButton("❌ 取消", callback_data="kwform_cancel")]]
            await query.edit_message_text(
                "请发送关键词回复的文本内容:\n\n"
                "发送完后请点击下方出现的「继续」按钮",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data['waiting_for'] = 'keyword_response'
            
        elif add_type == "media":
            # 添加媒体响应
            keyboard = [[InlineKeyboardButton("❌ 取消", callback_data="kwform_cancel")]]
            await query.edit_message_text(
                "请发送要添加的媒体:\n"
                "• 图片\n"
                "• 视频\n"
                "• 文件\n\n"
                "发送完后请点击下方出现的「继续」按钮",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data['waiting_for'] = 'keyword_media'
            
        elif add_type == "button":
            # 添加按钮
            keyboard = [[InlineKeyboardButton("❌ 取消", callback_data="kwform_cancel")]]
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
            context.user_data['waiting_for'] = 'keyword_buttons'
            
        else:
            logger.warning(f"未知的添加类型: {add_type}")
            await query.edit_message_text("❌ 未知的添加类型")
        
    elif action in ["response", "media", "buttons"] and len(params) >= 1 and params[0] == "received":
        # 已收到各类数据: kwform_response_received, kwform_media_received, kwform_buttons_received
        await show_keyword_response_options(update, context)
        
    elif action == "preview":
        # 预览关键词响应: kwform_preview
        await preview_keyword_response(update, context)
        
    elif action == "submit":
        # 提交关键词: kwform_submit
        await submit_keyword_form(update, context)
        
    else:
        logger.warning(f"未知的关键词表单操作: {action}, 参数: {params}")
        await query.edit_message_text("❌ 未知操作")

@handle_callback_errors
async def handle_keyword_detail_callback(update: Update, context: CallbackContext, data: str):
    """
    处理查看关键词详情的回调
    
    参数:
        update: 更新对象
        context: 上下文对象
        data: 回调数据
    """
    query = update.callback_query
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 立即应答回调查询
    await query.answer()
    
    # 解析回调数据获取关键词ID和群组ID
    parts = data.split('_')
    logger.info(f"关键词详情回调数据: {parts}")
    
    if len(parts) < 3:  # 应该有至少3部分: keyword, detail, keyword_id, (可能还有group_id)
        logger.error(f"关键词详情回调数据格式错误: {data}")
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    keyword_id = parts[2]  # 第三部分是keyword_id
    group_id = int(parts[3]) if len(parts) > 3 else None  # 第四部分是group_id (如果存在)
    
    logger.info(f"查看关键词详情: {keyword_id}, 群组ID: {group_id}")
    
    # 获取关键词详情
    try:
        keyword = await bot_instance.db.get_keyword_by_id(group_id, keyword_id)
        logger.info(f"关键词数据类型: {type(keyword)}, 内容: {keyword}")
        if not keyword:
            logger.warning(f"找不到关键词: {keyword_id}")
            await query.edit_message_text("❌ 找不到关键词")
            return
        
        # 获取匹配类型和模式
        match_type = keyword.get('type', 'exact')
        pattern = keyword.get('pattern', '无')
        match_type_text = '精确匹配' if match_type == 'exact' else '正则匹配'
        
        # 获取媒体类型和文本内容
        media_type = keyword.get('media', {}).get('type', '无')
        media_info = f"📎 媒体类型: {media_type}" if media_type else "📝 仅文本回复"
        text = keyword.get('response', '无文本内容')
        
        # 获取按钮数量
        buttons_count = len(keyword.get('buttons', []))
        buttons_info = f"🔘 {buttons_count} 个按钮" if buttons_count > 0 else "无按钮"
        
        # 构建详情文本
        detail_text = (
            f"🔑 关键词详情\n\n"
            f"📋 关键词: {pattern}\n"
            f"🔍 匹配方式: {match_type_text}\n\n"
            f"{media_info}\n\n"
            f"📝 回复内容:\n{text[:200]}{'...' if len(text) > 200 else ''}\n\n"
            f"{buttons_info}\n"
        )
        
        # 构建操作按钮
        keyboard = [
            [InlineKeyboardButton("👁️ 预览", callback_data=f"keyword_preview_{keyword_id}_{group_id}")],
            [InlineKeyboardButton("❌ 删除", callback_data=f"keyword_delete_{keyword_id}_{group_id}")],
            [InlineKeyboardButton("🔙 返回", callback_data=f"settings_keywords_{group_id}")]
        ]
        
        # 显示关键词详情
        await query.edit_message_text(
            detail_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"查看关键词详情出错: {str(e)}", exc_info=True)
        await query.edit_message_text(
            f"❌ 查看关键词详情出错: {str(e)}\n\n"
            f"请返回并重试",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 返回", callback_data=f"settings_keywords_{group_id}")
            ]])
        )
    
@handle_callback_errors
async def handle_keyword_preview_callback(update: Update, context: CallbackContext, data: str):
    """
    处理预览关键词的回调
    
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
    if len(parts) < 4:  # keyword, preview, keyword_id, group_id
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    keyword_id = parts[2]
    group_id = int(parts[3])
    
    # 获取关键词
    keyword = await bot_instance.db.get_keyword_by_id(group_id, keyword_id)
    if not keyword:
        await query.edit_message_text("❌ 找不到关键词")
        return
    
    # 获取内容数据
    text = keyword.get('response', '')
    media = keyword.get('media')
    buttons = keyword.get('buttons', [])
    
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
                await query.message.reply_photo(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'video':
                await query.message.reply_video(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'document':
                await query.message.reply_document(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            else:
                await query.message.reply_document(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
        elif text or buttons:
            await query.message.reply_text(
                text or "关键词回复内容",
                reply_markup=reply_markup
            )
        else:
            await query.answer("没有预览内容")
            return
    except Exception as e:
        logger.error(f"预览生成错误: {e}")
        await query.answer(f"预览生成失败: {str(e)}")
        return
    
    # 显示返回按钮
    keyboard = [
        [InlineKeyboardButton("🔙 返回详情", callback_data=f"keyword_detail_{keyword_id}_{group_id}")]
    ]
    await query.edit_message_text(
        "👆 上方为关键词回复预览\n\n点击「返回详情」继续查看",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@handle_callback_errors
async def handle_keyword_delete_callback(update: Update, context: CallbackContext, data: str):
    """
    处理删除关键词的回调
    
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
    if len(parts) < 4:  # keyword, delete, keyword_id, group_id
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    keyword_id = parts[2]
    group_id = int(parts[3])
    
    # 确认删除
    keyboard = [
        [
            InlineKeyboardButton("✅ 确认删除", callback_data=f"keyword_confirm_delete_{keyword_id}_{group_id}"),
            InlineKeyboardButton("❌ 取消", callback_data=f"keyword_detail_{keyword_id}_{group_id}")
        ]
    ]
    
    await query.edit_message_text(
        "⚠️ 确定要删除这个关键词吗？\n\n此操作不可撤销。",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@handle_callback_errors
async def handle_keyword_confirm_delete_callback(update: Update, context: CallbackContext, data: str):
    """
    处理确认删除关键词的回调
    
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
    if len(parts) < 5:  # keyword, confirm, delete, keyword_id, group_id
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    keyword_id = parts[3]
    group_id = int(parts[4])
    
    # 删除关键词
    try:
        result = await bot_instance.db.delete_keyword(keyword_id)
        if result:
            await query.edit_message_text(
                "✅ 关键词已删除",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("返回关键词列表", callback_data=f"settings_keywords_{group_id}")
                ]])
            )
        else:
            await query.edit_message_text(
                "❌ 删除关键词失败",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("返回关键词详情", callback_data=f"keyword_detail_{keyword_id}_{group_id}")
                ]])
            )
    except Exception as e:
        logger.error(f"删除关键词出错: {e}")
        await query.edit_message_text(
            f"❌ 删除关键词出错: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("返回关键词详情", callback_data=f"keyword_detail_{keyword_id}_{group_id}")
            ]])
        )
        
#######################################
# 表单输入处理
#######################################

async def handle_keyword_form_input(update: Update, context: CallbackContext, input_type: str) -> bool:
    """
    处理关键词表单输入
    
    参数:
        update: 更新对象
        context: 上下文对象
        input_type: 输入类型
        
    返回:
        是否处理了输入
    """
    message = update.effective_message
    form_data = context.user_data.get('keyword_form', {})
    user_id = update.effective_user.id
    
    if not form_data:
        logger.warning(f"用户 {user_id} 处于关键词输入模式但无表单数据")
        await message.reply_text("❌ 关键词表单数据丢失，请重新开始")
        context.user_data.pop('waiting_for', None)
        return True
        
    # 根据输入类型处理
    if input_type == 'keyword_pattern':
        # 接收关键词模式
        pattern = message.text
        if not pattern or len(pattern) > 100:
            await message.reply_text("❌ 关键词长度必须在1-100字符之间")
            return True
            
        # 存储关键词模式
        form_data['pattern'] = pattern
        context.user_data['keyword_form'] = form_data
        context.user_data.pop('waiting_for', None)
        
        # 提供继续按钮
        keyboard = [[InlineKeyboardButton("继续", callback_data="kwform_pattern_received")]]
        await message.reply_text(
            f"✅ 已设置关键词: {pattern}\n\n点击「继续」设置回复内容",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    elif input_type == 'keyword_response':
        # 接收关键词回复文本
        response = message.text
        if not response or len(response) > 1000:
            await message.reply_text("❌ 回复内容长度必须在1-1000字符之间")
            return True
            
        # 存储回复文本
        form_data['response'] = response
        context.user_data['keyword_form'] = form_data
        context.user_data.pop('waiting_for', None)
        
        # 提供继续按钮
        keyboard = [[InlineKeyboardButton("继续", callback_data="kwform_response_received")]]
        await message.reply_text(
            f"✅ 已设置回复文本\n\n点击「继续」进行下一步",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    elif input_type == 'keyword_media':
        # 接收关键词回复媒体
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
        context.user_data['keyword_form'] = form_data
        context.user_data.pop('waiting_for', None)
        
        # 提供继续按钮
        keyboard = [[InlineKeyboardButton("继续", callback_data="kwform_media_received")]]
        await message.reply_text(
            f"✅ 已设置{media_type}媒体\n\n点击「继续」进行下一步",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    elif input_type == 'keyword_buttons':
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
        context.user_data['keyword_form'] = form_data
        context.user_data.pop('waiting_for', None)
        
        # 提供继续按钮
        keyboard = [[InlineKeyboardButton("继续", callback_data="kwform_buttons_received")]]
        await message.reply_text(
            f"✅ 已设置 {len(buttons)} 个按钮\n\n点击「继续」进行下一步",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
        
    return False

#######################################
# 表单功能函数
#######################################

async def start_keyword_form(update: Update, context: CallbackContext, group_id: int):
    """
    启动关键词表单流程
    
    参数:
        update: 更新对象
        context: 上下文对象
        group_id: 群组ID
    """
    try:
        logger.info(f"启动关键词表单流程，群组ID: {group_id}")
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
        logger.info(f"用户ID: {user_id}, 开始处理关键词表单")
        
        # 清理旧的设置管理器状态
        active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
        logger.info(f"用户 {user_id} 的活动设置状态: {active_settings}")
    
        # 清理关键词相关的所有状态
        if 'keyword' in active_settings:
            await bot_instance.settings_manager.clear_setting_state(user_id, 'keyword')
            logger.info(f"已清理用户 {user_id} 的旧关键词设置状态")
    
        # 清理context.user_data中的旧表单数据
        for key in list(context.user_data.keys()):
            if key.startswith('keyword_') or key == 'waiting_for':
                del context.user_data[key]
                logger.info(f"已清理用户数据中的键: {key}")
    
        # 初始化新的表单数据
        context.user_data['keyword_form'] = {
            'group_id': group_id,
            'match_type': 'exact',  # 默认精确匹配
            'pattern': '',
            'response': '',
            'media': None,
            'buttons': []
        }
        logger.info(f"已为用户 {user_id} 初始化新的关键词表单数据")
    
        # 显示匹配类型选择
        keyboard = [
            [
                InlineKeyboardButton("精确匹配", callback_data=f"kwform_type_exact"),
                InlineKeyboardButton("正则匹配", callback_data=f"kwform_type_regex")
            ],
            [InlineKeyboardButton("❌ 取消", callback_data=f"kwform_cancel")]
        ]
    
        # 根据情境使用不同的发送方式
        if update.callback_query:
            await update.callback_query.edit_message_text(
                "📝 关键词添加向导\n\n请选择匹配类型：\n\n"
                "• 精确匹配：完全匹配输入的文本\n"
                "• 正则匹配：使用正则表达式匹配模式",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text(
                "📝 关键词添加向导\n\n请选择匹配类型：\n\n"
                "• 精确匹配：完全匹配输入的文本\n"
                "• 正则匹配：使用正则表达式匹配模式",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

    except Exception as e:
        logger.error(f"启动关键词表单流程出错: {e}", exc_info=True)
        if update.callback_query:
            await update.callback_query.edit_message_text(f"❌ 启动关键词表单出错: {str(e)}")
        else:
            await update.message.reply_text(f"❌ 启动关键词表单出错: {str(e)}")
        return

async def show_keyword_response_options(update: Update, context: CallbackContext):
    """
    显示关键词响应选项
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    logger.info("显示关键词响应选项")
    form_data = context.user_data.get('keyword_form', {})
    logger.info(f"当前关键词表单数据: {form_data}")
    
    # 构建当前状态摘要
    summary = "📝 关键词添加向导\n\n"
    summary += f"• 匹配类型: {'精确匹配' if form_data.get('match_type') == 'exact' else '正则匹配'}\n"
    summary += f"• 关键词: {form_data.get('pattern', '未设置')}\n"
    summary += f"• 文本回复: {'✅ 已设置' if form_data.get('response') else '❌ 未设置'}\n"
    summary += f"• 媒体回复: {'✅ 已设置' if form_data.get('media') else '❌ 未设置'}\n"
    summary += f"• 按钮: {len(form_data.get('buttons', []))} 个\n\n"
    summary += "请选择要添加或修改的内容:"
    
    # 构建操作按钮
    keyboard = [
        [InlineKeyboardButton("✏️ 修改关键词", callback_data=f"kwform_edit_pattern")],
        [InlineKeyboardButton("📝 添加/修改文本", callback_data=f"kwform_add_text")],
        [InlineKeyboardButton("🖼️ 添加/修改媒体", callback_data=f"kwform_add_media")],
        [InlineKeyboardButton("🔘 添加/修改按钮", callback_data=f"kwform_add_button")],
        [InlineKeyboardButton("👁️ 预览效果", callback_data=f"kwform_preview")],
        [InlineKeyboardButton("✅ 提交", callback_data=f"kwform_submit")],
        [InlineKeyboardButton("❌ 取消", callback_data=f"kwform_cancel")]
    ]
    
    # 检查是否至少有一项回复内容
    has_content = bool(form_data.get('response') or form_data.get('media') or form_data.get('buttons'))
    if not has_content:
        summary += "\n\n⚠️ 请至少添加一项回复内容(文本/媒体/按钮)"
    
    # 显示表单选项
    await update.callback_query.edit_message_text(
        summary,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def preview_keyword_response(update: Update, context: CallbackContext):
    """
    预览关键词响应效果
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    logger.info("预览关键词响应效果")
    form_data = context.user_data.get('keyword_form', {})
    logger.info(f"预览的关键词表单数据: {form_data}")
    
    # 获取回复数据
    text = form_data.get('response', '')
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
                text or "关键词回复",
                reply_markup=reply_markup
            )
        else:
            await update.callback_query.answer("没有预览内容")
            await show_keyword_response_options(update, context)
            return
    except Exception as e:
        logger.error(f"预览生成错误: {e}")
        await update.callback_query.answer(f"预览生成失败: {str(e)}")
    
    # 返回表单选项
    keyboard = [
        [InlineKeyboardButton("🔙 返回", callback_data=f"kwform_response_received")]
    ]
    await update.callback_query.edit_message_text(
        "👆 上方为关键词触发效果预览\n\n点击「返回」继续编辑",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def submit_keyword_form(update: Update, context: CallbackContext):
    """
    提交关键词表单
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    logger.info("提交关键词表单")
    form_data = context.user_data.get('keyword_form', {})
    logger.info(f"提交的表单数据: {form_data}")
    
    # 验证必要字段
    pattern = form_data.get('pattern')
    if not pattern:
        await update.callback_query.answer("❌ 关键词不能为空")
        await show_keyword_response_options(update, context)
        return
    
    # 检查是否有回复内容
    has_content = bool(form_data.get('response') or form_data.get('media') or form_data.get('buttons'))
    if not has_content:
        await update.callback_query.answer("❌ 请至少添加一项回复内容")
        await show_keyword_response_options(update, context)
        return
    
    # 构建关键词数据
    keyword_data = {
        'group_id': form_data['group_id'],
        'pattern': pattern,
        'type': form_data.get('match_type', 'exact'),
        'response': form_data.get('response', ''),
        'media': form_data.get('media'),
        'buttons': form_data.get('buttons', [])
    }
    
    # 添加关键词
    bot_instance = context.application.bot_data.get('bot_instance')
    try:
        await bot_instance.db.add_keyword(keyword_data)
        # 清理表单数据
        if 'keyword_form' in context.user_data:
            del context.user_data['keyword_form']
        if 'waiting_for' in context.user_data:
            del context.user_data['waiting_for']
        
        # 显示成功消息
        await update.callback_query.edit_message_text(
            "✅ 关键词添加成功！\n\n"
            f"关键词: {pattern}\n"
            f"匹配类型: {'精确匹配' if keyword_data['type'] == 'exact' else '正则匹配'}"
        )
    except Exception as e:
        logger.error(f"添加关键词错误: {e}")
        await update.callback_query.answer("❌ 添加关键词失败")
        await update.callback_query.edit_message_text(
            f"❌ 添加关键词失败: {str(e)}\n\n"
            "请重试或联系管理员"
        )
