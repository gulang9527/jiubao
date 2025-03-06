import logging
import asyncio
from datetime import datetime
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
Application, CommandHandler, MessageHandler, 
CallbackQueryHandler, filters, CallbackContext
)
from telegram.error import BadRequest

from middlewares import (
error_handler, require_admin, require_superadmin, 
check_command_usage, handle_callback_errors
)
from utils import validate_delete_timeout, validate_time_format, validate_interval, get_media_type

logger = logging.getLogger(__name__)

# 命令处理函数
@check_command_usage
async def handle_start(update: Update, context: CallbackContext):
    """处理/start命令"""
    if not update.effective_user or not update.message:
        return
        
    user_id = update.effective_user.id
    bot_instance = context.application.bot_data.get('bot_instance')
    is_superadmin = await bot_instance.is_superadmin(user_id)
    is_admin = await bot_instance.is_admin(user_id)
    
    # 构建欢迎文本
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
        "🚫 /cancel - 取消当前操作\n"
    )
    
    # 添加管理员命令
    if is_admin:
        welcome_text += (
            "\n管理员命令：\n"
            "👥 /admingroups - 查看可管理的群组\n"
            "⚙️ /settings - 群组设置管理\n"
        )
        
    # 添加超级管理员命令
    if is_superadmin:
        welcome_text += (
            "\n超级管理员命令：\n"
            "➕ /addsuperadmin <用户ID> - 添加超级管理员\n"
            "➖ /delsuperadmin <用户ID> - 删除超级管理员\n"
            "👤 /addadmin <用户ID> - 添加管理员\n"
            "🚫 /deladmin <用户ID> - 删除管理员\n"
            "✅ /authgroup <群组ID>  ... - 授权群组\n"
            "❌ /deauthgroup <群组ID> - 取消群组授权\n"
            "🔍 /checkconfig - 检查当前配置\n"
        )
        
    welcome_text += "\n如需帮助，请联系管理员。"
    await update.message.reply_text(welcome_text)

@check_command_usage
async def handle_settings(update: Update, context: CallbackContext):
    """处理/settings命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 获取用户可管理的群组
    manageable_groups = await bot_instance.db.get_manageable_groups(update.effective_user.id)
    if not manageable_groups:
        await update.message.reply_text("❌ 你没有权限管理任何群组")
        return
        
    # 构建群组选择键盘
    keyboard = []
    for group in manageable_groups:
        try:
            group_info = await context.bot.get_chat(group['group_id'])
            group_name = group_info.title or f"群组 {group['group_id']}"
        except Exception:
            group_name = f"群组 {group['group_id']}"
            
        keyboard.append([InlineKeyboardButton(group_name, callback_data=f"settings_select_{group['group_id']}")])
        
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("请选择要管理的群组：", reply_markup=reply_markup)

@check_command_usage
async def handle_rank_command(update: Update, context: CallbackContext):
    """处理/tongji和/tongji30命令"""
    if not update.effective_chat or not update.effective_user or not update.message:
        return
        
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 确定是哪个命令
    command = update.message.text.split('@')[0][1:]
    group_id = update.effective_chat.id
    
    # 检查权限
    from db import GroupPermission
    if not await bot_instance.has_permission(group_id, GroupPermission.STATS):
        await update.message.reply_text("❌ 此群组未启用统计功能")
        return
        
    # 解析页码
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
        stats, total_pages = await bot_instance.stats_manager.get_daily_stats(group_id, page)
        title = "📊 今日发言排行"
    else:
        stats, total_pages = await bot_instance.stats_manager.get_monthly_stats(group_id, page)
        title = "📊 近30天发言排行"
        
    # 检查是否有统计数据
    if not stats:
        await update.effective_user.send_message("📊 暂无统计数据")
        return
        
    # 构建排行文本
    text = f"{title}\n\n"
    for i, stat in enumerate(stats, start=(page-1)*15+1):
        try:
            user = await context.bot.get_chat_member(group_id, stat['_id'])
            name = user.user.full_name or user.user.username or f"用户{stat['_id']}"
        except Exception:
            name = f"用户{stat['_id']}"
            
        text += f"{i}. {name}\n   消息数: {stat['total_messages']}\n\n"
        
    # 添加分页信息
    text += f"\n\n第 {page}/{total_pages} 页"
    if total_pages > 1:
        text += f"\n使用 /{command} <页码> 查看其他页"
        
    # 发送排行消息
    msg = await update.effective_user.send_message(text)
    
    # 处理自动删除
    settings = await bot_instance.db.get_group_settings(group_id)
    if settings.get('auto_delete', False):
        timeout = validate_delete_timeout(message_type='ranking')
        asyncio.create_task(bot_instance._schedule_delete(msg, timeout))

@check_command_usage
async def handle_admin_groups(update: Update, context: CallbackContext):
    """处理/admingroups命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查权限
    if not await bot_instance.is_admin(update.effective_user.id):
        await update.message.reply_text("❌ 只有管理员可以使用此命令")
        return
        
    # 获取可管理的群组
    groups = await bot_instance.db.get_manageable_groups(update.effective_user.id)
    if not groups:
        await update.message.reply_text("📝 你目前没有可管理的群组")
        return
        
    # 构建群组列表文本
    text = "📝 你可以管理的群组：\n\n"
    for group in groups:
        try:
            group_info = await context.bot.get_chat(group['group_id'])
            group_name = group_info.title
        except Exception:
            group_name = f"群组 {group['group_id']}"
            
        text += f"• {group_name}\n  ID: {group['group_id']}\n  权限: {', '.join(group.get('permissions', []))}\n\n"
        
    await update.message.reply_text(text)

@check_command_usage
@require_superadmin
async def handle_add_admin(update: Update, context: CallbackContext):
    """处理/addadmin命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：/addadmin <用户ID>")
        return
        
    try:
        # 解析用户ID并添加管理员
        user_id = int(context.args[0])
        
        # 检查用户是否已经是管理员
        from db import UserRole
        user = await bot_instance.db.get_user(user_id)
        if user and user['role'] in [UserRole.ADMIN.value, UserRole.SUPERADMIN.value]:
            await update.message.reply_text("❌ 该用户已经是管理员")
            return
            
        # 添加管理员
        await bot_instance.db.add_user({'user_id': user_id, 'role': UserRole.ADMIN.value})
        await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为管理员")
        
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
    except Exception as e:
        logger.error(f"添加管理员错误: {e}")
        await update.message.reply_text("❌ 添加管理员时出错")

@check_command_usage
@require_superadmin
async def handle_del_admin(update: Update, context: CallbackContext):
    """处理/deladmin命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：/deladmin <用户ID>")
        return
        
    try:
        # 解析用户ID
        user_id = int(context.args[0])
        
        # 检查用户
        from db import UserRole
        user = await bot_instance.db.get_user(user_id)
        if not user:
            await update.message.reply_text("❌ 该用户不是管理员")
            return
            
        # 不能删除超级管理员
        if user['role'] == UserRole.SUPERADMIN.value:
            await update.message.reply_text("❌ 不能删除超级管理员")
            return
            
        # 删除管理员
        await bot_instance.db.remove_user(user_id)
        await update.message.reply_text(f"✅ 已删除管理员 {user_id}")
        
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
    except Exception as e:
        logger.error(f"删除管理员错误: {e}")
        await update.message.reply_text("❌ 删除管理员时出错")

@check_command_usage
@require_superadmin
async def handle_add_superadmin(update: Update, context: CallbackContext):
    """处理/addsuperadmin命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：/addsuperadmin <用户ID>")
        return
        
    try:
        # 解析用户ID
        user_id = int(context.args[0])
        
        # 检查用户是否已经是超级管理员
        from db import UserRole
        user = await bot_instance.db.get_user(user_id)
        if user and user['role'] == UserRole.SUPERADMIN.value:
            await update.message.reply_text("❌ 该用户已经是超级管理员")
            return
            
        # 添加超级管理员
        await bot_instance.db.add_user({'user_id': user_id, 'role': UserRole.SUPERADMIN.value})
        await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为超级管理员")
        
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
    except Exception as e:
        logger.error(f"添加超级管理员错误: {e}")
        await update.message.reply_text("❌ 添加超级管理员时出错")

@check_command_usage
@require_superadmin
async def handle_del_superadmin(update: Update, context: CallbackContext):
    """处理/delsuperadmin命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：/delsuperadmin <用户ID>")
        return
        
    try:
        # 解析用户ID
        user_id = int(context.args[0])
        
        # 不能删除自己
        if user_id == update.effective_user.id:
            await update.message.reply_text("❌ 不能删除自己的超级管理员权限")
            return
            
        # 检查用户
        from db import UserRole
        user = await bot_instance.db.get_user(user_id)
        if not user or user['role'] != UserRole.SUPERADMIN.value:
            await update.message.reply_text("❌ 该用户不是超级管理员")
            return
            
        # 删除超级管理员
        await bot_instance.db.remove_user(user_id)
        await update.message.reply_text(f"✅ 已删除超级管理员 {user_id}")
        
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
    except Exception as e:
        logger.error(f"删除超级管理员错误: {e}")
        await update.message.reply_text("❌ 删除超级管理员时出错")

@check_command_usage
@require_superadmin
async def handle_check_config(update: Update, context: CallbackContext):
    """处理/checkconfig命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 获取配置信息
    from db import UserRole
    superadmins = await bot_instance.db.get_users_by_role(UserRole.SUPERADMIN.value)
    superadmin_ids = [user['user_id'] for user in superadmins]
    groups = await bot_instance.db.find_all_groups()
    
    # 构建配置文本
    config_text = "🔧 当前配置信息：\n\n👥 超级管理员：\n" + "\n".join(f"• {admin_id}" for admin_id in superadmin_ids)
    config_text += "\n\n📋 已授权群组：\n" + "\n".join(f"• 群组 {group['group_id']}\n  权限: {', '.join(group.get('permissions', []))}" for group in groups)
    
    await update.message.reply_text(config_text)

@require_superadmin
async def handle_auth_group(update: Update, context: CallbackContext):
    """处理/authgroup命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：\n/authgroup <群组ID>")
        return
        
    try:
        # 解析群组ID
        group_id = int(context.args[0])
        
        # 获取群组信息
        try:
            group_info = await context.bot.get_chat(group_id)
            group_name = group_info.title
        except Exception:
            await update.message.reply_text("❌ 无法获取群组信息，请确保机器人已加入该群组")
            return
            
        # 授权群组
        from db import GroupPermission
        import config
        all_permissions = [perm.value for perm in GroupPermission]
        await bot_instance.db.add_group({
            'group_id': group_id,
            'permissions': all_permissions,
            'settings': {'auto_delete': False, 'auto_delete_timeout': config.AUTO_DELETE_SETTINGS['default_timeout']},
            'feature_switches': {'keywords': True, 'stats': True, 'broadcast': True}
        })
        
        await update.message.reply_text(f"✅ 已授权群组\n群组：{group_name}\nID：{group_id}\n已启用全部功能")
        
    except ValueError:
        await update.message.reply_text("❌ 群组ID必须是数字")
    except Exception as e:
        logger.error(f"授权群组错误: {e}")
        await update.message.reply_text("❌ 授权群组时出错")

@check_command_usage
@require_superadmin
async def handle_deauth_group(update: Update, context: CallbackContext):
    """处理/deauthgroup命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：/deauthgroup <群组ID>")
        return
        
    try:
        # 解析群组ID
        group_id = int(context.args[0])
        
        # 检查群组
        group = await bot_instance.db.get_group(group_id)
        if not group:
            await update.message.reply_text("❌ 该群组未授权")
            return
            
        # 解除授权
        await bot_instance.db.remove_group(group_id)
        await update.message.reply_text(f"✅ 已解除群组 {group_id} 的所有授权")
        
    except ValueError:
        await update.message.reply_text("❌ 群组ID必须是数字")
    except Exception as e:
        logger.error(f"解除群组授权错误: {e}")
        await update.message.reply_text("❌ 解除群组授权时出错")

@check_command_usage
async def handle_cancel(update: Update, context: CallbackContext):
    """处理/cancel命令"""
    bot_instance = context.application.bot_data.get('bot_instance')
    user_id = update.effective_user.id
    
    # 获取活动的设置
    active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
    if not active_settings:
        await update.message.reply_text("❌ 当前没有正在进行的设置操作")
        return
        
    # 清除所有设置状态
    for setting_type in active_settings:
        await bot_instance.settings_manager.clear_setting_state(user_id, setting_type)
        
    await update.message.reply_text("✅ 已取消所有正在进行的设置操作")

# 消息处理函数
async def handle_message(update: Update, context: CallbackContext):
    """处理所有非命令消息"""
    logger.info("进入handle_message方法")
    
    # 基本检查
    if not update.effective_message or not update.effective_user or not update.effective_chat:
        logger.warning("消息缺少基本属性")
        return
        
    bot_instance = context.application.bot_data.get('bot_instance')
    message = update.effective_message
    user_id = update.effective_user.id
    group_id = update.effective_chat.id
    
    logger.info(f"处理消息 - 用户ID: {user_id}, 群组ID: {group_id}, 消息类型: {get_media_type(message) or 'text'}")
    
    # 检查用户活动设置状态
    active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
    logger.info(f"用户 {user_id} 的活动设置: {active_settings}")
    
    # === 第1部分: 旧的设置处理逻辑 (现在会返回 False) ===
    # 旧的关键词设置处理 - 已修改为不处理
    if await handle_keyword_setting(bot_instance, user_id, message):
        logger.info("消息被旧关键词设置流程处理")
        return
        
    # 旧的轮播设置处理 - 已修改为不处理
    if await handle_broadcast_setting(bot_instance, user_id, group_id, message):
        logger.info("消息被旧轮播设置流程处理")
        return
        
    # === 第2部分: 其他设置处理 ===
    # 处理统计设置
    if await bot_instance.settings_manager.process_setting(user_id, 'stats_min_bytes', message, 
                                                        lambda state, msg: process_min_bytes_setting(bot_instance, state, msg)):
        return
        
    if await bot_instance.settings_manager.process_setting(user_id, 'stats_daily_rank', message, 
                                                        lambda state, msg: process_daily_rank_setting(bot_instance, state, msg)):
        return
        
    if await bot_instance.settings_manager.process_setting(user_id, 'stats_monthly_rank', message, 
                                                        lambda state, msg: process_monthly_rank_setting(bot_instance, state, msg)):
        return
        
    # 处理自动删除设置
    if await bot_instance.settings_manager.process_setting(user_id, 'auto_delete_timeout', message, 
                                                        lambda state, msg: process_auto_delete_timeout(bot_instance, state, msg)):
        return
    
    # === 第3部分: 新的表单处理逻辑 ===
    # 处理表单等待输入
    waiting_for = context.user_data.get('waiting_for')
    if waiting_for:
        logger.info(f"处理用户 {user_id} 的表单输入: {waiting_for}")
        
        # 处理关键词表单输入
        if waiting_for == 'keyword_pattern':
            # 关键词模式输入处理代码...
            return True
            
        elif waiting_for == 'keyword_response':
            # 关键词响应文本处理代码...
            return True
            
        # ... 其他表单处理代码 ...
            
        elif waiting_for == 'broadcast_interval':
            # 广播间隔处理代码...
            return True
        
        # 如果waiting_for值未知，记录警告
        logger.warning(f"未知的waiting_for值: {waiting_for}")
    
    # === 第4部分: 常规消息处理 ===
    # 处理关键词回复
    from db import GroupPermission
    if message.text and await bot_instance.has_permission(group_id, GroupPermission.KEYWORDS):
        logger.info(f"检查关键词匹配 - 群组: {group_id}, 文本: {message.text[:20]}...")
        response = await bot_instance.keyword_manager.match_keyword(group_id, message.text, message)
        
        if response:
            await send_keyword_response(bot_instance, message, response, group_id)
    
    # 处理消息统计
    if await bot_instance.has_permission(group_id, GroupPermission.STATS):
        try:
            await bot_instance.stats_manager.add_message_stat(group_id, user_id, message)
        except Exception as e:
            logger.error(f"添加消息统计失败: {e}", exc_info=True)

async def send_keyword_response(bot_instance, original_message: Message, keyword_id: str, group_id: int):
    """发送关键词回复"""
    try:
        # 获取关键词数据
        keyword = await bot_instance.keyword_manager.get_keyword_by_id(group_id, keyword_id)
        if not keyword:
            logger.error(f"关键词 {keyword_id} 不存在")
            return
            
        # 准备消息内容
        text = keyword.get('response', '')
        media = keyword.get('media')
        buttons = keyword.get('buttons', [])
        
        # 创建内联键盘（如果有按钮）
        reply_markup = None
        if buttons:
            keyboard = []
            for button in buttons:
                keyboard.append([InlineKeyboardButton(button['text'], url=button['url'])])
            reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 根据内容组合发送不同类型的消息
        if media and media.get('type'):
            if media['type'] == 'photo':
                msg = await original_message.reply_photo(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'video':
                msg = await original_message.reply_video(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'document':
                msg = await original_message.reply_document(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            elif media['type'] == 'animation':
                msg = await original_message.reply_animation(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
            else:
                # 默认作为文档发送
                msg = await original_message.reply_document(
                    media['file_id'], caption=text, reply_markup=reply_markup
                )
        else:
            # 纯文本消息或者只有按钮的消息
            msg = await original_message.reply_text(
                text or "关键词回复", reply_markup=reply_markup
            )
            
        # 处理自动删除
        settings = await bot_instance.db.get_group_settings(group_id)
        if settings.get('auto_delete', False):
            timeout = validate_delete_timeout(message_type='keyword')
            asyncio.create_task(bot_instance._schedule_delete(msg, timeout))
            
    except Exception as e:
        logger.error(f"发送关键词回复出错: {e}", exc_info=True)

async def process_min_bytes_setting(bot_instance, state, message):
    """处理最小字节数设置"""
    group_id = state['group_id']
    try:
        value = int(message.text)
        if value < 0:
            await message.reply_text("❌ 最小字节数不能为负数")
            return
            
        # 更新设置
        settings = await bot_instance.db.get_group_settings(group_id)
        settings['min_bytes'] = value
        await bot_instance.db.update_group_settings(group_id, settings)
        
        # 清理设置状态
        await bot_instance.settings_manager.clear_setting_state(message.from_user.id, 'stats_min_bytes')
        
        # 通知用户完成
        await message.reply_text(f"✅ 最小统计字节数已设置为 {value} 字节")
    except ValueError:
        await message.reply_text("❌ 请输入一个有效的数字")

async def process_daily_rank_setting(bot_instance, state, message):
    """处理日排行显示数量设置"""
    group_id = state['group_id']
    try:
        value = int(message.text)
        if value < 1 or value > 50:
            await message.reply_text("❌ 显示数量必须在1-50之间")
            return
            
        # 更新设置
        settings = await bot_instance.db.get_group_settings(group_id)
        settings['daily_rank_size'] = value
        await bot_instance.db.update_group_settings(group_id, settings)
        
        # 清理设置状态
        await bot_instance.settings_manager.clear_setting_state(message.from_user.id, 'stats_daily_rank')
        
        # 通知用户完成
        await message.reply_text(f"✅ 日排行显示数量已设置为 {value}")
    except ValueError:
        await message.reply_text("❌ 请输入一个有效的数字")

async def process_monthly_rank_setting(bot_instance, state, message):
    """处理月排行显示数量设置"""
    group_id = state['group_id']
    try:
        value = int(message.text)
        if value < 1 or value > 50:
            await message.reply_text("❌ 显示数量必须在1-50之间")
            return
            
        # 更新设置
        settings = await bot_instance.db.get_group_settings(group_id)
        settings['monthly_rank_size'] = value
        await bot_instance.db.update_group_settings(group_id, settings)
        
        # 清理设置状态
        await bot_instance.settings_manager.clear_setting_state(message.from_user.id, 'stats_monthly_rank')
        
        # 通知用户完成
        await message.reply_text(f"✅ 月排行显示数量已设置为 {value}")
    except ValueError:
        await message.reply_text("❌ 请输入一个有效的数字")

async def handle_keyword_setting(bot_instance, user_id, message):
    """处理关键词设置流程（旧版，现已废弃）"""
    # 旧版实现现在总是返回False
    return False

async def handle_broadcast_setting(bot_instance, user_id, group_id, message):
    """处理轮播设置流程（旧版，现已废弃）"""
    # 旧版实现现在总是返回False
    return False

async def process_auto_delete_timeout(bot_instance, state, message):
    """处理自动删除超时设置"""
    group_id = state['group_id']
    try:
        timeout = int(message.text)
        if timeout < 60 or timeout > 86400:
            await message.reply_text("❌ 超时时间必须在60-86400秒之间")
            return
            
        # 更新设置
        settings = await bot_instance.db.get_group_settings(group_id)
        settings['auto_delete_timeout'] = timeout
        await bot_instance.db.update_group_settings(group_id, settings)
        
        # 清理设置状态
        await bot_instance.settings_manager.clear_setting_state(message.from_user.id, 'auto_delete_timeout')
        
        # 通知用户完成
        from utils import format_duration
        await message.reply_text(f"✅ 自动删除超时时间已设置为 {format_duration(timeout)}")
    except ValueError:
        await message.reply_text("❌ 请输入一个有效的数字")

# 回调处理函数
@handle_callback_errors
async def handle_settings_callback(update: Update, context: CallbackContext):
    """处理设置菜单的回调"""
    query = update.callback_query
    logger.info(f"收到回调查询: {query.id} at {query.message.date}")
    try:
        # 立即响应回调查询
        await query.answer()    
        data = query.data
        logger.info(f"处理回调数据: {data}")
        
        bot_instance = context.application.bot_data.get('bot_instance')
        
        # 处理返回群组列表的情况
        if data == "show_manageable_groups":
            try:
                await show_manageable_groups(bot_instance, query, context)
                return
            except Exception as e:
                logger.error(f"获取可管理群组失败: {e}", exc_info=True)
                await query.edit_message_text("❌ 获取群组列表失败，请重试")
                return
                
        # 解析回调数据
        parts = data.split('_')
        if len(parts) < 3:
            await query.edit_message_text("❌ 无效的回调数据格式")
            logger.error(f"无效的回调数据格式: {data}")
            return
            
        action = parts[1]
        
        # 获取群组ID
        try:
            group_id = int(parts[-1])
        except ValueError:
            await query.edit_message_text("❌ 无效的群组ID")
            logger.error(f"无效的群组ID: {parts[-1]}")
            return
            
        # 验证用户权限
        if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 你没有权限管理此群组")
            logger.warning(f"用户 {update.effective_user.id} 尝试管理无权限的群组 {group_id}")
            return
            
        # 处理不同的设置操作
        if action == "select":
            # 显示群组的设置菜单
            try:
                await show_settings_menu(bot_instance, query, group_id)
            except Exception as e:
                logger.error(f"显示群组 {group_id} 设置菜单失败: {e}", exc_info=True)
                await query.edit_message_text(f"❌ 获取群组 {group_id} 设置失败，请重试")
        elif action == "switches":
            # 显示功能开关设置
            try:
                await show_feature_switches(bot_instance, query, group_id)
            except Exception as e:
                logger.error(f"显示功能开关设置失败 - 群组: {group_id}, 错误: {e}", exc_info=True)
                await query.edit_message_text(f"❌ 获取功能开关设置失败，请重试")
        elif action in ["stats", "broadcast", "keywords"]:
            # 处理设置的各个子部分
            try:
                await handle_settings_section(bot_instance, query, context, group_id, action)
            except Exception as e:
                logger.error(f"处理设置子部分失败 - 群组: {group_id}, 操作: {action}, 错误: {e}", exc_info=True)
                await query.edit_message_text(f"❌ 操作失败，请重试")
    except Exception as e:
        try:
            await context.bot.send_message(chat_id=query.message.chat_id, text="❌ 处理请求时出错，请重试")
        except Exception as ex:
            logger.error(f"无法发送错误消息: {ex}", exc_info=True)

async def show_manageable_groups(bot_instance, query, context):
    """显示用户可管理的群组列表"""
    manageable_groups = await bot_instance.db.get_manageable_groups(query.from_user.id)
    if not manageable_groups:
        await query.edit_message_text("❌ 你没有权限管理任何群组")
        return  
        
    keyboard = []
    for group in manageable_groups:
        try:
            group_info = await context.bot.get_chat(group['group_id'])
            group_name = group_info.title or f"群组 {group['group_id']}"
        except Exception as e:
            logger.warning(f"获取群组 {group['group_id']} 信息失败: {e}")
            group_name = f"群组 {group['group_id']}"   
            
        keyboard.append([InlineKeyboardButton(group_name, callback_data=f"settings_select_{group['group_id']}")])
        
    await query.edit_message_text("请选择要管理的群组：", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_settings_menu(bot_instance, query, group_id: int):
    """显示群组设置菜单"""
    group = await bot_instance.db.get_group(group_id)
    if not group:
        await query.edit_message_text(f"❌ 找不到群组 {group_id} 的信息")
        return
        
    # 获取权限列表
    permissions = group.get('permissions', [])
    
    # 构建功能按钮
    buttons = []
    if 'stats' in permissions:
        buttons.append(InlineKeyboardButton("📊 统计设置", callback_data=f"settings_stats_{group_id}"))
    if 'broadcast' in permissions:
        buttons.append(InlineKeyboardButton("📢 轮播消息", callback_data=f"settings_broadcast_{group_id}"))
    if 'keywords' in permissions:
        buttons.append(InlineKeyboardButton("🔑 关键词设置", callback_data=f"settings_keywords_{group_id}"))
        
    # 添加开关设置按钮
    buttons.append(InlineKeyboardButton("⚙️ 开关设置", callback_data=f"settings_switches_{group_id}"))
    
    # 添加自动删除设置
    settings = await bot_instance.db.get_group_settings(group_id)
    auto_delete_status = '开启' if settings.get('auto_delete', False) else '关闭'
    buttons.append(InlineKeyboardButton(f"🗑️ 自动删除: {auto_delete_status}", 
                                    callback_data=f"auto_delete_toggle_{group_id}"))
                                    
    # 添加返回按钮
    buttons.append(InlineKeyboardButton("🔙 返回群组列表", callback_data="show_manageable_groups"))
    
    # 构建键盘
    keyboard = []
    for i in range(0, len(buttons), 2):
        row = buttons[i:i+2]
        keyboard.append(row)
        
    # 处理单个按钮的情况
    if len(buttons) % 2 != 0:
        keyboard[-1] = [buttons[-1]]
        
    # 显示设置菜单
    await query.edit_message_text(
        f"管理群组: {group_id}\n\n请选择要管理的功能：", 
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_settings_section(bot_instance, query, context, group_id: int, section: str):
    """处理设置的各个部分"""
    if section == "stats":
        # 显示统计设置
        settings = await bot_instance.db.get_group_settings(group_id)
        await show_stats_settings(bot_instance, query, group_id, settings)
    elif section == "broadcast":
        # 显示轮播消息设置
        await show_broadcast_settings(bot_instance, query, group_id)
    elif section == "keywords":
        # 显示关键词设置
        await show_keyword_settings(bot_instance, query, group_id)

async def show_stats_settings(bot_instance, query, group_id: int, settings: dict):
    """显示统计设置"""
    count_media_status = '✅ 开启' if settings.get('count_media', False) else '❌ 关闭'
    keyboard = [
        [InlineKeyboardButton(f"最小统计字节数: {settings.get('min_bytes', 0)} 字节", callback_data=f"stats_edit_min_bytes_{group_id}")],
        [InlineKeyboardButton(f"统计多媒体: {count_media_status}", callback_data=f"stats_edit_toggle_media_{group_id}")],
        [InlineKeyboardButton(f"日排行显示数量: {settings.get('daily_rank_size', 15)}", callback_data=f"stats_edit_daily_rank_{group_id}")],
        [InlineKeyboardButton(f"月排行显示数量: {settings.get('monthly_rank_size', 15)}", callback_data=f"stats_edit_monthly_rank_{group_id}")],
        [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
    ]
    await query.edit_message_text(f"群组 {group_id} 的统计设置", reply_markup=InlineKeyboardMarkup(keyboard))
    
async def show_broadcast_settings(bot_instance, query, group_id: int):
    """显示轮播消息设置"""
    broadcasts = await bot_instance.db.get_broadcasts(group_id)
    keyboard = []  
    
    # 显示现有的轮播消息
    for bc in broadcasts:
        content_type = bc.get('content_type', '未知类型')
        content = bc.get('content', '')
        content_preview = str(content)[:20] + '...' if len(str(content)) > 20 else str(content)   
        keyboard.append([
            InlineKeyboardButton(
                f"📢 {content_type}: {content_preview}", 
                callback_data=f"broadcast_detail_{bc['_id']}_{group_id}"
            )
        ])
        
    # 添加功能按钮
    keyboard.append([InlineKeyboardButton("➕ 添加轮播消息", callback_data=f"bcform_select_group_{group_id}")])
    keyboard.append([InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")])
    
    await query.edit_message_text(f"群组 {group_id} 的轮播消息设置", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_keyword_settings(bot_instance, query, group_id: int, page: int = 1):
    """显示关键词设置"""
    # 获取关键词列表
    keywords = await bot_instance.db.get_keywords(group_id)
    
    # 计算分页信息
    total_pages = (len(keywords) + 9) // 10
    if page < 1:
        page = 1
    if page > total_pages and total_pages > 0:
        page = total_pages
        
    # 获取当前页的关键词
    start_idx = (page - 1) * 10
    end_idx = min(start_idx + 10, len(keywords))
    page_keywords = keywords[start_idx:end_idx] if keywords else []
    
    # 构建关键词按钮
    keyboard = [
        [InlineKeyboardButton(f"🔑 {kw['pattern'][:20] + '...' if len(kw['pattern']) > 20 else kw['pattern']}", 
                            callback_data=f"keyword_detail_{kw['_id']}_{group_id}")] 
        for kw in page_keywords
    ]
    
    # 添加分页导航按钮
    if total_pages > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️ 上一页", callback_data=f"keyword_list_page_{page-1}_{group_id}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("下一页 ▶️", callback_data=f"keyword_list_page_{page+1}_{group_id}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
            
    # 添加功能按钮
    keyboard.append([InlineKeyboardButton("➕ 添加关键词", callback_data=f"kwform_select_group_{group_id}")])
    keyboard.append([InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")])
    
    # 构建显示文本
    text = f"群组 {group_id} 的关键词设置" + (f"\n第 {page}/{total_pages} 页" if total_pages > 1 else "")
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

@handle_callback_errors
async def handle_keyword_callback(update: Update, context: CallbackContext):
    """处理关键词回调"""
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split('_')
    
    # 验证回调数据格式
    if len(parts) < 3:
        await query.edit_message_text("❌ 无效的操作")
        return
        
    action = parts[1]
    group_id = int(parts[-1])
    user_id = update.effective_user.id
    
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查用户权限
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        await query.edit_message_text("❌ 无权限管理此群组")
        return
        
    # 检查群组权限
    from db import GroupPermission
    if not await bot_instance.has_permission(group_id, GroupPermission.KEYWORDS):
        await query.edit_message_text("❌ 此群组未启用关键词功能")
        return
        
    # 处理不同的操作
    if action == "add":
        # 添加关键词 - 选择匹配类型
        await start_keyword_form(update, context, group_id)
        return
        
    elif action == "type":
        # 选择关键词类型后的处理
        match_type = parts[2]
        logger.info(f"用户 {update.effective_user.id} 为群组 {group_id} 选择关键词匹配类型: {match_type}")
        
        # 清理已有的设置状态
        active_settings = await bot_instance.settings_manager.get_active_settings(update.effective_user.id)
        if 'keyword' in active_settings:
            await bot_instance.settings_manager.clear_setting_state(update.effective_user.id, 'keyword')
            
        # 创建新的设置状态
        await bot_instance.settings_manager.start_setting(update.effective_user.id, 'keyword', group_id)
        await bot_instance.settings_manager.update_setting_state(update.effective_user.id, 'keyword', {'match_type': match_type})
        
        # 提示用户输入关键词
        match_type_text = "精确匹配" if match_type == "exact" else "正则匹配"
        await query.edit_message_text(
            f"您选择了{match_type_text}方式\n\n请发送关键词内容：\n{'(支持正则表达式)' if match_type == 'regex' else ''}\n\n发送 /cancel 取消"
        )
        
    elif action == "detail":
        # 查看关键词详情
        if len(parts) < 4:
            await query.edit_message_text("❌ 无效的关键词ID")
            return
            
        keyword_id = parts[2]
        keyword = await bot_instance.keyword_manager.get_keyword_by_id(group_id, keyword_id)
        
        if not keyword:
            await query.edit_message_text("❌ 未找到该关键词")
            return
            
        # 获取关键词信息
        pattern = keyword['pattern']
        response_type = keyword['response_type']
        match_type = keyword['type']
        
        # 准备预览信息
        response_preview = "无法预览媒体内容" if response_type != 'text' else (
            keyword['response'][:100] + "..." if len(keyword['response']) > 100 else keyword['response']
        )
        response_type_text = {'text': '文本', 'photo': '图片', 'video': '视频', 'document': '文件'}.get(response_type, response_type)
        
        # 构建键盘
        keyboard = [
            [InlineKeyboardButton("❌ 删除此关键词", callback_data=f"keyword_delete_confirm_{keyword_id}_{group_id}")],
            [InlineKeyboardButton("🔙 返回列表", callback_data=f"settings_keywords_{group_id}")]
        ]
        
        # 显示详情
        text = (
            f"📝 关键词详情：\n\n"
            f"🔹 匹配类型：{'正则匹配' if match_type == 'regex' else '精确匹配'}\n"
            f"🔹 关键词：{pattern}\n"
            f"🔹 回复类型：{response_type_text}\n"
            f"🔹 回复内容：{response_preview}\n"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif action == "delete_confirm":
        # 确认删除关键词
        if len(parts) < 4:
            await query.edit_message_text("❌ 无效的关键词ID")
            return
            
        keyword_id = parts[2]
        logger.info(f"确认删除关键词 - 回调数据: {data}, 解析后的ID: {keyword_id}")
        
        try:
                # 尝试创建ObjectId验证格式是否正确
                from bson import ObjectId
                ObjectId(keyword_id)
        except Exception as e:
                logger.error(f"无效的关键词ID: {keyword_id}, 错误: {e}")
                await query.edit_message_text("❌ 无效的关键词ID格式")
                return
        
        # 构建确认键盘
        keyboard = [
            [InlineKeyboardButton("✅ 确认删除", callback_data=f"keyword_delete_confirm_{keyword_id}_{group_id}"),
            InlineKeyboardButton("❌ 取消", callback_data=f"keyword_detail_{keyword_id}_{group_id}")]
        ]
        
        # 显示确认消息
        await query.edit_message_text(
            f"⚠️ 确定要删除关键词「{pattern}」吗？\n此操作不可撤销！", 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    elif action == "delete":
        # 执行删除关键词
        if len(parts) >= 5 and parts[2] == "confirm":
            keyword_id = parts[3]  # 从正确位置获取 keyword_id
            logger.info(f"检测到confirm格式: 执行删除关键词 - ID: {keyword_id}")
        elif len(parts) < 4:
            await query.edit_message_text("❌ 无效的关键词ID")
            return
        else:
            keyword_id = parts[2]  # 常规格式
 
        logger.info(f"执行删除关键词 - 回调数据: {data}, 解析后的ID: {keyword_id}")
    
        # 检查是否为有效ID
        try:
            from bson import ObjectId
            ObjectId(keyword_id)
        except Exception as e:
            logger.error(f"无效的关键词ID: {keyword_id}, 错误: {e}")
            await query.edit_message_text("❌ 无效的关键词ID格式")
            return
                
        keyword = await bot_instance.keyword_manager.get_keyword_by_id(group_id, keyword_id)
        pattern = keyword['pattern'] if keyword else "未知关键词"
        
        # 删除关键词
        await bot_instance.db.remove_keyword(group_id, keyword_id)
        
        # 更新关键词列表显示
        await show_keyword_settings(bot_instance, query, group_id, 1)
        
    elif action == "list_page":
        # 显示关键词列表的特定页码
        page = int(parts[2])
        await show_keyword_settings(bot_instance, query, group_id, page)

@handle_callback_errors
async def handle_keyword_continue_callback(update: Update, context: CallbackContext):
    """处理继续添加关键词的回调"""
    query = update.callback_query
    await query.answer()
    
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 解析群组ID
    group_id = int(update.callback_query.data.split('_')[2])
    
    # 检查权限
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        await query.edit_message_text("❌ 无权限管理此群组")
        return
        
    # 显示匹配类型选择
    keyboard = [
        [InlineKeyboardButton("精确匹配", callback_data=f"keyword_type_exact_{group_id}"),
        InlineKeyboardButton("正则匹配", callback_data=f"keyword_type_regex_{group_id}")],
        [InlineKeyboardButton("取消", callback_data=f"settings_keywords_{group_id}")]
    ]
    await query.edit_message_text("请选择关键词匹配类型：", reply_markup=InlineKeyboardMarkup(keyboard))

@handle_callback_errors
async def handle_broadcast_callback(update: Update, context: CallbackContext):
    """处理轮播消息回调"""
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split('_')
    
    # 验证回调数据格式
    if len(parts) < 3:
        await query.edit_message_text("❌ 无效的操作")
        return
        
    action = parts[1]
    group_id = int(parts[-1])
    
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查用户权限
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        await query.edit_message_text("❌ 无权限管理此群组")
        return
        
    # 检查群组权限
    from db import GroupPermission
    if not await bot_instance.has_permission(group_id, GroupPermission.BROADCAST):
        await query.edit_message_text("❌ 此群组未启用轮播功能")
        return 
        
    # 处理不同的操作
    if action == "add":
        # 开始添加轮播消息
        await start_broadcast_form(update, context, group_id)
        return
    elif action == "detail":
        # 查看轮播消息详情
        if len(parts) < 4:
            await query.edit_message_text("❌ 无效的轮播消息ID")
            return
            
        broadcast_id = ObjectId(parts[2])
        broadcast = await bot_instance.db.db.broadcasts.find_one({'_id': broadcast_id, 'group_id': group_id}) 
        
        if not broadcast:
            await query.edit_message_text("❌ 未找到该轮播消息")
            return
            
        # 准备显示信息
        content = broadcast.get('content', '无内容')
        content_preview = str(content)[:50] + "..." if len(str(content)) > 50 else str(content)
        
        # 安全处理时间和间隔
        try:
            import config
            start_time = broadcast.get('start_time').astimezone(config.TIMEZONE).strftime('%Y-%m-%d %H:%M') if 'start_time' in broadcast else '未设置'
            end_time = broadcast.get('end_time').astimezone(config.TIMEZONE).strftime('%Y-%m-%d %H:%M') if 'end_time' in broadcast else '未设置'
        except Exception:
            start_time = '时间格式错误'
            end_time = '时间格式错误'
            
        from utils import format_duration
        interval = format_duration(broadcast.get('interval', 0))
        
        # 构建详情文本
        text = (
            f"📢 轮播消息详情：\n\n"
            f"🔹 类型：{broadcast.get('content_type', '未知类型')}\n"
            f"🔹 内容：{content_preview}\n"
            f"🔹 开始时间：{start_time}\n"
            f"🔹 结束时间：{end_time}\n"
            f"🔹 间隔：{interval}"
        )
        
        # 构建键盘
        keyboard = [
            [InlineKeyboardButton("❌ 删除此轮播消息", callback_data=f"broadcast_delete_{broadcast_id}_{group_id}")],
            [InlineKeyboardButton("🔙 返回列表", callback_data=f"settings_broadcast_{group_id}")]
        ]  
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif action == "delete":
        # 删除轮播消息
        if len(parts) < 4:
            await query.edit_message_text("❌ 无效的轮播消息ID")
            return         
            
        broadcast_id = ObjectId(parts[2])   
        
        # 检查轮播消息是否存在
        broadcast = await bot_instance.db.db.broadcasts.find_one({'_id': broadcast_id, 'group_id': group_id})
        if not broadcast:
            await query.edit_message_text("❌ 未找到该轮播消息")
            return       
            
        # 删除轮播消息
        await bot_instance.db.db.broadcasts.delete_one({'_id': broadcast_id, 'group_id': group_id})      
        
        # 更新轮播消息列表显示
        await show_broadcast_settings(bot_instance, query, group_id)

@handle_callback_errors
async def handle_stats_edit_callback(update: Update, context: CallbackContext):
    """处理统计设置编辑回调"""
    query = update.callback_query
    await query.answer()
    data = query.data
    logger.info(f"处理统计设置编辑回调: {data}")
    
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 解析回调数据
    prefix = "stats_edit_"
    if not data.startswith(prefix):
        logger.error(f"无效的回调前缀: {data}")
        await query.edit_message_text("❌ 无效的操作")
        return
        
    data_without_prefix = data[len(prefix):]
    parts = data_without_prefix.rsplit('_', 1)
    if len(parts) != 2:
        logger.error(f"无效的回调数据格式: {data}")
        await query.edit_message_text("❌ 无效的操作")
        return
        
    setting_type = parts[0]
    
    try:
        group_id = int(parts[1])
    except ValueError:
        logger.error(f"无效的群组ID: {parts[1]}")
        await query.edit_message_text("❌ 无效的群组ID")
        return
        
    logger.info(f"统计设置编辑 - 类型: {setting_type}, 群组ID: {group_id}")
    
    # 权限检查
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        logger.warning(f"用户 {update.effective_user.id} 无权限管理群组 {group_id}")
        await query.edit_message_text("❌ 无权限管理此群组")
        return
        
    from db import GroupPermission
    if not await bot_instance.has_permission(group_id, GroupPermission.STATS):
        logger.warning(f"群组 {group_id} 未启用统计功能")
        await query.edit_message_text("❌ 此群组未启用统计功能")
        return
        
    # 获取当前设置
    try:
        settings = await bot_instance.db.get_group_settings(group_id)
        logger.info(f"群组 {group_id} 当前设置: {settings}")
    except Exception as e:
        logger.error(f"获取群组 {group_id} 设置失败: {e}", exc_info=True)
        await query.edit_message_text("❌ 获取设置信息失败")
        return
        
    # 根据设置类型处理不同的设置
    if setting_type == "min_bytes":
        # 设置最小统计字节数
        logger.info("开始设置最小统计字节数")
        try:
            await query.edit_message_text("请输入最小统计字节数：\n• 低于此值的消息将不计入统计\n• 输入 0 表示统计所有消息\n\n发送 /cancel 取消")
            await bot_instance.settings_manager.start_setting(update.effective_user.id, 'stats_min_bytes', group_id)
            logger.info(f"为用户 {update.effective_user.id}, 群组 {group_id} 启动最小字节数设置过程")
        except Exception as e:
            logger.error(f"启动最小字节数设置失败: {e}", exc_info=True)
            await query.edit_message_text("❌ 设置失败，请重试")
            
    elif setting_type == "toggle_media":
        # 切换是否统计多媒体
        logger.info("处理切换统计多媒体设置")
        try:
            # 切换设置并更新
            current_value = settings.get('count_media', False)
            new_value = not current_value
            settings['count_media'] = new_value
            await bot_instance.db.update_group_settings(group_id, settings)
            logger.info(f"更新群组 {group_id} 的count_media设置为 {new_value}")
            
            # 显示更新后的统计设置
            await show_stats_settings(bot_instance, query, group_id, settings)
            
        except Exception as e:
            logger.error(f"更新统计多媒体设置失败: {e}", exc_info=True)
            await query.edit_message_text("❌ 更新设置失败，请重试")
            
    elif setting_type == "daily_rank":
        # 设置日排行显示数量
        logger.info("开始设置日排行显示数量")
        try:
            await query.edit_message_text("请输入日排行显示的用户数量：\n• 建议在 5-20 之间\n\n发送 /cancel 取消")
            await bot_instance.settings_manager.start_setting(update.effective_user.id, 'stats_daily_rank', group_id)
            logger.info(f"为用户 {update.effective_user.id}, 群组 {group_id} 启动日排行设置过程")
        except Exception as e:
            logger.error(f"启动日排行设置失败: {e}", exc_info=True)
            await query.edit_message_text("❌ 设置失败，请重试")
            
    elif setting_type == "monthly_rank":
        # 设置月排行显示数量
        logger.info("开始设置月排行显示数量")
        try:
            await query.edit_message_text("请输入月排行显示的用户数量：\n• 建议在 5-20 之间\n\n发送 /cancel 取消")
            await bot_instance.settings_manager.start_setting(update.effective_user.id, 'stats_monthly_rank', group_id)
            logger.info(f"为用户 {update.effective_user.id}, 群组 {group_id} 启动月排行设置过程")
        except Exception as e:
            logger.error(f"启动月排行设置失败: {e}", exc_info=True)
            await query.edit_message_text("❌ 设置失败，请重试")
            
    else:
        # 未知的设置类型
        logger.warning(f"未知的设置类型: {setting_type}")
        await query.edit_message_text(f"❌ 未知的设置类型：{setting_type}")

@handle_callback_errors
async def handle_auto_delete_callback(update: Update, context: CallbackContext):
    """处理自动删除设置回调"""
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split('_')
    
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 验证回调数据格式
    if len(parts) < 3:
        await query.edit_message_text("❌ 无效的操作")
        return
        
    action = parts[1]
    
    # 处理不同的操作
    if action in ["toggle", "timeout", "set", "custom"]:
        group_id = int(parts[-1])
        
        # 检查用户权限
        if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
            await query.edit_message_text("❌ 无权限管理此群组")
            return
            
        # 获取当前设置
        settings = await bot_instance.db.get_group_settings(group_id)
        
        if action == "toggle":
            # 切换自动删除开关状态
            settings['auto_delete'] = not settings.get('auto_delete', False)
            await bot_instance.db.update_group_settings(group_id, settings)
            
            # 显示自动删除设置
            await show_auto_delete_settings(bot_instance, query, group_id, settings)
            
        elif action == "timeout":
            # 显示超时时间选择界面
            current_timeout = settings.get('auto_delete_timeout', config.AUTO_DELETE_SETTINGS['default_timeout'])
            
            # 构建选择键盘
            keyboard = [
                [InlineKeyboardButton(f"{'✅' if current_timeout == 300 else ' '} 5分钟", callback_data=f"auto_delete_set_timeout_{group_id}_300")],
                [InlineKeyboardButton(f"{'✅' if current_timeout == 600 else ' '} 10分钟", callback_data=f"auto_delete_set_timeout_{group_id}_600")],
                [InlineKeyboardButton(f"{'✅' if current_timeout == 1800 else ' '} 30分钟", callback_data=f"auto_delete_set_timeout_{group_id}_1800")],
                [InlineKeyboardButton("自定义", callback_data=f"auto_delete_custom_timeout_{group_id}")],
                [InlineKeyboardButton("返回", callback_data=f"auto_delete_toggle_{group_id}")]
            ]
            
            await query.edit_message_text("请选择自动删除的超时时间：", reply_markup=InlineKeyboardMarkup(keyboard))
            
        elif action == "set":
            # 设置特定的超时时间
            if len(parts) < 4:
                await query.edit_message_text("❌ 无效的超时时间")
                return
                
            timeout = int(parts[3])
            settings['auto_delete_timeout'] = timeout
            await bot_instance.db.update_group_settings(group_id, settings)
            
            # 显示更新后的自动删除设置
            await show_auto_delete_settings(bot_instance, query, group_id, settings)
            
        elif action == "custom":
            # 启动自定义超时设置流程
            await bot_instance.settings_manager.start_setting(update.effective_user.id, 'auto_delete_timeout', group_id)
            await query.edit_message_text("请输入自定义超时时间（单位：秒，60-86400）：\n\n发送 /cancel 取消")

async def show_auto_delete_settings(bot_instance, query, group_id: int, settings: dict):
    """显示自动删除设置"""
    status = '开启' if settings.get('auto_delete', False) else '关闭'
    import config
    timeout = settings.get('auto_delete_timeout', config.AUTO_DELETE_SETTINGS['default_timeout'])
    
    from utils import format_duration
    keyboard = [
        [InlineKeyboardButton(f"自动删除: {status}", callback_data=f"auto_delete_toggle_{group_id}")],
        [InlineKeyboardButton(f"超时时间: {format_duration(timeout)}", callback_data=f"auto_delete_timeout_{group_id}")],
        [InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")]
    ]
    
    await query.edit_message_text(
        f"🗑️ 自动删除设置\n\n"
        f"当前状态: {'✅ 已开启' if settings.get('auto_delete', False) else '❌ 已关闭'}\n"
        f"超时时间: {format_duration(timeout)}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_feature_switches(bot_instance, query, group_id: int):
    """显示功能开关设置"""
    # 获取群组信息
    group = await bot_instance.db.get_group(group_id)
    if not group:
        await query.edit_message_text(f"❌ 找不到群组 {group_id} 的信息")
        return
        
    # 获取当前功能开关状态
    switches = group.get('feature_switches', {'keywords': True, 'stats': True, 'broadcast': True})
    
    # 构建功能开关菜单
    keyboard = []
    
    # 检查群组权限并显示相应的功能开关
    permissions = group.get('permissions', [])
    
    if 'stats' in permissions:
        status = '✅ 开启' if switches.get('stats', True) else '❌ 关闭'
        keyboard.append([InlineKeyboardButton(f"📊 统计功能: {status}", callback_data=f"switch_toggle_stats_{group_id}")])
        
    if 'broadcast' in permissions:
        status = '✅ 开启' if switches.get('broadcast', True) else '❌ 关闭'
        keyboard.append([InlineKeyboardButton(f"📢 轮播功能: {status}", callback_data=f"switch_toggle_broadcast_{group_id}")])
        
    if 'keywords' in permissions:
        status = '✅ 开启' if switches.get('keywords', True) else '❌ 关闭'
        keyboard.append([InlineKeyboardButton(f"🔑 关键词功能: {status}", callback_data=f"switch_toggle_keywords_{group_id}")])
        
    # 返回按钮
    keyboard.append([InlineKeyboardButton("🔙 返回设置菜单", callback_data=f"settings_select_{group_id}")])
    
    await query.edit_message_text(
        f"⚙️ 群组 {group_id} 功能开关设置\n\n"
        "点击相应按钮切换功能开关状态：",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@handle_callback_errors
async def handle_switch_toggle_callback(update: Update, context: CallbackContext):
    """处理功能开关切换回调"""
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split('_')
    
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 验证回调数据格式
    if len(parts) < 4:
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    feature = parts[2]
    group_id = int(parts[3])
    
    # 检查用户权限
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        await query.edit_message_text("❌ 你没有权限管理此群组")
        return
        
    try:
        # 获取当前群组信息
        group = await bot_instance.db.get_group(group_id)
        if not group:
            await query.edit_message_text(f"❌ 找不到群组 {group_id} 的信息")
            return
            
        # 获取当前功能开关状态
        switches = group.get('feature_switches', {'keywords': True, 'stats': True, 'broadcast': True})
        
        # 检查该功能是否在群组权限中
        if feature not in group.get('permissions', []):
            await query.edit_message_text(f"❌ 群组 {group_id} 没有 {feature} 权限")
            return
            
        # 切换功能开关状态
        current_status = switches.get(feature, True)
        new_status = not current_status
        
        # 更新数据库
        await bot_instance.db.db.groups.update_one(
            {'group_id': group_id},
            {'$set': {f'feature_switches.{feature}': new_status}}
        )
        logger.info(f"用户 {update.effective_user.id} 将群组 {group_id} 的 {feature} 功能设置为 {new_status}")
        
        # 重新显示功能开关设置菜单
        await show_feature_switches(bot_instance, query, group_id)
        
    except Exception as e:
        logger.error(f"切换功能开关失败: {e}", exc_info=True)
        await query.edit_message_text(f"❌ 切换功能开关失败，请重试")

async def handle_easy_keyword(update: Update, context: CallbackContext):
    """处理 /easykeyword 命令，启动简化的关键词添加流程"""
    if not update.effective_user or not update.effective_chat:
        return
        
    user_id = update.effective_user.id
    group_id = update.effective_chat.id if update.effective_chat.type != 'private' else None
    
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查权限
    if not await bot_instance.is_admin(user_id):
        await update.message.reply_text("❌ 该命令仅管理员可用")
        return
        
    # 如果是私聊，让用户选择要管理的群组
    if not group_id:
        manageable_groups = await bot_instance.db.get_manageable_groups(user_id)
        if not manageable_groups:
            await update.message.reply_text("❌ 你没有权限管理任何群组")
            return
            
        keyboard = []
        for group in manageable_groups:
            try:
                group_info = await context.bot.get_chat(group['group_id'])
                group_name = group_info.title or f"群组 {group['group_id']}"
            except Exception:
                group_name = f"群组 {group['group_id']}"
                
            keyboard.append([InlineKeyboardButton(
                group_name, 
                callback_data=f"kwform_select_group_{group['group_id']}"
            )])
            
        await update.message.reply_text(
            "请选择要添加关键词的群组：", 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
        
    # 检查群组权限
    from db import GroupPermission
    if not await bot_instance.has_permission(group_id, GroupPermission.KEYWORDS):
        await update.message.reply_text("❌ 此群组未启用关键词功能")
        return
        
    # 开始关键词添加流程
    await start_keyword_form(update, context, group_id)

async def start_keyword_form(update: Update, context: CallbackContext, group_id: int):
    """启动关键词表单流程"""
    # 获取bot实例
    bot_instance = context.application.bot_data.get('bot_instance')
    user_id = update.effective_user.id
    
    # 1. 清理旧的设置管理器状态
    active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
    logger.info(f"用户 {user_id} 的活动设置状态: {active_settings}")
    
    # 清理关键词相关的所有状态
    if 'keyword' in active_settings:
        await bot_instance.settings_manager.clear_setting_state(user_id, 'keyword')
        logger.info(f"已清理用户 {user_id} 的旧关键词设置状态")
    
    # 2. 清理context.user_data中的旧表单数据
    for key in list(context.user_data.keys()):
        if key.startswith('keyword_') or key == 'waiting_for':
            del context.user_data[key]
            logger.info(f"已清理用户数据中的键: {key}")
    
    # 3. 初始化新的表单数据
    context.user_data['keyword_form'] = {
        'group_id': group_id,
        'match_type': 'exact',  # 默认精确匹配
        'pattern': '',
        'response': '',
        'media': None,
        'buttons': []
    }
    logger.info(f"已为用户 {user_id} 初始化新的关键词表单数据")
    
    # 4. 显示匹配类型选择
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

async def handle_keyword_form_callback(update: Update, context: CallbackContext):
    """处理关键词表单回调"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    parts = data.split('_')
    
    if len(parts) < 3:
        await query.edit_message_text("❌ 无效的操作")
        return
    
    action = parts[2]
    
    form_data = context.user_data.get('keyword_form', {})
    
    # 处理不同的表单操作
    if action == "cancel":
        # 取消操作 - 全面清理状态
        user_id = update.effective_user.id
        bot_instance = context.application.bot_data.get('bot_instance')
        
        # 清理用户数据
        for key in list(context.user_data.keys()):
            if key.startswith('keyword_') or key == 'waiting_for':
                del context.user_data[key]
        
        # 清理设置管理器状态
        active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
        if 'keyword' in active_settings:
            await bot_instance.settings_manager.clear_setting_state(user_id, 'keyword')
        
        await query.edit_message_text("✅ 已取消关键词添加")
        
    elif action == "type":
        # 设置匹配类型
        match_type = parts[3]
        form_data['match_type'] = match_type
        context.user_data['keyword_form'] = form_data
        
        # 提示输入关键词
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"kwform_cancel")]]
        await query.edit_message_text(
            f"已选择: {'精确匹配' if match_type == 'exact' else '正则匹配'}\n\n"
            "请发送关键词内容: \n"
            f"({'支持正则表达式' if match_type == 'regex' else '精确匹配文字'})\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        # 设置等待输入状态
        context.user_data['waiting_for'] = 'keyword_pattern'
        
    elif action == "select_group":
        # 选择群组
        group_id = int(parts[3])
        # 启动添加流程
        await start_keyword_form(update, context, group_id)
        
    elif action == "pattern_received":
        # 已收到关键词模式，继续设置回复
        await show_response_options(update, context)
        
    elif action == "add_text":
        # 添加文本响应
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"kwform_cancel")]]
        await query.edit_message_text(
            "请发送关键词回复的文本内容:\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'keyword_response'
        
    elif action == "add_media":
        # 添加媒体响应
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"kwform_cancel")]]
        await query.edit_message_text(
            "请发送要添加的媒体:\n"
            "• 图片\n"
            "• 视频\n"
            "• 文件\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'keyword_media'
        
    elif action == "add_button":
        # 添加按钮
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"kwform_cancel")]]
        await query.edit_message_text(
            "请发送按钮信息，格式:\n\n"
            "按钮文字|https://网址\n\n"
            "每行一个按钮，例如:\n"
            "访问官网|https://example.com\n"
            "联系我们|https://t.me/username\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'keyword_buttons'
        
    elif action in ["response_received", "media_received", "buttons_received"]:
        # 已收到各类数据，显示表单选项
        await show_response_options(update, context)
        
    elif action == "preview":
        # 预览关键词响应
        await preview_keyword_response(update, context)
        
    elif action == "submit":
        # 提交关键词
        await submit_keyword_form(update, context)
        
    else:
        await query.edit_message_text("❌ 未知操作")

async def show_response_options(update: Update, context: CallbackContext):
    """显示关键词响应选项"""
    form_data = context.user_data.get('keyword_form', {})
    
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
    """预览关键词响应效果"""
    form_data = context.user_data.get('keyword_form', {})
    
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
            await show_response_options(update, context)
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
    """提交关键词表单"""
    form_data = context.user_data.get('keyword_form', {})
    
    # 验证必要字段
    pattern = form_data.get('pattern')
    if not pattern:
        await update.callback_query.answer("❌ 关键词不能为空")
        await show_response_options(update, context)
        return
    
    # 检查是否有回复内容
    has_content = bool(form_data.get('response') or form_data.get('media') or form_data.get('buttons'))
    if not has_content:
        await update.callback_query.answer("❌ 请至少添加一项回复内容")
        await show_response_options(update, context)
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

async def handle_easy_broadcast(update: Update, context: CallbackContext):
    """处理 /easybroadcast 命令，启动简化的广播添加流程"""
    if not update.effective_user or not update.effective_chat:
        return
        
    user_id = update.effective_user.id
    group_id = update.effective_chat.id if update.effective_chat.type != 'private' else None
    
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查权限
    if not await bot_instance.is_admin(user_id):
        await update.message.reply_text("❌ 该命令仅管理员可用")
        return
        
    # 如果是私聊，让用户选择要管理的群组
    if not group_id:
        manageable_groups = await bot_instance.db.get_manageable_groups(user_id)
        if not manageable_groups:
            await update.message.reply_text("❌ 你没有权限管理任何群组")
            return
            
        keyboard = []
        for group in manageable_groups:
            try:
                group_info = await context.bot.get_chat(group['group_id'])
                group_name = group_info.title or f"群组 {group['group_id']}"
            except Exception:
                group_name = f"群组 {group['group_id']}"
                
            keyboard.append([InlineKeyboardButton(
                group_name, 
                callback_data=f"bcform_select_group_{group['group_id']}"
            )])
            
        await update.message.reply_text(
            "请选择要添加轮播消息的群组：", 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
        
    # 检查群组权限
    from db import GroupPermission
    if not await bot_instance.has_permission(group_id, GroupPermission.BROADCAST):
        await update.message.reply_text("❌ 此群组未启用轮播消息功能")
        return
        
    # 开始广播添加流程
    await start_broadcast_form(update, context, group_id)

async def start_broadcast_form(update: Update, context: CallbackContext, group_id: int):
    """启动广播表单流程"""
    # 获取bot实例
    bot_instance = context.application.bot_data.get('bot_instance')
    user_id = update.effective_user.id
    
    # 清理旧的设置管理器状态
    active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
    logger.info(f"用户 {user_id} 的活动设置状态: {active_settings}")
    
    # 清理广播相关的所有状态
    if 'broadcast' in active_settings:
        await bot_instance.settings_manager.clear_setting_state(user_id, 'broadcast')
        logger.info(f"已清理用户 {user_id} 的旧广播设置状态")
    
    # 清理context.user_data中的旧表单数据
    for key in list(context.user_data.keys()):
        if key.startswith('broadcast_') or key == 'waiting_for':
            del context.user_data[key]
            logger.info(f"已清理用户数据中的键: {key}")
    
    # 初始化表单数据
    from datetime import datetime, timedelta
    import config
    
    # 设置默认值：开始时间为当前时间，结束时间为一周后，间隔为一小时
    now = datetime.now(config.TIMEZONE)
    end_time = now + timedelta(days=7)
    
    context.user_data['broadcast_form'] = {
        'group_id': group_id,
        'text': '',
        'media': None,
        'buttons': [],
        'start_time': now,
        'end_time': end_time,
        'interval': 3600  # 默认间隔1小时
    }
    logger.info(f"已为用户 {user_id} 初始化新的广播表单数据")
    
    # 显示广播表单菜单
    keyboard = [
        [InlineKeyboardButton("📝 添加内容", callback_data=f"bcform_add_content")],
        [InlineKeyboardButton("⏰ 设置时间", callback_data=f"bcform_set_time")],
        [InlineKeyboardButton("🔄 设置间隔", callback_data=f"bcform_set_interval")],
        [InlineKeyboardButton("👁️ 预览效果", callback_data=f"bcform_preview")],
        [InlineKeyboardButton("✅ 提交", callback_data=f"bcform_submit")],
        [InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]
    ]
    
    # 根据情境使用不同的发送方式
    message_text = (
        "📢 轮播消息添加向导\n\n"
        "轮播消息会在设定的时间范围内按照指定的间隔自动发送。\n\n"
        "请选择要设置的项目："
    )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def handle_broadcast_form_callback(update: Update, context: CallbackContext):
    """处理广播表单回调"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    parts = data.split('_')
    
    if len(parts) < 3:
        await query.edit_message_text("❌ 无效的操作")
        return
    
    action = parts[2]
    
    form_data = context.user_data.get('broadcast_form', {})
    
    # 处理不同的表单操作
    if action == "cancel":
        # 取消操作
        if 'broadcast_form' in context.user_data:
            del context.user_data['broadcast_form']
        await query.edit_message_text("✅ 已取消轮播消息添加")
        
    elif action == "select_group":
        # 选择群组
        group_id = int(parts[3])
        # 启动添加流程
        await start_broadcast_form(update, context, group_id)
        
    elif action == "add_content":
        # 显示内容添加选项
        await show_content_options(update, context)
        
    elif action == "add_text":
        # 添加文本内容
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        await query.edit_message_text(
            "请发送轮播消息的文本内容:\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'broadcast_text'
        
    elif action == "add_media":
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
        # 添加按钮
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        await query.edit_message_text(
            "请发送按钮信息，格式:\n\n"
            "按钮文字|https://网址\n\n"
            "每行一个按钮，例如:\n"
            "访问官网|https://example.com\n"
            "联系我们|https://t.me/username\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        context.user_data['waiting_for'] = 'broadcast_buttons'
        
    elif action in ["text_received", "media_received", "buttons_received"]:
        # 已收到各类数据，返回内容选项
        await show_content_options(update, context)
        
    elif action == "content_done":
        # 内容设置完成，返回主菜单
        await show_broadcast_summary(update, context)
        
    elif action == "set_time":
        # 显示时间设置选项
        await show_time_options(update, context)
        
    elif action == "set_start_time":
        # 设置开始时间
        await show_time_preset_options(update, context, "start")
        
    elif action == "set_end_time":
        # 设置结束时间
        await show_time_preset_options(update, context, "end")
        
    elif action == "time_preset":
        # 处理预设时间选择
        time_type = parts[3]  # start 或 end
        preset = parts[4]
        
        from datetime import datetime, timedelta
        import config
        
        now = datetime.now(config.TIMEZONE)
        
        # 根据预设计算时间
        if preset == "now":
            selected_time = now
        elif preset == "today":
            # 今天结束
            selected_time = now.replace(hour=23, minute=59, second=59)
        elif preset == "tomorrow":
            # 明天结束
            selected_time = (now + timedelta(days=1)).replace(hour=23, minute=59, second=59)
        elif preset == "week":
            # 一周后
            selected_time = now + timedelta(days=7)
        elif preset == "month":
            # 一个月后
            selected_time = now + timedelta(days=30)
        else:
            await query.answer("无效的时间预设")
            return
        
        # 更新表单数据
        if time_type == "start":
            form_data['start_time'] = selected_time
        else:
            form_data['end_time'] = selected_time
            
        context.user_data['broadcast_form'] = form_data
        
        # 返回时间设置菜单
        await show_time_options(update, context)
        
    elif action == "time_custom":
        # 自定义时间输入
        time_type = parts[3]  # start 或 end
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        
        await query.edit_message_text(
            f"请输入{'开始' if time_type == 'start' else '结束'}时间\n\n"
            "格式: YYYY-MM-DD HH:MM\n"
            "例如: 2025-03-15 14:30\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        context.user_data['waiting_for'] = f'broadcast_{time_type}_time'
        
    elif action == "time_done":
        # 时间设置完成
        await show_broadcast_summary(update, context)
        
    elif action == "set_interval":
        # 设置发送间隔
        await show_interval_options(update, context)
        
    elif action == "interval_preset":
        # 处理预设间隔
        preset = parts[3]
        
        # 根据预设设置间隔（秒）
        if preset == "hourly":
            interval = 3600  # 1小时
        elif preset == "daily":
            interval = 86400  # 24小时
        elif preset == "twice_daily":
            interval = 43200  # 12小时
        else:
            try:
                interval = int(preset)
            except ValueError:
                await query.answer("无效的间隔预设")
                return
                
        # 验证间隔是否符合最小要求
        import config
        min_interval = config.BROADCAST_SETTINGS['min_interval']
        if interval < min_interval:
            await query.answer(f"间隔不能小于 {min_interval} 秒")
            return
            
        # 更新表单数据
        form_data['interval'] = interval
        context.user_data['broadcast_form'] = form_data
        
        # 返回主菜单
        await show_broadcast_summary(update, context)
        
    elif action == "interval_custom":
        # 自定义间隔输入
        keyboard = [[InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]]
        
        import config
        min_interval = config.BROADCAST_SETTINGS['min_interval']
        
        await query.edit_message_text(
            "请输入轮播消息发送间隔（秒）\n\n"
            f"最小间隔: {min_interval} 秒\n"
            "常用间隔:\n"
            "- 1小时: 3600秒\n"
            "- 6小时: 21600秒\n"
            "- 12小时: 43200秒\n"
            "- 24小时: 86400秒\n\n"
            "发送完后请点击下方出现的「继续」按钮",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        context.user_data['waiting_for'] = 'broadcast_interval'
        
    elif action == "preview":
        # 预览广播效果
        await preview_broadcast(update, context)
        
    elif action == "submit":
        # 提交广播
        await submit_broadcast_form(update, context)
        
    else:
        await query.edit_message_text("❌ 未知操作")

async def show_content_options(update: Update, context: CallbackContext):
    """显示广播内容选项"""
    form_data = context.user_data.get('broadcast_form', {})
    
    # 构建当前状态摘要
    summary = "📢 轮播内容设置\n\n"
    summary += f"• 文本: {'✅ 已设置' if form_data.get('text') else '❌ 未设置'}\n"
    summary += f"• 媒体: {'✅ 已设置' if form_data.get('media') else '❌ 未设置'}\n"
    summary += f"• 按钮: {len(form_data.get('buttons', []))} 个\n\n"
    summary += "请选择要添加或修改的内容:"
    
    # 构建操作按钮
    keyboard = [
        [InlineKeyboardButton("📝 添加/修改文本", callback_data=f"bcform_add_text")],
        [InlineKeyboardButton("🖼️ 添加/修改媒体", callback_data=f"bcform_add_media")],
        [InlineKeyboardButton("🔘 添加/修改按钮", callback_data=f"bcform_add_button")],
        [InlineKeyboardButton("✅ 完成", callback_data=f"bcform_content_done")],
        [InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]
    ]
    
    # 检查是否至少有一项内容
    has_content = bool(form_data.get('text') or form_data.get('media') or form_data.get('buttons'))
    if not has_content:
        summary += "\n\n⚠️ 请至少添加一项内容(文本/媒体/按钮)"
    
    # 显示内容选项
    await update.callback_query.edit_message_text(
        summary,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_time_options(update: Update, context: CallbackContext):
    """显示时间设置选项"""
    form_data = context.user_data.get('broadcast_form', {})
    
    # 获取当前设置的时间
    from utils import format_datetime
    start_time = format_datetime(form_data.get('start_time'))
    end_time = format_datetime(form_data.get('end_time'))
    
    # 构建当前状态摘要
    summary = "⏰ 轮播时间设置\n\n"
    summary += f"• 开始时间: {start_time}\n"
    summary += f"• 结束时间: {end_time}\n\n"
    
    # 检查时间设置是否有效
    is_valid = form_data.get('start_time') < form_data.get('end_time')
    if not is_valid:
        summary += "⚠️ 结束时间必须晚于开始时间\n\n"
    
    summary += "请选择要设置的项目:"
    
    # 构建操作按钮
    keyboard = [
        [InlineKeyboardButton("⏱️ 设置开始时间", callback_data=f"bcform_set_start_time")],
        [InlineKeyboardButton("⏱️ 设置结束时间", callback_data=f"bcform_set_end_time")],
        [InlineKeyboardButton("✅ 完成", callback_data=f"bcform_time_done")],
        [InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]
    ]
    
    # 显示时间选项
    await update.callback_query.edit_message_text(
        summary,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_time_preset_options(update: Update, context: CallbackContext, time_type):
    """显示时间预设选项"""
    # 构建预设选项
    keyboard = []
    
    if time_type == "start":
        keyboard.append([InlineKeyboardButton("⏱️ 立即开始", callback_data=f"bcform_time_preset_start_now")])
    else:
        keyboard.extend([
            [InlineKeyboardButton("⏱️ 今天结束", callback_data=f"bcform_time_preset_end_today")],
            [InlineKeyboardButton("⏱️ 明天结束", callback_data=f"bcform_time_preset_end_tomorrow")],
            [InlineKeyboardButton("⏱️ 一周后结束", callback_data=f"bcform_time_preset_end_week")],
            [InlineKeyboardButton("⏱️ 一个月后结束", callback_data=f"bcform_time_preset_end_month")]
        ])
    
    # 添加自定义选项
    keyboard.append([InlineKeyboardButton("📝 自定义时间", callback_data=f"bcform_time_custom_{time_type}")])
    keyboard.append([InlineKeyboardButton("🔙 返回", callback_data=f"bcform_set_time")])
    
    # 显示预设选项
    await update.callback_query.edit_message_text(
        f"请选择{'开始' if time_type == 'start' else '结束'}时间:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_interval_options(update: Update, context: CallbackContext):
    """显示间隔设置选项"""
    form_data = context.user_data.get('broadcast_form', {})
    current_interval = form_data.get('interval', 3600)
    
    from utils import format_duration
    interval_display = format_duration(current_interval)
    
    # 构建当前状态摘要
    summary = "🔄 轮播间隔设置\n\n"
    summary += f"当前设置: {interval_display}\n\n"
    summary += "请选择轮播消息的发送间隔:"
    
    # 构建预设选项
    keyboard = [
        [InlineKeyboardButton("⏱️ 每小时", callback_data=f"bcform_interval_preset_hourly")],
        [InlineKeyboardButton("⏱️ 每12小时", callback_data=f"bcform_interval_preset_twice_daily")],
        [InlineKeyboardButton("⏱️ 每天", callback_data=f"bcform_interval_preset_daily")],
        [InlineKeyboardButton("📝 自定义间隔", callback_data=f"bcform_interval_custom")],
        [InlineKeyboardButton("🔙 返回", callback_data=f"bcform_content_done")]
    ]
    
    # 显示间隔选项
    await update.callback_query.edit_message_text(
        summary,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_broadcast_summary(update: Update, context: CallbackContext):
    """显示广播设置摘要"""
    form_data = context.user_data.get('broadcast_form', {})
    
    # 获取当前设置
    from utils import format_datetime, format_duration
    start_time = format_datetime(form_data.get('start_time'))
    end_time = format_datetime(form_data.get('end_time'))
    interval = format_duration(form_data.get('interval', 3600))
    
    # 构建摘要信息
    summary = "📢 轮播消息摘要\n\n"
    summary += f"• 文本: {'✅ 已设置' if form_data.get('text') else '❌ 未设置'}\n"
    summary += f"• 媒体: {'✅ 已设置' if form_data.get('media') else '❌ 未设置'}\n"
    summary += f"• 按钮: {len(form_data.get('buttons', []))} 个\n"
    summary += f"• 开始时间: {start_time}\n"
    summary += f"• 结束时间: {end_time}\n"
    summary += f"• 发送间隔: {interval}\n\n"
    
    # 检查设置是否有效
    is_valid_time = form_data.get('start_time') < form_data.get('end_time')
    has_content = bool(form_data.get('text') or form_data.get('media') or form_data.get('buttons'))
    
    if not is_valid_time:
        summary += "⚠️ 结束时间必须晚于开始时间\n"
    if not has_content:
        summary += "⚠️ 请至少添加一项内容(文本/媒体/按钮)\n"
    
    summary += "\n请选择操作:"
    
    # 构建操作按钮
    keyboard = [
        [InlineKeyboardButton("📝 编辑内容", callback_data=f"bcform_add_content")],
        [InlineKeyboardButton("⏰ 编辑时间", callback_data=f"bcform_set_time")],
        [InlineKeyboardButton("🔄 编辑间隔", callback_data=f"bcform_set_interval")],
        [InlineKeyboardButton("👁️ 预览效果", callback_data=f"bcform_preview")],
        [InlineKeyboardButton("✅ 提交", callback_data=f"bcform_submit")],
        [InlineKeyboardButton("❌ 取消", callback_data=f"bcform_cancel")]
    ]
    
    # 显示摘要
    await update.callback_query.edit_message_text(
        summary,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def preview_broadcast(update: Update, context: CallbackContext):
    """预览广播效果"""
    form_data = context.user_data.get('broadcast_form', {})
    
    # 获取回复数据
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
                text or "轮播消息",
                reply_markup=reply_markup
            )
        else:
            await update.callback_query.answer("没有预览内容")
            await show_broadcast_summary(update, context)
            return
    except Exception as e:
        logger.error(f"预览生成错误: {e}")
        await update.callback_query.answer(f"预览生成失败: {str(e)}")
    
    # 返回操作菜单
    keyboard = [
        [InlineKeyboardButton("🔙 返回", callback_data=f"bcform_content_done")]
    ]
    await update.callback_query.edit_message_text(
        "👆 上方为轮播消息预览\n\n点击「返回」继续编辑",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def submit_broadcast_form(update: Update, context: CallbackContext):
    """提交广播表单"""
    form_data = context.user_data.get('broadcast_form', {})
    
    # 检查是否有内容
    has_content = bool(form_data.get('text') or form_data.get('media') or form_data.get('buttons'))
    if not has_content:
        await update.callback_query.answer("❌ 请至少添加一项内容")
        await show_broadcast_summary(update, context)
        return
    
    # 检查时间设置是否有效
    is_valid_time = form_data.get('start_time') < form_data.get('end_time')
    if not is_valid_time:
        await update.callback_query.answer("❌ 结束时间必须晚于开始时间")
        await show_broadcast_summary(update, context)
        return
    
    # 构建广播数据
    broadcast_data = {
        'group_id': form_data['group_id'],
        'text': form_data.get('text', ''),
        'media': form_data.get('media'),
        'buttons': form_data.get('buttons', []),
        'start_time': form_data['start_time'],
        'end_time': form_data['end_time'],
        'interval': form_data['interval']
    }
    
    # 添加广播
    bot_instance = context.application.bot_data.get('bot_instance')
    try:
        await bot_instance.broadcast_manager.add_broadcast(broadcast_data)
        
        # 清理表单数据
        if 'broadcast_form' in context.user_data:
            del context.user_data['broadcast_form']
        if 'waiting_for' in context.user_data:
            del context.user_data['waiting_for']
        
        # 显示成功消息
        from utils import format_datetime, format_duration
        await update.callback_query.edit_message_text(
            "✅ 轮播消息添加成功！\n\n"
            f"开始时间: {format_datetime(broadcast_data['start_time'])}\n"
            f"结束时间: {format_datetime(broadcast_data['end_time'])}\n"
            f"发送间隔: {format_duration(broadcast_data['interval'])}"
        )
    except Exception as e:
        logger.error(f"添加轮播消息错误: {e}")
        await update.callback_query.answer("❌ 添加轮播消息失败")
        await update.callback_query.edit_message_text(
            f"❌ 添加轮播消息失败: {str(e)}\n\n"
            "请重试或联系管理员"
        )



# 注册所有处理函数
def register_all_handlers(application):
    """注册所有处理函数"""
    # 注册命令处理器
    application.add_handler(CommandHandler("start", handle_start))
    application.add_handler(CommandHandler("tongji", handle_rank_command))
    application.add_handler(CommandHandler("tongji30", handle_rank_command))
    application.add_handler(CommandHandler("settings", handle_settings))
    application.add_handler(CommandHandler("admingroups", handle_admin_groups))
    application.add_handler(CommandHandler("cancel", handle_cancel))
    application.add_handler(CommandHandler("addsuperadmin", handle_add_superadmin))
    application.add_handler(CommandHandler("delsuperadmin", handle_del_superadmin))
    application.add_handler(CommandHandler("addadmin", handle_add_admin))
    application.add_handler(CommandHandler("deladmin", handle_del_admin))
    application.add_handler(CommandHandler("authgroup", handle_auth_group))
    application.add_handler(CommandHandler("deauthgroup", handle_deauth_group))
    application.add_handler(CommandHandler("checkconfig", handle_check_config))

    # 注册回调查询处理器
    application.add_handler(CallbackQueryHandler(handle_settings_callback, pattern=r'^settings_'))
    application.add_handler(CallbackQueryHandler(handle_keyword_callback, pattern=r'^keyword_'))
    application.add_handler(CallbackQueryHandler(handle_broadcast_callback, pattern=r'^broadcast_'))
    application.add_handler(CallbackQueryHandler(handle_keyword_continue_callback, pattern=r'^keyword_continue_'))
    application.add_handler(CallbackQueryHandler(handle_stats_edit_callback, pattern=r'^stats_edit_'))
    application.add_handler(CallbackQueryHandler(handle_auto_delete_callback, pattern=r'^auto_delete_'))
    application.add_handler(CallbackQueryHandler(handle_switch_toggle_callback, pattern=r'^switch_toggle_'))
    
    # 添加简化的关键词和广播处理器
    application.add_handler(CommandHandler("easykeyword", handle_easy_keyword))
    application.add_handler(CommandHandler("easybroadcast", handle_easy_broadcast))
    application.add_handler(CallbackQueryHandler(handle_keyword_form_callback, pattern=r'^kwform_'))
    application.add_handler(CallbackQueryHandler(handle_broadcast_form_callback, pattern=r'^bcform_'))
    
    # 注册消息处理器
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))

    # 错误处理器会由 ErrorHandlingMiddleware 处理import logging

    


