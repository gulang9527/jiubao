"""
命令处理函数，处理各种命令的逻辑
"""
import logging
import asyncio
from typing import Optional, List, Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

from utils.decorators import (
    check_command_usage, require_admin, require_superadmin, handle_callback_errors
)
from utils.message_utils import validate_delete_timeout
from db.models import UserRole, GroupPermission

logger = logging.getLogger(__name__)

#######################################
# 基础命令处理函数
#######################################

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
        "我是啤酒群管理机器人，主要功能包括：\n"
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
            "🔑 /easykeyword - 添加关键词\n"
            "📢 /easybroadcast - 添加轮播消息\n"
        )
        
    # 添加超级管理员命令
    if is_superadmin:
        welcome_text += (
            "\n超级管理员命令：\n"
            "➕ /addsuperadmin <用户ID> - 添加超级管理员\n"
            "➖ /delsuperadmin <用户ID> - 删除超级管理员\n"
            "👤 /addadmin <用户ID> - 添加管理员\n"
            "🚫 /deladmin <用户ID> - 删除管理员\n"
            "✅ /authgroup <群组ID> - 授权群组\n"
            "❌ /deauthgroup <群组ID> - 取消群组授权\n"
            "🔍 /checkconfig - 检查当前配置\n"
        )
        
    welcome_text += "\n如需帮助，请联系管理员。"
    
    # 检查是否在群组中
    if update.effective_chat.type in ['group', 'supergroup']:
        try:
            # 尝试向用户发送私聊消息
            await context.bot.send_message(
                chat_id=user_id,
                text=welcome_text
            )
            
            # 在群组中回复一个简短的提示
            await update.message.reply_text(
                f"@{update.effective_user.username or update.effective_user.first_name}，我已经向你发送了帮助信息，请查看私聊。"
            )
        except Exception as e:
            logger.error(f"无法向用户 {user_id} 发送私聊消息: {e}")
            # 如果用户没有先私聊机器人，则在群组中提示
            await update.message.reply_text(
                f"@{update.effective_user.username or update.effective_user.first_name}，请先私聊我一次(@qdjiubao_bot)，这样我才能向你发送帮助信息。"
            )
    else:
        # 在私聊中正常发送欢迎消息
        await update.message.reply_text(welcome_text)

@check_command_usage
async def handle_settings(update: Update, context: CallbackContext):
    """处理/settings命令 - 显示群组选择菜单"""
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
async def handle_rank_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /rank 命令，显示群组消息排行榜"""
    # 只在群组中响应
    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("此命令只能在群组中使用。")
        return

    # 获取群组信息
    chat = update.effective_chat
    group_id = chat.id
    
    # 设置排行榜标题
    title = "📊 消息数量排行榜 📊"
    
    # 从数据库获取排名前15的用户数据（按消息数量降序排序）
    page = 1
    stats = await get_message_stats_from_db(group_id, limit=50)
    
    # 如果没有数据，显示提示信息
    if not stats:
        await update.message.reply_text("暂无排行数据。")
        return
    
    # 计算总页数（每页15条记录）
    total_pages = (len(stats) + 14) // 15
    
    # 只显示第一页的15条记录
    stats = stats[:15]
    
    # 构建分页按钮
    keyboard = []
    if total_pages > 1:
        buttons = []
        if page < total_pages:
            buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"rank_next_{page}"))
        keyboard.append(buttons)

    # 构建HTML格式的排行文本
    text = f"""
<html>
<head>
<style>
.ranking-container {{
  width: 100%;
}}
.title {{
  text-align: center;
  font-size: 16px;
  font-weight: bold;
  margin-bottom: 10px;
}}
.user-row {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 3px 0;
}}
.user-info {{
  display: flex;
  align-items: center;
  max-width: 70%;
}}
.rank {{
  width: 25px;
  text-align: right;
  margin-right: 8px;
}}
.username {{
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: calc(100% - 35px);
}}
.message-count {{
  font-weight: bold;
  min-width: 80px;
  text-align: right;
}}
.pagination-info {{
  text-align: center;
  margin-top: 10px;
  color: #666;
}}
</style>
</head>
<body>
<div class="ranking-container">
  <div class="title">{title}</div>
"""

    # 添加用户排行
    for i, stat in enumerate(stats, start=(page-1)*15+1):
        try:
            user = await context.bot.get_chat_member(group_id, stat['_id'])
            display_name = user.user.full_name
            # 处理HTML特殊字符
            import html
            display_name = html.escape(display_name)
            user_mention = f'<a href="tg://user?id={stat["_id"]}">{display_name}</a>'
        except Exception:
            user_mention = f'用户{stat["_id"]}'
        
        # 添加奖牌图标（前三名）
        rank_prefix = ""
        if page == 1:
            if i == 1:
                rank_prefix = "🥇 "
            elif i == 2:
                rank_prefix = "🥈 "
            elif i == 3:
                rank_prefix = "🥉 "
        
        # 构建用户行
        text += f"""
  <div class="user-row">
    <div class="user-info">
      <div class="rank">{rank_prefix}{i}.</div>
      <div class="username">{user_mention}</div>
    </div>
    <div class="message-count">消息数: {stat['total_messages']}</div>
  </div>
"""

    # 添加分页信息
    text += f"""
  <div class="pagination-info">第 {page}/{total_pages} 页</div>
</div>
</body>
</html>
"""

    # 发送排行消息到群组
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    msg = await update.message.reply_text(
        text=text, 
        parse_mode="HTML", 
        reply_markup=reply_markup
    )
    
    # 如果启用了自动删除，设置消息过期时间
    await set_message_expiry(
        context=context,
        chat_id=group_id,
        message_id=msg.message_id,
        feature="rank_command"
    )

@handle_callback_errors
async def handle_rank_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理排行榜分页回调"""
    query = update.callback_query
    await query.answer()

    # 获取按钮数据
    data = query.data.split("_")
    action = data[1]
    current_page = int(data[2])
    
    if action == "prev":
        page = max(1, current_page - 1)
    elif action == "next":
        page = current_page + 1
    else:
        page = current_page

    # 获取群组信息
    chat = update.effective_chat
    group_id = chat.id
    
    # 获取排行数据
    title = "📊 消息数量排行榜 📊"
    
    # 从数据库获取排名前50的用户数据（按消息数量降序排序）
    stats = await get_message_stats_from_db(group_id, limit=50, skip=(page-1)*15)
    
    # 如果没有数据，显示提示信息
    if not stats:
        await query.edit_message_text(
            "暂无排行数据。", 
            reply_markup=None
        )
        return

    # 计算总页数（每页15条记录）
    total_pages = (len(stats) + 14) // 15
    
    # 如果请求的页码超出范围，显示最后一页
    if page > total_pages:
        page = total_pages
        stats = await get_message_stats_from_db(group_id, limit=15, skip=(page-1)*15)
    
    # 只显示当前页的15条记录
    stats = stats[:15]

    # 构建分页按钮
    keyboard = []
    if total_pages > 1:
        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"rank_prev_{page}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"rank_next_{page}"))
        keyboard.append(buttons)

    # 构建HTML格式的排行文本
    text = f"""
<html>
<head>
<style>
.ranking-container {{
  width: 100%;
}}
.title {{
  text-align: center;
  font-size: 16px;
  font-weight: bold;
  margin-bottom: 10px;
}}
.user-row {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 3px 0;
}}
.user-info {{
  display: flex;
  align-items: center;
  max-width: 70%;
}}
.rank {{
  width: 25px;
  text-align: right;
  margin-right: 8px;
}}
.username {{
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: calc(100% - 35px);
}}
.message-count {{
  font-weight: bold;
  min-width: 80px;
  text-align: right;
}}
.pagination-info {{
  text-align: center;
  margin-top: 10px;
  color: #666;
}}
</style>
</head>
<body>
<div class="ranking-container">
  <div class="title">{title}</div>
"""

    # 添加用户排行
    for i, stat in enumerate(stats, start=(page-1)*15+1):
        try:
            user = await context.bot.get_chat_member(group_id, stat['_id'])
            display_name = user.user.full_name
            # 处理HTML特殊字符
            import html
            display_name = html.escape(display_name)
            user_mention = f'<a href="tg://user?id={stat["_id"]}">{display_name}</a>'
        except Exception:
            user_mention = f'用户{stat["_id"]}'
        
        # 添加奖牌图标（前三名）
        rank_prefix = ""
        if page == 1:
            if i == 1:
                rank_prefix = "🥇 "
            elif i == 2:
                rank_prefix = "🥈 "
            elif i == 3:
                rank_prefix = "🥉 "
        
        # 构建用户行
        text += f"""
  <div class="user-row">
    <div class="user-info">
      <div class="rank">{rank_prefix}{i}.</div>
      <div class="username">{user_mention}</div>
    </div>
    <div class="message-count">消息数: {stat['total_messages']}</div>
  </div>
"""

    # 添加分页信息
    text += f"""
  <div class="pagination-info">第 {page}/{total_pages} 页</div>
</div>
</body>
</html>
"""

    # 更新消息内容
    await query.edit_message_text(
        text=text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
    )
        
@check_command_usage
async def handle_admin_groups(update: Update, context: CallbackContext):
    """处理/admingroups命令 - 显示可管理的群组列表"""
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
async def handle_cancel(update: Update, context: CallbackContext):
    """处理/cancel命令 - 取消当前进行的操作"""
    bot_instance = context.application.bot_data.get('bot_instance')
    user_id = update.effective_user.id
    
    # 清理表单数据
    for key in list(context.user_data.keys()):
        if key.startswith(('keyword_', 'broadcast_')) or key == 'waiting_for':
            del context.user_data[key]
            
    # 获取活动的设置
    active_settings = await bot_instance.settings_manager.get_active_settings(user_id)
    if not active_settings:
        await update.message.reply_text("❌ 当前没有正在进行的设置操作")
        return
        
    # 清除所有设置状态
    for setting_type in active_settings:
        await bot_instance.settings_manager.clear_setting_state(user_id, setting_type)
        
    await update.message.reply_text("✅ 已取消所有正在进行的设置操作")

#######################################
# 管理员命令处理函数
#######################################

@check_command_usage
async def handle_easy_keyword(update: Update, context: CallbackContext):
    """处理 /easykeyword 命令，启动简化的关键词添加流程"""
    logger.info(f"进入 handle_easy_keyword 函数，处理用户 {update.effective_user.id if update.effective_user else 'unknown'} 的请求")
    if not update.effective_user or not update.effective_chat:
        logger.warning("无法获取用户或聊天信息")
        return
        
    user_id = update.effective_user.id
    group_id = update.effective_chat.id if update.effective_chat.type != 'private' else None
    logger.info(f"用户ID: {user_id}, 群组ID: {group_id}")
    
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
    if not await bot_instance.has_permission(group_id, GroupPermission.KEYWORDS):
        await update.message.reply_text("❌ 此群组未启用关键词功能")
        return
        
    # 开始关键词添加流程
    from handlers.keyword_handlers import start_keyword_form
    await start_keyword_form(update, context, group_id)

@check_command_usage
async def handle_easy_broadcast(update: Update, context: CallbackContext):
    """处理 /easybroadcast 命令，启动简化的轮播消息添加流程"""
    logger.info(f"进入 handle_easy_broadcast 函数，处理用户 {update.effective_user.id if update.effective_user else 'unknown'} 的请求")
    if not update.effective_user or not update.effective_chat:
        logger.warning("无法获取用户或聊天信息")
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
    if not await bot_instance.has_permission(group_id, GroupPermission.BROADCAST):
        await update.message.reply_text("❌ 此群组未启用轮播消息功能")
        return
        
    # 开始轮播消息添加流程
    from handlers.broadcast_handlers import start_broadcast_form
    await start_broadcast_form(update, context, group_id)

#######################################
# 超级管理员命令处理函数
#######################################

@check_command_usage
@require_superadmin
async def handle_add_admin(update: Update, context: CallbackContext):
    """处理/addadmin命令 - 添加管理员"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：/addadmin <用户ID>")
        return
        
    try:
        # 解析用户ID并添加管理员
        user_id = int(context.args[0])
        
        # 检查用户是否已经是管理员
        from db.models import UserRole
        user = await bot_instance.db.get_user(user_id)
        if user and user.get('role') in [UserRole.ADMIN.value, UserRole.SUPERADMIN.value]:
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
    """处理/deladmin命令 - 删除管理员"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：/deladmin <用户ID>")
        return
        
    try:
        # 解析用户ID
        user_id = int(context.args[0])
        
        # 检查用户
        from db.models import UserRole
        user = await bot_instance.db.get_user(user_id)
        if not user:
            await update.message.reply_text("❌ 该用户不是管理员")
            return
            
        # 不能删除超级管理员
        if user.get('role') == UserRole.ADMIN.value:
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
    """处理/addsuperadmin命令 - 添加超级管理员"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 检查参数
    if not context.args:
        await update.message.reply_text("❌ 请使用正确的格式：/addsuperadmin <用户ID>")
        return
        
    try:
        # 解析用户ID
        user_id = int(context.args[0])
        
        # 检查用户是否已经是超级管理员
        from db.models import UserRole
        logger.info(f"SUPERADMIN值: {UserRole.SUPERADMIN.value}")
        
        user = await bot_instance.db.get_user(user_id)
        # 安全地检查role字段
        if user and user.get('role') == UserRole.SUPERADMIN.value:
            await update.message.reply_text("❌ 该用户已经是超级管理员")
            return
            
        # 添加超级管理员
        user_data = {'user_id': user_id, 'role': UserRole.SUPERADMIN.value}
        logger.info(f"添加超级管理员数据: {user_data}")
        await bot_instance.db.add_user(user_data)
        await update.message.reply_text(f"✅ 已将用户 {user_id} 设置为超级管理员")
        
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
    except Exception as e:
        logger.error(f"添加超级管理员错误: {e}", exc_info=True)
        await update.message.reply_text("❌ 添加超级管理员时出错")

@check_command_usage
@require_superadmin
async def handle_del_superadmin(update: Update, context: CallbackContext):
    """处理/delsuperadmin命令 - 删除超级管理员"""
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
        from db.models import UserRole
        user = await bot_instance.db.get_user(user_id)
        if not user or user.get('role') != UserRole.SUPERADMIN.value:
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
    """处理/checkconfig命令 - 检查当前配置"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 获取配置信息
    from db.models import UserRole
    superadmins = await bot_instance.db.get_users_by_role(UserRole.SUPERADMIN.value)
    superadmin_ids = [user['user_id'] for user in superadmins]
    groups = await bot_instance.db.find_all_groups()
    
    # 构建配置文本
    config_text = "🔧 当前配置信息：\n\n👥 超级管理员：\n" + "\n".join(f"• {admin_id}" for admin_id in superadmin_ids)
    config_text += "\n\n📋 已授权群组：\n" + "\n".join(f"• 群组 {group['group_id']}\n  权限: {', '.join(group.get('permissions', []))}" for group in groups)
    
    await update.message.reply_text(config_text)

@check_command_usage
@require_superadmin
async def handle_auth_group(update: Update, context: CallbackContext):
    """处理/authgroup命令 - 授权群组"""
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
        from db.models import GroupPermission
        import config
        all_permissions = [perm.value for perm in GroupPermission]
        await bot_instance.db.add_group({
            'group_id': group_id,
            'permissions': all_permissions,
            'settings': {'auto_delete': False, 'auto_delete_timeout': config.AUTO_DELETE_SETTINGS['default_timeout']},
            'feature_switches': {'keywords': True, 'stats': True, 'broadcast': True}
        })
        
        # 添加默认关键词
        await bot_instance.add_default_keywords(group_id)
        
        await update.message.reply_text(f"✅ 已授权群组\n群组：{group_name}\nID：{group_id}\n已启用全部功能")
        
    except ValueError:
        await update.message.reply_text("❌ 群组ID必须是数字")
    except Exception as e:
        logger.error(f"授权群组错误: {e}")
        await update.message.reply_text("❌ 授权群组时出错")
        
@check_command_usage
@require_superadmin
async def handle_deauth_group(update: Update, context: CallbackContext):
    """处理/deauthgroup命令 - 取消群组授权"""
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
@require_superadmin
async def handle_add_default_keywords(update: Update, context: CallbackContext):
    """处理/adddefaultkeywords命令 - 为所有群组添加默认关键词"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 获取所有群组
    groups = await bot_instance.db.find_all_groups()
    count = 0
    
    for group in groups:
        group_id = group['group_id']
        await bot_instance.add_default_keywords(group_id)
        count += 1
    
    await update.message.reply_text(f"✅ 已为 {count} 个群组添加默认关键词")
