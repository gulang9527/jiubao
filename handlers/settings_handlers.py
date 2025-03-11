"""
设置处理函数，处理设置相关操作
"""
import logging
from typing import Dict, Any, Optional, List

from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

from utils.decorators import handle_callback_errors, require_admin
from utils.time_utils import format_duration
from utils.keyboard_utils import KeyboardBuilder
from db.models import GroupPermission

logger = logging.getLogger(__name__)

#######################################
# 设置处理函数
#######################################

@handle_callback_errors
async def handle_settings_callback(update: Update, context: CallbackContext, data: str):
    """
    处理设置菜单的回调
    
    参数:
        update: 更新对象
        context: 上下文对象
        data: 回调数据
    """
    query = update.callback_query
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 立即响应回调查询
    await query.answer()
    logger.info(f"处理设置回调: {data}")
    
    # 解析回调数据
    if data.startswith("settings_"):
        parts = data[9:].split('_')  # 去掉"settings_"前缀
    elif data.startswith("auto_delete_"):
        parts = data[12:].split('_')  # 去掉"auto_delete_"前缀
        return await handle_auto_delete_callback(update, context, parts)
    elif data.startswith("switch_toggle_"):
        parts = data[14:].split('_')  # 去掉"switch_toggle_"前缀
        return await handle_switch_toggle_callback(update, context, parts)
    elif data.startswith("stats_edit_"):
        parts = data[11:].split('_')  # 去掉"stats_edit_"前缀
        return await handle_stats_edit_callback(update, context, parts)
    else:
        logger.warning(f"未知的设置回调前缀: {data}")
        await query.edit_message_text("❌ 未知的设置操作")
        return
    
    if not parts:
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    action = parts[0]
    
    # 处理返回群组列表的情况
    if action == "back" or data == "show_manageable_groups":
        await show_manageable_groups(bot_instance, query, context)
        return
    
    # 获取群组ID
    try:
        if len(parts) > 1:
            group_id = int(parts[-1])
        else:
            # 尝试从回调数据中提取群组ID
            from utils.keyboard_utils import CallbackDataBuilder
            group_id = CallbackDataBuilder.get_group_id(data)
            if group_id is None:
                raise ValueError("无法获取群组ID")
    except ValueError:
        await query.edit_message_text("❌ 无效的群组ID")
        return
    
    # 验证用户权限
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        await query.edit_message_text("❌ 你没有权限管理此群组")
        return
        
    # 处理不同的设置操作
    if action == "select":
        # 显示群组的设置菜单
        await show_settings_menu(bot_instance, query, group_id)
    elif action == "stats":
        # 显示统计设置
        await show_stats_settings(bot_instance, query, group_id)
    elif action == "broadcast":
        # 显示轮播消息设置
        await show_broadcast_settings(bot_instance, query, group_id)
    elif action == "keywords":
        # 显示关键词设置
        await show_keyword_settings(bot_instance, query, group_id)
    elif action == "switches":
        # 显示开关设置
        await show_feature_switches(bot_instance, query, group_id)
    else:
        logger.warning(f"未知的设置操作: {action}")
        await query.edit_message_text(f"❌ 未知的设置操作: {action}")

#######################################
# 自动删除设置处理
#######################################

async def handle_auto_delete_callback(update: Update, context: CallbackContext, parts: List[str]):
    """
    处理自动删除设置的回调
    
    参数:
        update: 更新对象
        context: 上下文对象
        parts: 回调数据部分
    """
    query = update.callback_query
    bot_instance = context.application.bot_data.get('bot_instance')
    
    if not parts:
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    action = parts[0]
    
    # 获取群组ID
    try:
        group_id = int(parts[-1])
    except (ValueError, IndexError):
        await query.edit_message_text("❌ 无效的群组ID")
        return
    
    # 验证用户权限
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        await query.edit_message_text("❌ 你没有权限管理此群组")
        return
        
    # 获取群组设置
    settings = await bot_instance.db.get_group_settings(group_id)
    
    # 处理不同的操作
    if action == "toggle":
        # 切换自动删除开关
        auto_delete = not settings.get('auto_delete', False)
        settings['auto_delete'] = auto_delete
        await bot_instance.db.update_group_settings(group_id, settings)
        await show_auto_delete_settings(bot_instance, query, group_id, settings)
        
    elif action == "timeout":
        # 显示超时时间设置菜单
        await show_timeout_settings(bot_instance, query, group_id, settings)
        
    elif action == "set_timeout":
        # 设置特定的超时时间
        if len(parts) < 2:
            await query.edit_message_text("❌ 无效的超时时间")
            return
            
        timeout = int(parts[1])
        settings['auto_delete_timeout'] = timeout
        await bot_instance.db.update_group_settings(group_id, settings)
        await show_auto_delete_settings(bot_instance, query, group_id, settings)
        
    elif action == "custom_timeout":
        # 启动自定义超时时间设置
        await bot_instance.settings_manager.start_setting(
            update.effective_user.id, 
            'auto_delete_timeout', 
            group_id
        )
        await query.edit_message_text(
            "请输入自定义超时时间（单位：秒）：\n"
            "• 最小值: 60秒\n"
            "• 最大值: 86400秒（24小时）\n\n"
            "发送 /cancel 取消"
        )
    else:
        logger.warning(f"未知的自动删除操作: {action}")
        await query.edit_message_text(f"❌ 未知的自动删除操作: {action}")

#######################################
# 功能开关设置处理
#######################################

async def handle_switch_toggle_callback(update: Update, context: CallbackContext, parts: List[str]):
    """
    处理功能开关设置的回调
    
    参数:
        update: 更新对象
        context: 上下文对象
        parts: 回调数据部分
    """
    query = update.callback_query
    bot_instance = context.application.bot_data.get('bot_instance')
    
    if len(parts) < 2:
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    feature = parts[0]
    
    # 获取群组ID
    try:
        group_id = int(parts[1])
    except ValueError:
        await query.edit_message_text("❌ 无效的群组ID")
        return
    
    # 验证用户权限
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        await query.edit_message_text("❌ 你没有权限管理此群组")
        return
        
    # 获取群组信息
    group = await bot_instance.db.get_group(group_id)
    if not group:
        await query.edit_message_text(f"❌ 找不到群组 {group_id} 的信息")
        return
        
    # 检查该功能是否在群组权限中
    if feature not in group.get('permissions', []):
        await query.edit_message_text(f"❌ 群组 {group_id} 没有 {feature} 权限")
        return
        
    # 获取当前开关状态
    switches = group.get('feature_switches', {'keywords': True, 'stats': True, 'broadcast': True})
    current_status = switches.get(feature, True)
    
    # 切换功能开关状态
    new_status = not current_status
    
    # 更新数据库
    await bot_instance.db.db.groups.update_one(
        {'group_id': group_id},
        {'$set': {f'feature_switches.{feature}': new_status}}
    )
    
    # 重新显示功能开关设置菜单
    await show_feature_switches(bot_instance, query, group_id)

#######################################
# 设置菜单显示函数
#######################################

async def show_manageable_groups(bot_instance, query, context):
    """
    显示用户可管理的群组列表
    
    参数:
        bot_instance: 机器人实例
        query: 回调查询
        context: 上下文对象
    """
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
    """
    显示群组设置菜单
    
    参数:
        bot_instance: 机器人实例
        query: 回调查询
        group_id: 群组ID
    """
    group = await bot_instance.db.get_group(group_id)
    if not group:
        await query.edit_message_text(f"❌ 找不到群组 {group_id} 的信息")
        return
        
    # 获取权限列表
    permissions = group.get('permissions', [])
    
    # 使用键盘构建器创建设置菜单
    keyboard = KeyboardBuilder.create_settings_keyboard(group_id, permissions)
    
    # 显示设置菜单
    await query.edit_message_text(
        f"管理群组: {group_id}\n\n请选择要管理的功能：", 
        reply_markup=keyboard
    )

async def show_stats_settings(bot_instance, query, group_id: int):
    """
    显示统计设置
    
    参数:
        bot_instance: 机器人实例
        query: 回调查询
        group_id: 群组ID
    """
    settings = await bot_instance.db.get_group_settings(group_id)
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
    """
    显示轮播消息设置
    
    参数:
        bot_instance: 机器人实例
        query: 回调查询
        group_id: 群组ID
    """
    broadcasts = await bot_instance.db.get_broadcasts(group_id)
    keyboard = []  
    
    # 显示现有的轮播消息
    for bc in broadcasts:
        if bc is None:
            continue  # 跳过None值
        
        try:
            broadcast_type = '图片' if bc.get('media', {}).get('type') == 'photo' else \
                            '视频' if bc.get('media', {}).get('type') == 'video' else \
                            '文件' if bc.get('media', {}).get('type') == 'document' else '文本'
                            
            content_preview = bc.get('text', '')[:20] + '...' if len(bc.get('text', '')) > 20 else bc.get('text', '无内容')   
            keyboard.append([
                InlineKeyboardButton(
                    f"📢 {broadcast_type}: {content_preview}", 
                    callback_data=f"broadcast_detail_{bc['_id']}_{group_id}"
                )
            ])
        except Exception as e:
            logger.error(f"处理轮播消息时出错: {e}, 消息数据: {bc}")
            continue  # 跳过有问题的消息
        
    # 添加功能按钮
    keyboard.append([InlineKeyboardButton("➕ 添加轮播消息", callback_data=f"bcform_select_group_{group_id}")])
    keyboard.append([InlineKeyboardButton("返回设置菜单", callback_data=f"settings_select_{group_id}")])
    
    await query.edit_message_text(f"群组 {group_id} 的轮播消息设置", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_keyword_settings(bot_instance, query, group_id: int, page: int = 1):
    """
    显示关键词设置
    
    参数:
        bot_instance: 机器人实例
        query: 回调查询
        group_id: 群组ID
        page: 页码
    """
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

async def show_feature_switches(bot_instance, query, group_id: int):
    """
    显示功能开关设置
    
    参数:
        bot_instance: 机器人实例
        query: 回调查询
        group_id: 群组ID
    """
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

async def show_auto_delete_settings(bot_instance, query, group_id: int, settings: Optional[Dict[str, Any]] = None):
    """
    显示自动删除设置
    
    参数:
        bot_instance: 机器人实例
        query: 回调查询
        group_id: 群组ID
        settings: 群组设置
    """
    if settings is None:
        settings = await bot_instance.db.get_group_settings(group_id)
        
    status = '开启' if settings.get('auto_delete', False) else '关闭'
    import config
    timeout = settings.get('auto_delete_timeout', config.AUTO_DELETE_SETTINGS['default_timeout'])
    
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

async def show_timeout_settings(bot_instance, query, group_id: int, settings: Dict[str, Any]):
    """
    显示超时时间设置菜单
    
    参数:
        bot_instance: 机器人实例
        query: 回调查询
        group_id: 群组ID
        settings: 群组设置
    """
    current_timeout = settings.get('auto_delete_timeout', 300)
    
    # 构建选择键盘
    keyboard = [
        [InlineKeyboardButton(f"{'✅' if current_timeout == 300 else ' '} 5分钟", callback_data=f"auto_delete_set_timeout_{group_id}_300")],
        [InlineKeyboardButton(f"{'✅' if current_timeout == 600 else ' '} 10分钟", callback_data=f"auto_delete_set_timeout_{group_id}_600")],
        [InlineKeyboardButton(f"{'✅' if current_timeout == 1800 else ' '} 30分钟", callback_data=f"auto_delete_set_timeout_{group_id}_1800")],
        [InlineKeyboardButton(f"{'✅' if current_timeout == 3600 else ' '} 1小时", callback_data=f"auto_delete_set_timeout_{group_id}_3600")],
        [InlineKeyboardButton(f"{'✅' if current_timeout == 7200 else ' '} 2小时", callback_data=f"auto_delete_set_timeout_{group_id}_7200")],
        [InlineKeyboardButton("自定义", callback_data=f"auto_delete_custom_timeout_{group_id}")],
        [InlineKeyboardButton("返回", callback_data=f"auto_delete_toggle_{group_id}")]
    ]
    
    await query.edit_message_text("请选择自动删除的超时时间：", reply_markup=InlineKeyboardMarkup(keyboard))

#######################################
# 设置处理器
#######################################

async def handle_stats_edit_callback(update: Update, context: CallbackContext, parts: List[str]):
    logger.info(f"统计设置编辑回调数据部分: {parts}")
    """
    处理统计设置编辑的回调
    
    参数:
        update: 更新对象
        context: 上下文对象
        parts: 回调数据部分
    """
    query = update.callback_query
    bot_instance = context.application.bot_data.get('bot_instance')
    
    logger.info(f"统计设置编辑回调数据部分: {parts}")
    
    # 处理可能的特殊情况
    if len(parts) >= 2 and parts[0] == "min" and parts[1].startswith("bytes"):
        action = "min_bytes"
        group_id_part = parts[1].split("_", 1)[1] if "_" in parts[1] else parts[-1]
    # 处理 toggle_media 特殊情况
    elif len(parts) >= 2 and parts[0] == "toggle" and parts[1] == "media":
        action = "toggle_media"
    else:
        if len(parts) < 2:
            await query.edit_message_text("❌ 无效的回调数据")
            return
        action = parts[0]
    
    # 获取群组ID
    try:
        group_id = int(parts[-1])
    except ValueError:
        await query.edit_message_text("❌ 无效的群组ID")
        return
    
    # 验证用户权限
    if not await bot_instance.db.can_manage_group(update.effective_user.id, group_id):
        await query.edit_message_text("❌ 你没有权限管理此群组")
        return
    
    # 处理不同的设置编辑
    if action == "min_bytes":
        # 启动最小字节数设置
        await bot_instance.settings_manager.start_setting(
            update.effective_user.id, 
            'stats_min_bytes', 
            group_id
        )
        await query.edit_message_text(
            "请输入最小统计字节数：\n"
            "• 设置为0表示统计所有消息\n"
            "• 建议设置为10-100之间的数值\n\n"
            "发送 /cancel 取消"
        )
    elif action == "toggle_media":
        # 切换媒体统计开关
        settings = await bot_instance.db.get_group_settings(group_id)
        count_media = not settings.get('count_media', False)
        settings['count_media'] = count_media
        await bot_instance.db.update_group_settings(group_id, settings)
        await show_stats_settings(bot_instance, query, group_id)
    elif action == "daily_rank":
        # 设置日排行显示数量
        await bot_instance.settings_manager.start_setting(
            update.effective_user.id, 
            'stats_daily_rank', 
            group_id
        )
        await query.edit_message_text(
            "请输入日排行显示数量：\n"
            "• 最小值: 5\n"
            "• 最大值: 50\n\n"
            "发送 /cancel 取消"
        )
    elif action == "monthly_rank":
        # 设置月排行显示数量
        await bot_instance.settings_manager.start_setting(
            update.effective_user.id, 
            'stats_monthly_rank', 
            group_id
        )
        await query.edit_message_text(
            "请输入月排行显示数量：\n"
            "• 最小值: 5\n"
            "• 最大值: 50\n\n"
            "发送 /cancel 取消"
        )
    else:
        logger.warning(f"未知的统计设置编辑操作: {action}")
        await query.edit_message_text(f"❌ 未知的设置操作: {action}")
        
async def process_min_bytes_setting(bot_instance, state, message):
    """
    处理最小字节数设置
    
    参数:
        bot_instance: 机器人实例
        state: 设置状态
        message: 消息对象
    """
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
    """
    处理日排行显示数量设置
    
    参数:
        bot_instance: 机器人实例
        state: 设置状态
        message: 消息对象
    """
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
    """
    处理月排行显示数量设置
    
    参数:
        bot_instance: 机器人实例
        state: 设置状态
        message: 消息对象
    """
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

async def process_auto_delete_timeout(bot_instance, state, message):
    """
    处理自动删除超时设置
    
    参数:
        bot_instance: 机器人实例
        state: 设置状态
        message: 消息对象
    """
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
        await message.reply_text(f"✅ 自动删除超时时间已设置为 {format_duration(timeout)}")
    except ValueError:
        await message.reply_text("❌ 请输入一个有效的数字")
