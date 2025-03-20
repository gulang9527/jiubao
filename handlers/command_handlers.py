"""
排行榜显示完整优化代码
"""
import logging
import html
import math
import time
import datetime
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from utils.decorators import check_command_usage, handle_callback_errors, require_superadmin
from utils.message_utils import set_message_expiry

logger = logging.getLogger(__name__)

# 用户信息缓存
user_cache = {}

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
        "📊 /checkstats - 检查统计设置\n"
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
            "🧹 /cleanupinvalidgroups - 清理无效群组\n"
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

def get_char_width(char):
    """
    计算字符的显示宽度，更准确的实现
    - 汉字、日文、韩文等全角字符宽度为2
    - ASCII字符宽度为1
    - 其他字符根据Unicode范围确定宽度
    """
    code = ord(char)
    
    # ASCII字符
    if code <= 127:
        return 1
        
    # 全角空格
    if char == '\u3000':
        return 2
        
    # 中文字符范围
    if any([
        '\u4e00' <= char <= '\u9fff',  # CJK基本汉字
        '\u3400' <= char <= '\u4dbf',  # CJK扩展A
        '\uf900' <= char <= '\ufaff',  # CJK兼容汉字
        '\u20000' <= char <= '\u2a6df',  # CJK扩展B
        '\u2a700' <= char <= '\u2b73f',  # CJK扩展C
        '\u2b740' <= char <= '\u2b81f',  # CJK扩展D
        '\u2b820' <= char <= '\u2ceaf',  # CJK扩展E
        '\u2ceb0' <= char <= '\u2ebef',  # CJK扩展F
    ]):
        return 2
        
    # 日文
    if any([
        '\u3040' <= char <= '\u309f',  # 平假名
        '\u30a0' <= char <= '\u30ff',  # 片假名
    ]):
        return 2
        
    # 韩文
    if '\uac00' <= char <= '\ud7a3':
        return 2
        
    # 全角标点和符号
    if any([
        '\u3000' <= char <= '\u303f',  # CJK符号和标点
        '\uff00' <= char <= '\uffef',  # 全角ASCII、全角中英文标点
    ]):
        return 2
        
    # 表情符号和特殊符号
    if any([
        '\u2600' <= char <= '\u27bf',  # 杂项符号
        '\u1f300' <= char <= '\u1f64f',  # Emoji表情
        '\u1f680' <= char <= '\u1f6ff',  # 交通和地图符号
    ]):
        return 2
        
    # 其他字符默认宽度1
    return 1

def get_string_display_width(s):
    """
    计算字符串的显示宽度
    """
    return sum(get_char_width(c) for c in s)

def truncate_string_by_width(s, max_width):
    """
    按显示宽度截断字符串，确保在任何情况下都不会超过最大宽度
    
    参数:
        s: 输入字符串
        max_width: 最大显示宽度
        
    返回:
        截断后的字符串，如果截断则添加"…"符号
    """
    if not s:
        return s
        
    width = 0
    result = []
    
    for i, char in enumerate(s):
        char_width = get_char_width(char)
        # 检查添加当前字符是否会超出最大宽度(减去省略号的宽度1)
        if width + char_width > max_width - 1:
            # 确保不超出最大宽度
            return ''.join(result) + "…"
        
        width += char_width
        result.append(char)
        
    return ''.join(result) 

# 添加一个简单内存缓存
class SimpleCache:
    def __init__(self):
        self.data = {}
        self.expiry = {}
        self._lock = asyncio.Lock()
    
    async def set(self, key, value, expire_seconds=None):
        async with self._lock:
            self.data[key] = value
            if expire_seconds:
                self.expiry[key] = time.time() + expire_seconds
    
    async def get(self, key):
        async with self._lock:
            if key in self.data:
                if key in self.expiry and time.time() > self.expiry[key]:
                    del self.data[key]
                    del self.expiry[key]
                    return None
                return self.data[key]
            return None
    
    async def exists(self, key):
        async with self._lock:
            return key in self.data
    
    async def delete(self, key):
        async with self._lock:
            if key in self.data:
                del self.data[key]
            if key in self.expiry:
                del self.expiry[key]

# 初始化缓存
memory_cache = SimpleCache()

async def get_user_display_name(chat_id, user_id, context):
    """获取用户显示名称，带缓存"""
    cache_key = f"{chat_id}:{user_id}"
    
    # 尝试从缓存获取
    cached_name = await memory_cache.get(cache_key)
    if cached_name:
        return cached_name
        
    try:
        # 从Telegram API获取
        user = await asyncio.wait_for(
            context.bot.get_chat_member(chat_id, user_id),
            timeout=2.0
        )
        display_name = html.escape(user.user.full_name)
        
        # 缓存结果，24小时过期
        await memory_cache.set(cache_key, display_name, 86400)
        return display_name
    except Exception as e:
        logger.warning(f"获取用户 {user_id} 信息失败: {e}")
        return f'用户{user_id}'

async def get_message_stats_from_db(group_id: int, time_range: str = 'day', limit: int = 15, skip: int = 0, context=None):
    """
    从数据库获取消息统计数据 - 优化版本
    
    参数:
        group_id: 群组ID
        time_range: 时间范围，'day'表示24小时内，'month'表示30天内
        limit: 返回结果数量限制
        skip: 跳过的结果数量（用于分页）
        context: 可选上下文对象，用于获取bot_instance
        
    返回:
        消息统计数据列表
    """
    try:
        bot_instance = None
        
        # 如果提供了上下文，从上下文获取bot_instance
        if context and hasattr(context, 'application'):
            bot_instance = context.application.bot_data.get('bot_instance')
        
        # 如果没有bot_instance，记录错误并返回空列表
        if not bot_instance or not bot_instance.db:
            logger.error("无法获取数据库实例")
            return []
        
        # 设置时间过滤条件
        now = datetime.datetime.now()
        
        # 基础过滤条件 - 增加更严格的过滤
        match = {
            'group_id': group_id,
            'total_messages': {'$gt': 0},
            'is_bot': {'$ne': True}  # 排除机器人
        }
        
        # 添加时间范围过滤条件
        if time_range == 'day':
            # 当天
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            match['date'] = today
        elif time_range == 'month':
            # 30天前的日期（YYYY-MM-DD格式）
            thirty_days_ago = (now - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
            today = now.strftime('%Y-%m-%d')
            match['date'] = {'$gte': thirty_days_ago, '$lte': today}
        
        # 日志记录查询条件，帮助调试
        logger.info(f"消息统计查询条件: {match}")
        
        # 优化的聚合管道，解决重复计数问题
        pipeline = [
            # 1. 初始匹配阶段 - 基本过滤
            {'$match': match},
            
            # 2. 确保每条消息只被计数一次并加强过滤
            {'$group': {
                '_id': {'msg_id': '$message_id', 'user_id': '$user_id', 'date': '$date'},
                'message_count': {'$sum': 1},
                'user_id': {'$first': '$user_id'},
                'valid': {'$first': {'$gt': ['$total_messages', 0]}}
            }},
            
            # 3. 按用户ID分组汇总，确保只统计有效消息
            {'$match': {
                'valid': True
            }},
            
            {'$group': {
                '_id': '$user_id',
                'total_messages': {'$sum': '$message_count'}
            }},
            
            # 4. 更严格的过滤条件，确保排除无效用户和消息数为0的记录
            {'$match': {
                '$and': [
                    {'_id': {'$ne': None}},
                    {'_id': {'$ne': 0}},
                    {'total_messages': {'$gt': 0}}
                ]
            }},
            
            # 5. 排序
            {'$sort': {'total_messages': -1}},
            
            # 6. 分页
            {'$skip': skip},
            {'$limit': limit}
        ]
        
        # 设置超时选项 - 增加超时时间
        options = {
            'maxTimeMS': 10000  # 10秒超时
        }
        
        # 执行聚合查询
        stats = await bot_instance.db.db.message_stats.aggregate(pipeline, **options).to_list(None)
        
        # 深度复制结果，避免引用问题
        validated_stats = []
        for stat in stats:
            try:
                # 确保关键字段存在且有效
                if not stat or '_id' not in stat or 'total_messages' not in stat:
                    continue
                    
                # 确保ID不为空且为数字
                user_id = stat.get('_id')
                if user_id is None or not isinstance(user_id, (int, float, str)):
                    continue
                    
                # 确保消息计数为正整数
                message_count = stat.get('total_messages', 0)
                if not isinstance(message_count, (int, float)) or message_count <= 0:
                    continue
                    
                # 安全地进行类型转换
                try:
                    user_id_int = int(user_id)
                    if user_id_int <= 0:  # 用户ID应为正数
                        continue
                        
                    message_count_int = int(message_count)
                    if message_count_int <= 0:  # 消息数应为正数
                        continue
                        
                    validated_stats.append({
                        '_id': user_id_int,
                        'total_messages': message_count_int
                    })
                except (ValueError, TypeError):
                    # 转换失败，跳过此记录
                    continue
            except Exception as e:
                logger.error(f"验证统计数据出错: {e}", exc_info=True)
                # 继续处理下一条记录
                continue
        
        return validated_stats
    except asyncio.TimeoutError:
        logger.error(f"获取消息统计超时: 群组={group_id}, 时间范围={time_range}")
        return []
    except Exception as e:
        logger.error(f"获取消息统计失败: {e}", exc_info=True)
        return []

async def get_total_stats_count(group_id, time_range, context):
    """获取统计总记录数 - 用于准确计算页数"""
    try:
        bot_instance = context.application.bot_data.get('bot_instance')
        
        # 基础过滤条件 - 与 get_message_stats_from_db 保持一致
        match = {
            'group_id': group_id,
            'total_messages': {'$gt': 0},
            'is_bot': {'$ne': True}
        }
        
        # 添加时间范围过滤条件
        if time_range == 'day':
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            match['date'] = today
        elif time_range == 'month':
            thirty_days_ago = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            match['date'] = {'$gte': thirty_days_ago, '$lte': today}
        
        # 使用与 get_message_stats_from_db 相同的逻辑来计数
        pipeline = [
            {'$match': match},
            # 确保每条消息只被计数一次
            {'$group': {
                '_id': {'msg_id': '$message_id', 'user_id': '$user_id', 'date': '$date'},
                'user_id': {'$first': '$user_id'}
            }},
            # 按用户ID分组
            {'$group': {
                '_id': '$user_id'
            }},
            # 过滤无效用户
            {'$match': {
                '_id': {'$ne': None}
            }},
            {'$count': 'total'}
        ]
        
        # 设置超时选项
        options = {
            'maxTimeMS': 5000  # 5秒超时
        }
        
        result = await bot_instance.db.db.message_stats.aggregate(pipeline, **options).to_list(None)
        if result and len(result) > 0:
            return result[0].get('total', 0)
        return 0
    except asyncio.TimeoutError:
        logger.error(f"获取统计总数超时: 群组={group_id}, 时间范围={time_range}")
        return 0
    except Exception as e:
        logger.error(f"获取统计总数失败: {e}", exc_info=True)
        return 0

async def format_rank_rows(stats, page, group_id, context):
    """
    格式化排行榜行数据，考虑中英文字符宽度差异，使用普通文本（非链接）显示用户名
    
    参数:
        stats: 统计数据
        page: 当前页码
        group_id: 群组ID
        context: 回调上下文
        
    返回:
        格式化后的排行榜行HTML文本
    """
    import html
    
    # 固定用户名最大显示宽度
    MAX_NAME_WIDTH = 20
    # 消息数的固定位置（从行首开始的字符数）
    FIXED_MSG_POSITION = 24
    
    # 构建每一行文本
    rows = []
    start_rank = (page-1)*15 + 1
    
    for i, stat in enumerate(stats, start=start_rank):
        try:
            # 跳过无效数据
            if not isinstance(stat, dict) or '_id' not in stat or 'total_messages' not in stat:
                logger.warning(f"跳过无效的统计数据: {stat}")
                continue
                
            # 验证消息数是否为正数
            total_messages = stat.get('total_messages', 0)
            if not isinstance(total_messages, (int, float)) or total_messages <= 0:
                logger.warning(f"跳过消息数无效的统计数据: {stat}")
                continue
                
            # 添加奖牌图标（前三名）
            rank_prefix = ""
            if page == 1:
                if i == 1:
                    rank_prefix = "🥇 "  # 金牌
                elif i == 2:
                    rank_prefix = "🥈 "  # 银牌
                elif i == 3:
                    rank_prefix = "🥉 "  # 铜牌
            
            # 获取用户信息 - 使用缓存
            display_name = await get_user_display_name(group_id, stat['_id'], context)
            
            # 确保必须截断超长用户名
            original_width = get_string_display_width(display_name)
            if original_width > MAX_NAME_WIDTH:
                display_name = truncate_string_by_width(display_name, MAX_NAME_WIDTH)
            
            # 检查是否有奖牌
            has_medal = rank_prefix != ""
            
            # 计算序号部分的宽度（包括排名图标）
            # 注意：奖牌图标是表情符号，占用2个字符宽度
            rank_prefix_width = 2 if rank_prefix else 0
            rank_num_width = len(str(i))
            
            # 计算当前内容的显示宽度
            user_width = get_string_display_width(display_name)
            
            # 计算需要添加的空格数，确保"消息数"位置固定
            # 基础宽度: 排名前缀 + 序号 + ". " + 用户名
            base_width = rank_prefix_width + rank_num_width + 2 + user_width
            space_count = max(2, FIXED_MSG_POSITION - base_width)
            space_padding = ' ' * space_count
            
            # 构建一行，注意对奖牌emoji进行特殊处理
            message_count = f"{total_messages}条"
                
            if has_medal:
                # 对于有奖牌的行，确保序号和名字对齐
                row = f"{rank_prefix}{i}. {display_name}{space_padding}{message_count}"
            else:
                # 对于没有奖牌的行，增加两个空格保持对齐
                row = f"  {i}. {display_name}{space_padding}{message_count}"
            
            rows.append(row)
        except Exception as e:
            logger.error(f"格式化排行行出错: {e}", exc_info=True)
            # 继续处理下一条，不中断整个格式化过程
            continue
    
    # 如果没有成功格式化任何行，返回提示信息
    if not rows:
        if page == 1:
            return "暂无聊天记录，快来聊天吧！"
        else:
            return "没有更多数据了"
        
    # 不添加恢复数据的解释
    result = "\n".join(rows)
    return result

@check_command_usage
async def handle_rank_command(update: Update, context: CallbackContext):
    """处理 /rank 命令，显示群组消息排行榜"""
    try:
        # 只在群组中响应
        if update.effective_chat.type not in ['group', 'supergroup']:
            await update.message.reply_text("此命令只能在群组中使用。")
            return

        # 获取群组信息
        chat = update.effective_chat
        group_id = chat.id
        group_name = chat.title
        
        # 获取命令类型
        command = update.message.text.split()[0].lower()
        
        # 设置页码和标题
        page = 1
        
        # 获取统计数据
        if command == '/tongji':
            # 获取今日统计
            title = f"📊 {group_name} 今日消息排行"
            time_range = 'day'
        else:  # /tongji30
            # 获取30天统计
            title = f"📊 {group_name} 30天消息排行"
            time_range = 'month'
        
        # 获取统计数据 - 使用超时控制
        try:
            stats = await asyncio.wait_for(
                get_message_stats_from_db(group_id, time_range=time_range, limit=15, context=context),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.error(f"获取消息统计超时: 群组={group_id}, 时间范围={time_range}")
            msg = await update.message.reply_text("获取排行数据超时，请稍后再试。")
            await set_message_expiry(context=context, chat_id=group_id, message_id=msg.message_id, feature="rank_command")
            return
        
        # 如果没有数据，显示提示信息
        if not stats:
            msg = await update.message.reply_text("暂无排行数据。")
            
            # 确保自动删除设置生效
            await set_message_expiry(
                context=context,
                chat_id=group_id,
                message_id=msg.message_id,
                feature="rank_command"
            )
            return
        
        # 获取总记录数计算总页数
        total_count = await get_total_stats_count(group_id, time_range, context)
        
        # 计算总页数（每页15条记录）
        total_pages = max(1, (total_count + 14) // 15)
        
        # 构建分页按钮
        keyboard = []
        if total_pages > 1:
            buttons = []
            if page < total_pages:
                buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"rank_next_{page+1}_{command.replace('/', '')}"))
            keyboard.append(buttons)

        # 构建HTML格式的排行文本
        text = f"<b>{title}</b>\n\n"
        
        # 使用格式化函数生成排行行文本
        try:
            text += await asyncio.wait_for(
                format_rank_rows(stats, page, group_id, context),
                timeout=3.0
            )
        except asyncio.TimeoutError:
            text += "格式化数据超时，请稍后再试。"
        
        # 添加分页信息，减少空行
        if total_pages > 1:
            text += f"\n<i>第 {page}/{total_pages} 页</i>"

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
    except Exception as e:
        logger.error(f"处理排行命令出错: {e}", exc_info=True)
        await update.message.reply_text("处理命令时出错，请稍后再试。")

@check_command_usage
async def handle_check_stats_settings(update: Update, context: CallbackContext):
    """处理/checkstats命令 - 检查统计设置"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    # 获取群组ID
    group_id = update.effective_chat.id
    
    # 获取群组设置
    settings = await bot_instance.db.get_group_settings(group_id)
    
    # 统计相关设置
    min_bytes = settings.get('min_bytes', 0)
    count_media = settings.get('count_media', True)
    daily_rank_size = settings.get('daily_rank_size', 15)
    monthly_rank_size = settings.get('monthly_rank_size', 15)
    
    # 检查权限
    has_stats_perm = await bot_instance.has_permission(group_id, GroupPermission.STATS)
    
    # 构建消息
    message = f"📊 统计设置检查\n\n"
    message += f"群组ID: {group_id}\n"
    message += f"统计权限: {'✅ 已启用' if has_stats_perm else '❌ 未启用'}\n"
    message += f"最小字节数: {min_bytes}\n"
    message += f"统计媒体消息: {'✅ 是' if count_media else '❌ 否'}\n"
    message += f"日排行显示数量: {daily_rank_size}\n"
    message += f"月排行显示数量: {monthly_rank_size}\n\n"
    
    # 检查数据库记录
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        count = await bot_instance.db.db.message_stats.count_documents({
            'group_id': group_id,
            'date': today
        })
        message += f"今日消息记录数: {count}\n"
        
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        month_count = await bot_instance.db.db.message_stats.count_documents({
            'group_id': group_id,
            'date': {'$gte': thirty_days_ago, '$lte': today}
        })
        message += f"30天内消息记录数: {month_count}"
    except Exception as e:
        logger.error(f"检查数据库记录失败: {e}", exc_info=True)
        message += "⚠️ 数据库记录查询失败"
    
    await update.message.reply_text(message)

@handle_callback_errors
async def handle_rank_page_callback(update: Update, context: CallbackContext, *args, **kwargs):
    """处理排行榜分页回调，优化以防止快速翻页崩溃"""
    query = update.callback_query
    
    try:
        # 立即响应回调以减少用户等待
        await query.answer()
        
        # 获取按钮数据
        data = query.data.split("_")
        if len(data) < 3:
            logger.error(f"无效的回调数据: {query.data}")
            await query.edit_message_text("无效的回调数据，请重新尝试。")
            return
            
        action = data[1]
        current_page = int(data[2])
        
        # 获取命令类型（tongji 或 tongji30）
        command_type = data[3] if len(data) > 3 else "tongji"
        time_range = 'day' if command_type == 'tongji' else 'month'
        
        # 获取群组信息
        chat = update.effective_chat
        if not chat:
            logger.error("无法获取聊天信息")
            return
            
        group_id = chat.id
        group_name = chat.title or f"群组 {group_id}"
        
        logger.info(f"处理排行榜回调: 群组={group_id}, 页码={current_page}, 时间范围={time_range}")
        
        # 增强的并发控制和超时保护
        user_id = update.effective_user.id
        processing_key = f"rank_processing:{user_id}:{group_id}:{action}"
        processing_time_key = f"{processing_key}_time"
        
        # 检查是否有待处理请求以及是否已超时
        current_time = time.time()
        last_processing_time = context.user_data.get(processing_time_key, 0)
        is_processing = context.user_data.get(processing_key, False)
        
        # 如果上次处理已超过30秒，认为已超时可以重新处理
        if is_processing and (current_time - last_processing_time) > 30:
            logger.warning(f"用户 {user_id} 在群组 {group_id} 的排行榜请求已超时，允许新请求")
            is_processing = False
        
        if is_processing:
            logger.warning(f"用户 {user_id} 在群组 {group_id} 中有待处理的排行榜请求，忽略新请求")
            await query.answer("正在处理您的上一个请求，请稍后再试")
            return
        
        # 设置处理标记和时间戳
        context.user_data[processing_key] = True
        context.user_data[processing_time_key] = current_time
        
        try:
            # 改进页码逻辑：处理上一页和下一页
            if action == "prev":
                page = max(1, current_page - 1)  # 确保页码不小于1
            elif action == "next":
                page = current_page  # 直接使用回调数据中的当前页值
                                     # 因为回调数据中已经包含了正确的下一页
            else:
                page = current_page
            
            # 使用缓存优化总记录数查询
            cache_key = f"total_count:{group_id}:{time_range}"
            total_count = context.user_data.get(cache_key)
            
            # 如果没有缓存，再进行数据库查询
            if total_count is None:
                total_count = await get_total_stats_count(group_id, time_range, context)
                # 缓存结果，设置较短的有效期
                context.user_data[cache_key] = total_count
                # 设置定时器清除缓存（如果context有job_queue）
                if hasattr(context, 'job_queue') and context.job_queue:
                    context.job_queue.run_once(
                        lambda _: context.user_data.pop(cache_key, None) if cache_key in context.user_data else None,
                        120  # 2分钟后清除缓存
                    )
            
            # 使用更准确的页数计算方法
            total_pages = math.ceil(total_count / 15) if total_count > 0 else 1
            
            # 处理边界情况
            if total_count == 0:
                # 没有数据
                await query.edit_message_text("暂无排行数据。")
                return
                
            # 确保页码在有效范围内
            if page < 1:
                page = 1
            elif page > total_pages:
                page = total_pages
            
            # 安全获取排行数据 - 使用超时控制
            try:
                skip = (page - 1) * 15
                stats = await asyncio.wait_for(
                    get_message_stats_from_db(
                        group_id, 
                        time_range=time_range, 
                        limit=15,
                        skip=skip,
                        context=context
                    ),
                    timeout=5.0  # 5秒超时
                )
            except asyncio.TimeoutError:
                logger.error(f"获取排行数据超时: 群组={group_id}, 时间范围={time_range}")
                await query.edit_message_text(
                    "获取排行数据超时，请稍后再试。",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("重试", callback_data=f"rank_{action}_{current_page}_{command_type}")
                    ]])
                )
                return
            
            # 如果没有数据，显示提示信息
            if not stats:
                await query.edit_message_text("暂无更多排行数据。", reply_markup=None)
                return
            
            # 构建分页按钮
            keyboard = []
            if total_pages > 1:
                buttons = []
                if page > 1:
                    buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"rank_prev_{page}_{command_type}"))
                if page < total_pages:
                    buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"rank_next_{page+1}_{command_type}"))
                keyboard.append(buttons)
            
            # 获取标题
            title = f"📊 {group_name} {'今日' if time_range == 'day' else '30天'}消息排行"
            
            # 构建HTML格式的排行文本
            text = f"<b>{title}</b>\n\n"
            
            # 使用格式化函数生成排行行文本
            try:
                formatted_rows = await asyncio.wait_for(
                    format_rank_rows(stats, page, group_id, context),
                    timeout=3.0  # 3秒超时
                )
                text += formatted_rows
            except asyncio.TimeoutError:
                logger.error(f"格式化排行行文本超时: 群组={group_id}, 页码={page}")
                text += "格式化数据超时，请重试。"
            
            # 添加分页信息，减少空行
            if total_pages > 1:
                text += f"\n<i>第 {page}/{total_pages} 页</i>"
            
            # 更新消息内容，使用异常处理增强稳定性
            try:
                await query.edit_message_text(
                    text=text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
                )
            except Exception as e:
                logger.error(f"更新排行榜消息失败: {e}")
                # 尝试发送新消息而不是编辑
                try:
                    await context.bot.send_message(
                        chat_id=group_id,
                        text=f"排行榜更新失败，请重新查询。",
                        reply_to_message_id=query.message.message_id
                    )
                except:
                    pass
        finally:
            # 清除处理标记和时间戳
            context.user_data[processing_key] = False
            context.user_data.pop(processing_time_key, None)
            
    except Exception as e:
        logger.error(f"处理排行榜回调时出错: {e}", exc_info=True)
        try:
            await query.edit_message_text(
                "处理请求时出错，请稍后再试。",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("重试", callback_data=query.data)
                ]])
            )
        except:
            pass

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
        if user.get('role') == UserRole.SUPERADMIN.value:
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

@check_command_usage
@require_superadmin
async def handle_cleanup_invalid_groups(update: Update, context: CallbackContext):
    """处理/cleanupinvalidgroups命令 - 清理无效的群组"""
    bot_instance = context.application.bot_data.get('bot_instance')
    
    try:
        # 确认操作
        await update.message.reply_text(
            "⚠️ 此操作将删除所有无效的群组记录，包括:\n"
            "- 群组ID为0或空的记录\n"
            "- 群组ID为正数的记录\n"
            "- 群组ID为默认值(-1001234567890)的记录\n\n"
            "请回复 'confirm' 确认执行，或 'cancel' 取消操作"
        )
        
        # 设置等待确认状态
        context.user_data['waiting_for_cleanup_confirm'] = True
        return
    except Exception as e:
        logger.error(f"清理无效群组命令出错: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 命令处理出错: {str(e)}")

# 优化后新增的消息统计更新函数
async def update_message_stats(update: Update, context: CallbackContext):
    """更新消息统计，使用改进的去重逻辑"""
    if not update.effective_user or not update.effective_chat or not update.message:
        return
    
    # 跳过机器人消息
    if update.effective_user.is_bot:
        return
    
    user_id = update.effective_user.id
    group_id = update.effective_chat.id
    message_id = update.message.message_id
    
    # 确保是群组消息
    if update.effective_chat.type not in ['group', 'supergroup']:
        return
    
    bot_instance = context.application.bot_data.get('bot_instance')
    if not bot_instance:
        return
    
    # 检查统计权限
    if not await bot_instance.has_permission(group_id, GroupPermission.STATS):
        return
    
    # 获取当前日期
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # 获取群组设置
    settings = await bot_instance.db.get_group_settings(group_id)
    min_bytes = settings.get('min_bytes', 0)
    count_media = settings.get('count_media', True)
    
    # 确定消息类型
    message_type = 'text'
    if update.message.photo:
        message_type = 'photo'
    elif update.message.video:
        message_type = 'video'
    elif update.message.document:
        message_type = 'document'
    elif update.message.sticker:
        message_type = 'sticker'
    
    # 检查是否应该计数该消息
    should_count = True
    
    # 对于文本消息，检查长度
    if message_type == 'text' and update.message.text:
        if len(update.message.text.encode('utf-8')) < min_bytes:
            should_count = False
    
    # 对于媒体消息，检查是否计数
    elif not count_media and message_type != 'text':
        should_count = False
    
    # 如果应该计数，则添加到数据库
    if should_count:
        try:
            # 使用更可靠的去重方式 - 尝试使用唯一键插入
            # 如果数据库支持唯一索引，可以考虑在 group_id 和 message_id 上创建复合唯一索引
            # 这里使用查询+更新的原子操作
            result = await bot_instance.db.db.message_stats.update_one(
                {
                    'group_id': group_id,
                    'message_id': message_id
                },
                {
                    '$setOnInsert': {
                        'group_id': group_id,
                        'user_id': user_id,
                        'date': today,
                        'message_id': message_id,
                        'message_type': message_type,
                        'total_messages': 1,
                        'is_bot': False,
                        'timestamp': datetime.datetime.now()
                    }
                },
                upsert=True
            )
            
            # 如果没有插入新文档，说明消息已存在
            if result.matched_count > 0:
                return
                
            logger.debug(f"已记录消息统计: 用户={user_id}, 群组={group_id}, 类型={message_type}")
        except Exception as e:
            logger.error(f"记录消息统计失败: {e}", exc_info=True)
            

