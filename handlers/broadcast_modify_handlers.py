"""
轮播消息修改功能处理器
"""
import logging
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

from utils.decorators import handle_callback_errors
from utils.time_utils import format_datetime

logger = logging.getLogger(__name__)

@handle_callback_errors
async def handle_broadcast_edit_callback(update: Update, context: CallbackContext, data: str):
    """
    处理轮播消息编辑回调
    
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
    logger.info(f"处理轮播消息编辑回调: {parts}")
    
    if len(parts) < 4:  # bc, edit, broadcast_id, group_id
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    broadcast_id = parts[2]
    group_id = int(parts[3])
    
    # 获取轮播消息
    broadcast = await bot_instance.db.get_broadcast_by_id(broadcast_id)
    if not broadcast:
        await query.edit_message_text(
            "❌ 找不到轮播消息",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("返回", callback_data=f"settings_broadcast_{group_id}")
            ]])
        )
        return
    
    # 将轮播数据存入用户上下文，用于编辑
    context.user_data['broadcast_form'] = {
        'group_id': group_id,
        'broadcast_id': broadcast_id,
        'text': broadcast.get('text', ''),
        'media': broadcast.get('media'),
        'buttons': broadcast.get('buttons', []),
        'repeat_type': broadcast.get('repeat_type', 'once'),
        'repeat_interval': broadcast.get('interval', 0),
        'start_time': broadcast.get('start_time').strftime('%Y-%m-%d %H:%M:%S') if broadcast.get('start_time') else None,
        'end_time': broadcast.get('end_time').strftime('%Y-%m-%d %H:%M:%S') if broadcast.get('end_time') else None,
        'is_editing': True  # 标记为编辑模式
    }
    
    # 显示编辑选项
    await show_broadcast_edit_options(update, context)

async def show_broadcast_edit_options(update: Update, context: CallbackContext):
    """
    显示轮播消息编辑选项
    
    参数:
        update: 更新对象
        context: 上下文对象
    """
    form_data = context.user_data.get('broadcast_form', {})
    broadcast_id = form_data.get('broadcast_id')
    group_id = form_data.get('group_id')
    
    # 构建当前状态摘要
    summary = "📝 编辑轮播消息\n\n"
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
    
    summary += "\n请选择要修改的内容:"
    
    # 构建操作按钮
    keyboard = [
        [InlineKeyboardButton("📝 修改文本", callback_data=f"bcform_add_text")],
        [InlineKeyboardButton("🖼️ 修改媒体", callback_data=f"bcform_add_media")],
        [InlineKeyboardButton("🔘 修改按钮", callback_data=f"bcform_add_button")],
        [InlineKeyboardButton("⏰ 修改计划", callback_data=f"bcform_set_schedule")],
    ]
    
    # 如果不是单次发送，添加结束时间设置按钮
    if form_data.get('repeat_type') and form_data.get('repeat_type') != 'once':
        keyboard.append([InlineKeyboardButton("🏁 修改结束时间", callback_data=f"bcform_set_end_time")])
    
    keyboard.extend([
        [InlineKeyboardButton("👁️ 预览效果", callback_data=f"bcform_preview")],
        [InlineKeyboardButton("✅ 保存修改", callback_data=f"bc_save_edit_{broadcast_id}_{group_id}")],
        [InlineKeyboardButton("❌ 取消", callback_data=f"broadcast_detail_{broadcast_id}_{group_id}")]
    ])
    
    # 显示编辑选项
    await update.callback_query.edit_message_text(
        summary,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@handle_callback_errors
async def handle_broadcast_save_edit_callback(update: Update, context: CallbackContext, data: str):
    """
    处理保存轮播消息编辑的回调
    
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
    if len(parts) < 5:  # bc, save, edit, broadcast_id, group_id
        await query.edit_message_text("❌ 无效的回调数据")
        return
        
    broadcast_id = parts[3]
    group_id = int(parts[4])
    
    # 获取表单数据
    form_data = context.user_data.get('broadcast_form', {})
    
    # 验证必要字段
    has_content = bool(form_data.get('text') or form_data.get('media') or form_data.get('buttons'))
    if not has_content:
        await query.answer("❌ 请至少添加一项内容")
        await show_broadcast_edit_options(update, context)
        return
    
    # 获取更新前的轮播消息数据，用于日志和比较
    try:
        old_broadcast = await bot_instance.db.get_broadcast_by_id(broadcast_id)
        logger.info(f"更新前的轮播消息数据: {old_broadcast}")
    except Exception as e:
        logger.warning(f"获取旧轮播消息数据失败: {e}")
        old_broadcast = None
    
    # 构建更新数据
    update_data = {
        'text': form_data.get('text', ''),
        'media': form_data.get('media'),
        'buttons': form_data.get('buttons', []),
        'repeat_type': form_data.get('repeat_type', 'once'),
        'interval': form_data.get('repeat_interval', 0)
    }
    
    # 处理开始时间
    if form_data.get('start_time'):
        try:
            start_time = datetime.strptime(form_data.get('start_time'), '%Y-%m-%d %H:%M:%S')
            update_data['start_time'] = start_time
        except ValueError:
            if form_data.get('start_time').lower() == 'now':
                update_data['start_time'] = datetime.now()
            else:
                await query.answer("❌ 开始时间格式不正确")
                await show_broadcast_edit_options(update, context)
                return
    
    # 处理结束时间
    if form_data.get('end_time'):
        try:
            end_time = datetime.strptime(form_data.get('end_time'), '%Y-%m-%d %H:%M:%S')
            update_data['end_time'] = end_time
        except ValueError:
            await query.answer("❌ 结束时间格式不正确")
            await show_broadcast_edit_options(update, context)
            return
    
    # 更新轮播消息
    try:
        logger.info(f"准备更新轮播消息，ID: {broadcast_id}，更新数据: {update_data}")
        success = await bot_instance.db.update_broadcast(broadcast_id, update_data)
        
        if success:
            # 清理表单数据
            if 'broadcast_form' in context.user_data:
                del context.user_data['broadcast_form']
            if 'waiting_for' in context.user_data:
                del context.user_data['waiting_for']
            
            # 获取更新后的数据用于重新调度
            updated_broadcast = await bot_instance.db.get_broadcast_by_id(broadcast_id)
            logger.info(f"轮播消息更新成功，更新后数据: {updated_broadcast}")
            
            # 刷新轮播调度器
            # 检查轮播管理器类型并调用相应方法
            if bot_instance.broadcast_manager:
                try:
                    # 检查是否是增强版轮播管理器
                    if hasattr(bot_instance.broadcast_manager, 'stop_broadcast') and hasattr(bot_instance.broadcast_manager, 'schedule_broadcast'):
                        # 先尝试停止旧的轮播任务
                        logger.info(f"停止旧的轮播任务: {broadcast_id}")
                        await bot_instance.broadcast_manager.stop_broadcast(broadcast_id)
                        
                        # 重新调度更新后的轮播消息
                        logger.info(f"重新调度轮播消息: {broadcast_id}")
                        await bot_instance.broadcast_manager.schedule_broadcast(updated_broadcast)
                    # 标准轮播管理器
                    elif hasattr(bot_instance.broadcast_manager, 'schedule_broadcast'):
                        # 重新调度更新后的轮播消息
                        logger.info(f"使用标准轮播管理器重新调度轮播消息: {broadcast_id}")
                        await bot_instance.broadcast_manager.schedule_broadcast(updated_broadcast)
                    else:
                        logger.warning(f"轮播管理器没有必要的方法来重新调度轮播消息: {broadcast_id}")
                        
                    logger.info(f"轮播消息已重新调度")
                except Exception as scheduler_error:
                    logger.error(f"重新调度轮播消息失败: {scheduler_error}", exc_info=True)
                    await query.edit_message_text(
                        "⚠️ 轮播消息已更新但重新调度失败，请检查日志",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("返回详情", callback_data=f"broadcast_detail_{broadcast_id}_{group_id}")
                        ]])
                    )
                    return
            else:
                logger.warning("找不到轮播管理器，无法重新调度轮播消息")
                
            await query.edit_message_text(
                "✅ 轮播消息已更新",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("返回详情", callback_data=f"broadcast_detail_{broadcast_id}_{group_id}")
                ]])
            )
        else:
            logger.error(f"更新轮播消息失败: {broadcast_id}")
            await query.edit_message_text(
                "❌ 轮播消息更新失败，请重试",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("返回编辑", callback_data=f"bc_edit_{broadcast_id}_{group_id}")
                ]])
            )
    except Exception as e:
        logger.error(f"更新轮播消息出错: {e}", exc_info=True)
        await query.edit_message_text(
            f"❌ 更新轮播消息出错: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("返回编辑", callback_data=f"bc_edit_{broadcast_id}_{group_id}")
            ]])
        )
