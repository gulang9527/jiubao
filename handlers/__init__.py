"""
处理器模块初始化文件
"""
import logging
from telegram.ext import (
    CommandHandler, MessageHandler, CallbackQueryHandler, filters
)

from handlers.command_handlers import (
    handle_start, handle_settings, handle_rank_command, 
    handle_admin_groups, handle_add_admin, handle_del_admin,
    handle_add_superadmin, handle_del_superadmin, handle_auth_group,
    handle_deauth_group, handle_check_config, handle_cancel,
    handle_easy_keyword, handle_easy_broadcast, handle_add_default_keywords
)
from handlers.message_handlers import handle_message
from handlers.callback_handlers import handle_callback

logger = logging.getLogger(__name__)

def register_all_handlers(application, callback_handler):
    """
    注册所有处理器
    
    参数:
        application: 应用实例
        callback_handler: 回调处理器
    """
    logger.info("开始注册所有处理器")
    
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
    application.add_handler(CommandHandler("adddefaultkeywords", handle_add_default_keywords))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(
        filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document() | filters.ANIMATION,
        handle_message
    ))
    
    
    # 添加简化的关键词和广播处理器
    application.add_handler(CommandHandler("easykeyword", handle_easy_keyword))
    application.add_handler(CommandHandler("easybroadcast", handle_easy_broadcast))

    # 注册回调查询处理器
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # 设置回调处理器
    # 导入各种回调处理函数
    from handlers.settings_handlers import handle_settings_callback
    from handlers.keyword_handlers import (
        handle_keyword_form_callback,
        handle_keyword_detail_callback,
        handle_keyword_preview_callback,
        handle_keyword_delete_callback,
        handle_keyword_confirm_delete_callback
    )
    from handlers.broadcast_handlers import (
        handle_broadcast_form_callback,
        handle_broadcast_detail_callback,
        handle_broadcast_preview_callback,
        handle_broadcast_delete_callback,
        handle_broadcast_confirm_delete_callback
    )
    
    # 注册设置相关回调前缀
    callback_handler.register("settings_", handle_settings_callback)
    callback_handler.register("auto_delete_", handle_settings_callback)
    callback_handler.register("switch_toggle_", handle_settings_callback)
    callback_handler.register("stats_edit_", handle_settings_callback)
    
    # 注册关键词相关回调前缀
    callback_handler.register("kwform_", handle_keyword_form_callback)
    callback_handler.register("keyword_detail_", handle_keyword_detail_callback)
    callback_handler.register("keyword_preview_", handle_keyword_preview_callback)
    callback_handler.register("keyword_delete_", handle_keyword_delete_callback)
    callback_handler.register("keyword_confirm_delete_", handle_keyword_confirm_delete_callback)
    callback_handler.register("keyword_list_page_", handle_keyword_detail_callback)  # 假设列表分页由detail处理
    
    # 注册轮播消息相关回调前缀
    callback_handler.register("bcform_", handle_broadcast_form_callback)
    callback_handler.register("broadcast_detail_", handle_broadcast_detail_callback)
    callback_handler.register("bc_preview_", handle_broadcast_preview_callback)
    callback_handler.register("bc_delete_", handle_broadcast_delete_callback)
    callback_handler.register("bc_confirm_delete_", handle_broadcast_confirm_delete_callback)
    
    logger.info("所有处理函数注册完成")

__all__ = ['register_all_handlers']
