"""
核心模块初始化文件
"""
from core.callback_handler import CallbackHandler
from core.error_handler import ErrorHandler
from core.state_machine import (
    State, 
    StateMachine, 
    FormStateMachine, 
    StateMachineManager
)

# 将TelegramBot的导入移到最后，并改为函数导入方式
__all__ = [
    'TelegramBot',
    'CallbackHandler',
    'ErrorHandler',
    'State',
    'StateMachine',
    'FormStateMachine',
    'StateMachineManager',
    'get_telegram_bot'
]

# 提供获取TelegramBot的函数
def get_telegram_bot():
    from core.telegram_bot import TelegramBot
    return TelegramBot

# 将TelegramBot类的导入移到模块末尾，避免循环导入
# 这样当其他模块导入TelegramBot时，会延迟到实际使用时才加载
def TelegramBot():
    from core.telegram_bot import TelegramBot
    return TelegramBot
