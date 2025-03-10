"""
数据模型定义
"""
from enum import Enum
from typing import Dict, Any, Optional, List, Union
from datetime import datetime

class UserRole(Enum):
    """用户角色枚举类"""
    USER = "user"
    ADMIN = "admin"
    SUPERADMIN = "superadmin"

class GroupPermission(Enum):
    """群组权限枚举类"""
    KEYWORDS = "keywords"
    STATS = "stats"
    BROADCAST = "broadcast"

class User:
    """用户模型"""
    def __init__(
        self,
        user_id: int,
        role: str = UserRole.USER.value,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        is_banned: bool = False,
        total_messages: int = 0,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self.user_id = user_id
        self.role = role
        self.username = username
        self.first_name = first_name
        self.last_name = last_name
        self.is_banned = is_banned
        self.total_messages = total_messages
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
        self.extra_data = kwargs
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'User':
        """从字典创建用户对象"""
        return cls(
            user_id=data.get('user_id'),
            role=data.get('role', UserRole.USER.value),
            username=data.get('username'),
            first_name=data.get('first_name'),
            last_name=data.get('last_name'),
            is_banned=data.get('is_banned', False),
            total_messages=data.get('total_messages', 0),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            **{k: v for k, v in data.items() if k not in [
                'user_id', 'role', 'username', 'first_name', 'last_name',
                'is_banned', 'total_messages', 'created_at', 'updated_at'
            ]}
        )
        
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            'user_id': self.user_id,
            'role': self.role,
            'username': self.username,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'is_banned': self.is_banned,
            'total_messages': self.total_messages,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
        # 添加额外数据
        result.update(self.extra_data)
        return result
        
    @property
    def full_name(self) -> str:
        """获取完整名称"""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.first_name or self.username or f"用户{self.user_id}"
        
    def is_admin(self) -> bool:
        """检查是否为管理员"""
        return self.role in [UserRole.ADMIN.value, UserRole.SUPERADMIN.value]
        
    def is_superadmin(self) -> bool:
        """检查是否为超级管理员"""
        return self.role == UserRole.SUPERADMIN.value

class Group:
    """群组模型"""
    def __init__(
        self,
        group_id: int,
        name: Optional[str] = None,
        permissions: Optional[List[str]] = None,
        settings: Optional[Dict[str, Any]] = None,
        feature_switches: Optional[Dict[str, bool]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self.group_id = group_id
        self.name = name
        self.permissions = permissions or []
        self.settings = settings or {}
        self.feature_switches = feature_switches or {
            'keywords': True,
            'stats': True,
            'broadcast': True
        }
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
        self.extra_data = kwargs
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Group':
        """从字典创建群组对象"""
        return cls(
            group_id=data.get('group_id'),
            name=data.get('name'),
            permissions=data.get('permissions', []),
            settings=data.get('settings', {}),
            feature_switches=data.get('feature_switches', {
                'keywords': True,
                'stats': True,
                'broadcast': True
            }),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            **{k: v for k, v in data.items() if k not in [
                'group_id', 'name', 'permissions', 'settings',
                'feature_switches', 'created_at', 'updated_at'
            ]}
        )
        
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            'group_id': self.group_id,
            'name': self.name,
            'permissions': self.permissions,
            'settings': self.settings,
            'feature_switches': self.feature_switches,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
        # 添加额外数据
        result.update(self.extra_data)
        return result
        
    def has_permission(self, permission: Union[GroupPermission, str]) -> bool:
        """检查是否有指定权限"""
        if isinstance(permission, GroupPermission):
            perm_value = permission.value
        else:
            perm_value = permission
            
        # 首先检查权限列表
        if perm_value not in self.permissions:
            return False
            
        # 然后检查功能开关
        return self.feature_switches.get(perm_value, True)
        
    def update_settings(self, new_settings: Dict[str, Any]) -> None:
        """更新设置"""
        self.settings.update(new_settings)
        self.updated_at = datetime.now()
        
    def toggle_feature(self, feature: str, enabled: bool) -> None:
        """切换功能开关"""
        self.feature_switches[feature] = enabled
        self.updated_at = datetime.now()

class Keyword:
    """关键词模型"""
    def __init__(
        self,
        group_id: int,
        pattern: str,
        match_type: str = 'exact',  # 'exact' 或 'regex'
        response: Optional[str] = None,
        media: Optional[Dict[str, Any]] = None,
        buttons: Optional[List[Dict[str, str]]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self.group_id = group_id
        self.pattern = pattern
        self.match_type = match_type
        self.response = response or ''
        self.media = media
        self.buttons = buttons or []
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
        self.extra_data = kwargs
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Keyword':
        """从字典创建关键词对象"""
        return cls(
            group_id=data.get('group_id'),
            pattern=data.get('pattern'),
            match_type=data.get('match_type', 'exact'),
            response=data.get('response', ''),
            media=data.get('media'),
            buttons=data.get('buttons', []),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            **{k: v for k, v in data.items() if k not in [
                'group_id', 'pattern', 'match_type', 'response',
                'media', 'buttons', 'created_at', 'updated_at'
            ]}
        )
        
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            'group_id': self.group_id,
            'pattern': self.pattern,
            'match_type': self.match_type,
            'response': self.response,
            'media': self.media,
            'buttons': self.buttons,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
        # 添加额外数据
        result.update(self.extra_data)
        return result

class Broadcast:
    """轮播消息模型"""
    def __init__(
        self,
        group_id: int,
        start_time: datetime,
        end_time: datetime,
        interval: int,  # 秒
        text: Optional[str] = None,
        media: Optional[Dict[str, Any]] = None,
        buttons: Optional[List[Dict[str, str]]] = None,
        last_broadcast: Optional[datetime] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self.group_id = group_id
        self.start_time = start_time
        self.end_time = end_time
        self.interval = interval
        self.text = text or ''
        self.media = media
        self.buttons = buttons or []
        self.last_broadcast = last_broadcast
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
        self.extra_data = kwargs
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Broadcast':
        """从字典创建轮播消息对象"""
        return cls(
            group_id=data.get('group_id'),
            start_time=data.get('start_time'),
            end_time=data.get('end_time'),
            interval=data.get('interval'),
            text=data.get('text', ''),
            media=data.get('media'),
            buttons=data.get('buttons', []),
            last_broadcast=data.get('last_broadcast'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            **{k: v for k, v in data.items() if k not in [
                'group_id', 'start_time', 'end_time', 'interval',
                'text', 'media', 'buttons', 'last_broadcast',
                'created_at', 'updated_at'
            ]}
        )
        
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            'group_id': self.group_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'interval': self.interval,
            'text': self.text,
            'media': self.media,
            'buttons': self.buttons,
            'last_broadcast': self.last_broadcast,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
        # 添加额外数据
        result.update(self.extra_data)
        return result
        
    def is_active(self, current_time: Optional[datetime] = None) -> bool:
        """检查是否处于活动状态"""
        if current_time is None:
            current_time = datetime.now()
        return self.start_time <= current_time <= self.end_time
        
    def is_due(self, current_time: Optional[datetime] = None) -> bool:
        """检查是否应该发送"""
        if current_time is None:
            current_time = datetime.now()
            
        # 检查是否在活动时间范围内
        if not self.is_active(current_time):
            return False
            
        # 检查是否已经过了间隔时间
        if self.last_broadcast is None:
            return True
            
        time_diff = (current_time - self.last_broadcast).total_seconds()
        return time_diff >= self.interval
        
    def update_last_broadcast(self, broadcast_time: Optional[datetime] = None) -> None:
        """更新最后发送时间"""
        self.last_broadcast = broadcast_time or datetime.now()
        self.updated_at = datetime.now()

class MessageStat:
    """消息统计模型"""
    def __init__(
        self,
        group_id: int,
        user_id: int,
        date: str,  # 格式: YYYY-MM-DD
        total_messages: int = 1,
        total_size: int = 0,
        media_type: Optional[str] = None,
        created_at: Optional[datetime] = None,
        **kwargs
    ):
        self.group_id = group_id
        self.user_id = user_id
        self.date = date
        self.total_messages = total_messages
        self.total_size = total_size
        self.media_type = media_type
        self.created_at = created_at or datetime.now()
        self.extra_data = kwargs
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MessageStat':
        """从字典创建消息统计对象"""
        return cls(
            group_id=data.get('group_id'),
            user_id=data.get('user_id'),
            date=data.get('date'),
            total_messages=data.get('total_messages', 1),
            total_size=data.get('total_size', 0),
            media_type=data.get('media_type'),
            created_at=data.get('created_at'),
            **{k: v for k, v in data.items() if k not in [
                'group_id', 'user_id', 'date', 'total_messages',
                'total_size', 'media_type', 'created_at'
            ]}
        )
        
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            'group_id': self.group_id,
            'user_id': self.user_id,
            'date': self.date,
            'total_messages': self.total_messages,
            'total_size': self.total_size,
            'media_type': self.media_type,
            'created_at': self.created_at
        }
        # 添加额外数据
        result.update(self.extra_data)
        return result
