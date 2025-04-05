from typing import Dict, Any, Optional, List
from pydantic import Field
from .base import BaseEventPayload


# User-related payloads
class UserPayload(BaseEventPayload):
    """Common user data structure."""
    user_id: str
    email: Optional[str] = None
    username: Optional[str] = None
    roles: Optional[List[str]] = None
    is_active: Optional[bool] = None


class UserCreatedPayload(BaseEventPayload):
    """Payload for user.created events."""
    after: UserPayload

class UserUpdatedPayload(BaseEventPayload):
    """Payload for user.updated events."""
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    updated_fields: List[str]

class UserDeletedPayload(BaseEventPayload):
    """Payload for user.deleted events."""
    user_id: str

class UserLoginPayload(BaseEventPayload):
    """Payload for user.login events."""
    user_id: str
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    success: bool = True

class UserLogoutPayload(BaseEventPayload):
    """Payload for user.logout events."""
    user_id: str
    session_id: Optional[str] = None


# Token-related payloads
class TokenPayload(BaseEventPayload):
    """Common token data structure."""
    token_id: str
    user_id: str
    token_type: Optional[str] = None
    expires_at: Optional[str] = None


class TokenCreatedPayload(BaseEventPayload):
    """Payload for token.created events."""
    after: TokenPayload

class TokenVerifiedPayload(BaseEventPayload):
    """Payload for token.verified events."""
    token_id: str
    user_id: str

class TokenRevokedPayload(BaseEventPayload):
    """Payload for token.revoked events."""
    token_id: str
    user_id: str
    reason: Optional[str] = None

# System-related payloads
class SystemStartupPayload(BaseEventPayload):
    """Payload for system.startup events."""
    version: str
    environment: str
    modules: List[str]

class SystemShutdownPayload(BaseEventPayload):
    """Payload for system.shutdown events."""
    reason: Optional[str] = None

class SystemErrorPayload(BaseEventPayload):
    """Payload for system.error events."""
    error_type: str
    error_message: str
    stacktrace: Optional[str] = None
    severity: str
