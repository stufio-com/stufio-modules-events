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

class UserPasswordResetPayload(BaseEventPayload):
    """Payload for user.password_reset events."""
    user_id: str
    reset_token: Optional[str] = None
    reset_link: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    success: bool = True

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

class TokenRefreshPayload(BaseEventPayload):
    """Payload for token.verified events."""
    token_id: str
    user_id: str

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

class APIRequestPayload(BaseEventPayload):
    """Payload for API request events."""
    method: str = Field(..., description="HTTP method used")
    path: str = Field(..., description="Request path")
    status_code: int = Field(..., description="HTTP status code")
    query_params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers (sanitized)")
    error: Optional[Dict[str, Any]] = Field(None, description="Error details if status code >= 400")
    duration_ms: int = Field(..., description="Request processing time in milliseconds")
    response_size_bytes: Optional[int] = Field(None, description="Size of response in bytes")
    user_id: str = Field(..., description="User ID or 'anonymous'")
    user_agent: Optional[str] = Field(None, description="User agent string")
    remote_ip: Optional[str] = Field(None, description="Remote IP address")
