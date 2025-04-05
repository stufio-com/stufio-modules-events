"""
Base events used across the Stufio framework.

Other modules should import and extend these event definitions.
"""
from .schemas.event_definition import EventDefinition
from .schemas.payloads import (
    UserCreatedPayload, UserUpdatedPayload, UserDeletedPayload,
    UserLoginPayload, UserLogoutPayload, TokenCreatedPayload,
    TokenVerifiedPayload, TokenRevokedPayload, SystemStartupPayload,
    SystemShutdownPayload, SystemErrorPayload
)

# User-related events using the decorator pattern
class UserCreatedEvent(EventDefinition[UserCreatedPayload]):
    """Event triggered when a new user is created."""
    name = "user.created"
    entity_type = "user"
    action = "created"
    require_actor = False
    require_entity = True
    description = "Triggered when a new user is created"
    payload_example = {
        "after": {
            "user_id": "550e8400-e29b-41d4-a716-446655440000",
            "email": "user@example.com",
            "username": "username",
            "roles": ["user"],
            "is_active": True,
        }
    }


class UserUpdatedEvent(EventDefinition[UserUpdatedPayload]):
    """Event triggered when a user is updated."""
    name = ("user.updated",)
    entity_type = ("user",)
    action = ("updated",)
    require_actor = (True,)
    require_entity = (True,)
    description = ("Triggered when a user's information is updated",)
    payload_example = {
        "updated_fields": ["email", "username"],
        "before": {"email": "old@example.com"},
        "after": {"email": "new@example.com"},
    }


class UserDeletedEvent(EventDefinition[UserDeletedPayload]):
    """Event triggered when a user is deleted."""
    name = "user.deleted"
    entity_type = "user"
    action = "deleted"
    require_actor = True
    require_entity = True
    description = "Triggered when a user is deleted"
    payload_example = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000"
    }


class UserLoginEvent(EventDefinition[UserLoginPayload]):
    """Event triggered when a user logs in."""
    name = "user.login"
    entity_type = "user"
    action = "login"
    require_entity = False
    description = "Triggered when a user logs in"
    payload_example = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0...",
        "success": True
    }


class UserLogoutEvent(EventDefinition[UserLogoutPayload]):
    """Event triggered when a user logs out."""
    name = "user.logout"
    entity_type = "user"
    action = "logout"
    require_actor = True
    require_entity = False
    description = "Triggered when a user logs out"
    payload_example = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "session_id": "session123"
    }


class TokenCreatedEvent(EventDefinition[TokenCreatedPayload]):
    """Event triggered when a new token is created."""
    name="token.created",
    entity_type="token",
    action="created",
    require_actor=True,
    require_entity=True,
    description="Triggered when a new token is created",
    payload_class=TokenCreatedPayload,
    payload_example={
        "token_id": "token123",
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "token_type": "access",
        "expires_at": "2025-04-01T12:00:00Z"
    }

class TokenVerifiedEvent(EventDefinition[TokenVerifiedPayload]):
    """Event triggered when a token is verified."""
    name="token.verified",
    entity_type="token",
    action="verified",
    description="Triggered when a token is successfully verified",
    payload_example={
        "token_id": "token123",
        "user_id": "550e8400-e29b-41d4-a716-446655440000"
    }

class TokenRevokedEvent(EventDefinition[TokenRevokedPayload]):
    """Event triggered when a token is revoked."""
    name="token.revoked",
    entity_type="token",
    action="revoked",
    require_actor=True,
    require_entity=True,
    description="Triggered when a token is revoked",
    payload_example={
        "token_id": "token123",
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "reason": "user_logout"
    }


class SystemStartupEvent(EventDefinition[SystemStartupPayload]):
    """Event triggered when the system starts up."""
    name="system.startup",
    entity_type="system",
    action="startup",
    require_actor=False,
    require_entity=False,
    description="Triggered when the system starts up",
    payload_example={
        "version": "1.0.0",
        "environment": "production",
        "modules": ["users", "events", "activity"]
    }
    
class SystemShutdownEvent(EventDefinition[SystemShutdownPayload]):
    """Event triggered when the system shuts down."""
    name="system.shutdown",
    entity_type="system",
    action="shutdown",
    require_actor=False,
    require_entity=False,
    description="Triggered when the system shuts down",
    payload_example={
        "version": "1.0.0",
        "environment": "production",
        "shutdown_time": "2025-04-01T12:00:00Z"
    }

class SystemErrorEvent(EventDefinition[SystemErrorPayload]):
    """Event triggered when a system error occurs."""
    name="system.error",
    entity_type="system",
    action="error",
    require_actor=False,
    require_entity=False,
    description="Triggered when a system error occurs",
    payload_example={
        "error_code": "500",
        "error_message": "Internal Server Error",
        "stack_trace": "Traceback (most recent call last)..."
    }
