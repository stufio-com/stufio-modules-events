"""
Base events used across the Stufio framework.

Other modules should import and extend these event definitions.
"""
from .schemas import EventDefinition

# User-related events
class UserCreatedEvent(EventDefinition):
    name = "user.created"
    entity_type = "user"
    action = "created"
    require_actor = False
    require_entity = True
    description = "Triggered when a new user is created"
    payload_example = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "email": "user@example.com",
        "username": "username",
        "roles": ["user"],
        "is_active": True
    }

class UserUpdatedEvent(EventDefinition):
    name = "user.updated"
    entity_type = "user"
    action = "updated"
    require_actor = True
    require_entity = True
    description = "Triggered when a user's information is updated"
    payload_example = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "updated_fields": ["email", "username"],
        "before": {"email": "old@example.com"},
        "after": {"email": "new@example.com"}
    }

class UserDeletedEvent(EventDefinition):
    name = "user.deleted"
    entity_type = "user"
    action = "deleted"
    require_actor = True
    require_entity = True
    description = "Triggered when a user is deleted"
    payload_example = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000"
    }

class UserLoginEvent(EventDefinition):
    name = "user.login"
    entity_type = "user"
    action = "login"
    require_entity = True
    description = "Triggered when a user logs in"
    payload_example = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0...",
        "success": True
    }

class UserLogoutEvent(EventDefinition):
    name = "user.logout"
    entity_type = "user"
    require_actor = True
    require_entity = True
    action = "logout"
    description = "Triggered when a user logs out"
    payload_example = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "session_id": "session123"
    }

# Token-related events
class TokenCreatedEvent(EventDefinition):
    name = "token.created"
    entity_type = "token"
    action = "created"
    require_actor = True
    require_entity = True
    description = "Triggered when a new token is created"
    payload_example = {
        "token_id": "token123",
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "token_type": "access",
        "expires_at": "2025-04-01T12:00:00Z"
    }

class TokenVerifiedEvent(EventDefinition):
    name = "token.verified"
    entity_type = "token"
    action = "verified"
    description = "Triggered when a token is successfully verified"
    payload_example = {
        "token_id": "token123",
        "user_id": "550e8400-e29b-41d4-a716-446655440000"
    }

class TokenRevokedEvent(EventDefinition):
    name = "token.revoked"
    entity_type = "token"
    action = "revoked"
    require_actor = True
    require_entity = True
    description = "Triggered when a token is revoked"
    payload_example = {
        "token_id": "token123",
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "reason": "user_logout"
    }

# System events
class SystemStartupEvent(EventDefinition):
    name = "system.startup"
    entity_type = "system"
    action = "startup"
    require_actor = False
    require_entity = False
    description = "Triggered when the system starts up"
    payload_example = {
        "version": "1.0.0",
        "environment": "production",
        "modules": ["users", "events", "activity"]
    }

class SystemShutdownEvent(EventDefinition):
    name = "system.shutdown"
    entity_type = "system"
    action = "shutdown"
    require_actor = False
    require_entity = False
    description = "Triggered when the system shuts down"
    payload_example = {
        "reason": "scheduled_maintenance"
    }

class SystemErrorEvent(EventDefinition):
    name = "system.error"
    entity_type = "system"
    action = "error"
    require_actor = False
    require_entity = False
    description = "Triggered when a system error occurs"
    payload_example = {
        "error_type": "database_connection",
        "error_message": "Connection refused",
        "stacktrace": "...",
        "severity": "critical"
    }

# All events for discovery
ALL_EVENTS = [
    UserCreatedEvent,
    UserUpdatedEvent,
    UserDeletedEvent,
    UserLoginEvent,
    UserLogoutEvent,
    TokenCreatedEvent,
    TokenVerifiedEvent,
    TokenRevokedEvent,
    SystemStartupEvent,
    SystemShutdownEvent,
    SystemErrorEvent
]
