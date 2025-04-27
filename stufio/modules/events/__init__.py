from .__version__ import __version__
from .config import EventsSettings
from .settings import settings_registry
from .module import EventsModule, KafkaModuleMixin, EventsModuleMixin
from .schemas.base import ActorType
from .schemas.event_definition import EventDefinition
from .schemas.payloads import BaseEventPayload, APIRequestPayload
from .schemas.handler import HandlerResponse, PublishEventInfo
from .schemas.messages import BaseEventMessage
from .helpers import publish_event, subscribe_to_event, register_module_events
from .consumers import get_kafka_router, get_kafka_broker
from .consumers.asyncapi import (
    stufio_subscriber,
    stufio_event_subscriber,
)
from .consumers.topic_initializer import initialize_kafka_topics
from .services.event_bus import get_event_bus
from .services.publisher_registry import register_publisher_channel
from .middleware.base import BaseStufioMiddleware
from .services.background_tasks import create_background_task, run_background_task

# Import events and register them
from .events import (
    UserCreatedEvent,
    UserLoginEvent,
    UserLogoutEvent,
    UserUpdatedEvent,
    UserDeletedEvent,
    UserPasswordResetEvent,
    TokenCreatedEvent,
    TokenRevokedEvent,
    TokenRefreshEvent,
    TokenVerifiedEvent,
    SystemStartupEvent,
    SystemShutdownEvent,
    SystemErrorEvent,
    APIRequestEvent,
)

__all__ = [
    # Module settings and configuration
    "EventsSettings",
    # Module class
    "EventsModule",
    "__version__",
    # Kafka components
    "KafkaModuleMixin",
    "EventsModuleMixin",
    "ActorType",
    "EventDefinition",
    "BaseEventPayload",
    "BaseEventMessage",
    "APIRequestPayload",
    # Exported helper functions
    "publish_event",
    "subscribe_to_event",
    "initialize_kafka_topics",
    # Background tasks
    "create_background_task",
    "run_background_task",
    # Middleware
    "BaseStufioMiddleware",
    # Kafka components
    "get_kafka_router",
    "get_kafka_broker",
    "get_event_bus",
    # Consumer components
    "HandlerResponse",
    "PublishEventInfo",
    # Stufio event subscriber decorator
    "stufio_subscriber",
    "stufio_event_subscriber",
    # New registration functions
    "register_module_events",
    "register_publisher_channel",
    # Exported events
    "UserCreatedEvent",
    "UserLoginEvent",
    "UserLogoutEvent",
    "UserUpdatedEvent",
    "UserDeletedEvent",
    "UserPasswordResetEvent",
    "UserProfileUpdateEvent",
    "UserAccountDeletionEvent",
    "TokenCreatedEvent",
    "TokenRevokedEvent",
    "TokenRefreshEvent",
    "TokenVerifiedEvent",
    "SystemStartupEvent",
    "SystemShutdownEvent",
    "SystemErrorEvent",
    "APIRequestEvent",
]
