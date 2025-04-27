"""Event consumer modules."""

# Import and apply patching immediately
from .asyncapi_patches import patched_get_schema

# Make sure the patch is always applied
from faststream.kafka.subscriber.asyncapi import AsyncAPIDefaultSubscriber
AsyncAPIDefaultSubscriber.get_schema = patched_get_schema

# Then define getters for lazy loading
def get_kafka_router():
    """Get the Kafka router singleton."""
    from .kafka import get_kafka_router as _get_kafka_router
    return _get_kafka_router()

def get_kafka_broker():
    """Get the Kafka broker singleton."""
    from .kafka import get_kafka_broker as _get_kafka_broker
    return _get_kafka_broker()

def get_patched_app_schema():
    """Get the patched schema generator function."""
    from .asyncapi_patches import get_patched_app_schema as _get_patched_app_schema
    return _get_patched_app_schema

# Define what's available for import
__all__ = [
    "get_kafka_router", 
    "get_kafka_broker", 
    "get_patched_app_schema"
]

# Register all consumers at initialization time
def _register_module_consumers():
    """Register all consumers from the events module."""
    # Import lazily to avoid circular imports
    from .kafka import get_kafka_router
    # Import the consumer registry
    from ..services.consumer_registry import consumer_registry

    # Register with the registry
    consumer_registry.register_router("events", "kafka", get_kafka_router())
    consumer_registry.register_broker("events", "kafka", get_kafka_broker())

# Register consumers
_register_module_consumers()


from stufio.core.config import get_settings

settings = get_settings()

if settings.events_APP_CONSUME_ROUTES:
    # Lazy load modules
    from . import clickhouse_event_log
