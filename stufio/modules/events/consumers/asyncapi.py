from typing import Any, Dict, Union, Optional, Type, get_type_hints, get_args, get_origin
import re
import inspect
import logging
from faststream.asyncapi.schema import Channel, ChannelBinding, CorrelationId, Message, Operation
from faststream.asyncapi.schema.bindings import kafka
from faststream.asyncapi.utils import resolve_payloads
from faststream.kafka.subscriber.asyncapi import AsyncAPIDefaultSubscriber
from faststream.kafka.broker.registrator import KafkaRegistrator
from faststream.broker.types import Filter, MsgType
from faststream.broker.utils import default_filter
from ..schemas.base import MessageHeader
from ..schemas.event_definition import EventDefinition
from stufio.core.config import get_settings
from faststream.asyncapi.generate import get_app_schema as original_get_app_schema
from ..services.publisher_registry import get_publisher_channels
from .asyncapi_patches import register_channel_info

settings = get_settings()
logger = logging.getLogger(__name__)


def extract_payload_class(func):
    """Extract payload class from a function's type annotations."""
    try:
        # Get type hints from the function
        hints = get_type_hints(func)

        # Get the first parameter (usually 'event')
        signature = inspect.signature(func)
        if not signature.parameters:
            return None

        # Get the first parameter name
        first_param_name = next(iter(signature.parameters))
        if (first_param_name not in hints):
            return None

        # Get the parameter type
        param_type = hints[first_param_name]

        # Check if it's a generic type (like BaseEventMessage[SomePayload])
        origin = get_origin(param_type)
        if origin is None:
            return None

        # Get the arguments of the generic type
        args = get_args(param_type)
        if not args:
            return None

        # The last argument is typically the payload type
        payload_class = args[-1]
        logger.debug(f"Extracted payload class: {payload_class}")
        return payload_class
    except Exception as e:
        logger.warning(f"Error extracting payload class: {e}")
        return None


# Create a custom decorator wrapper around the broker's subscriber method
def stufio_subscriber(
    *topics: str,
    broker: KafkaRegistrator = None,
    channel: Optional[str] = None,
    group_id: Optional[str] = None,
    filter: Filter = default_filter,
    filter_kwargs: Optional[Dict[str, Any]] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    include_in_schema: bool = True,
    event_id: Optional[str] = None,
    payload_class: Optional[Type] = None,  # Add payload_class parameter
    # Add explicit consumer config params
    max_poll_interval_ms: Optional[int] = None,
    session_timeout_ms: Optional[int] = None,
    heartbeat_interval_ms: Optional[int] = None,
    auto_commit_interval_ms: Optional[int] = None,
    auto_offset_reset: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """Enhanced subscriber decorator that properly sets channel names for AsyncAPI schema."""

    if broker is None:
        from stufio.modules.events.consumers.kafka import get_kafka_router
        broker = get_kafka_router()

    # Extract Kafka consumer settings from settings if not explicitly provided
    if max_poll_interval_ms is None:
        max_poll_interval_ms = getattr(settings, "events_KAFKA_MAX_POLL_INTERVAL_MS", 300000)
    if session_timeout_ms is None:
        session_timeout_ms = getattr(settings, "events_KAFKA_SESSION_TIMEOUT_MS", 60000)
    if heartbeat_interval_ms is None:
        heartbeat_interval_ms = getattr(settings, "events_KAFKA_HEARTBEAT_INTERVAL_MS", 20000)
    if auto_commit_interval_ms is None:
        auto_commit_interval_ms = getattr(settings, "events_KAFKA_AUTO_COMMIT_INTERVAL_MS", 5000)
    if auto_offset_reset is None:
        auto_offset_reset = getattr(settings, "events_KAFKA_AUTO_OFFSET_RESET", "earliest")

    # Set these parameters directly in kwargs
    kwargs["max_poll_interval_ms"] = max_poll_interval_ms
    kwargs["session_timeout_ms"] = session_timeout_ms
    kwargs["heartbeat_interval_ms"] = heartbeat_interval_ms
    kwargs["auto_commit_interval_ms"] = auto_commit_interval_ms
    kwargs["auto_offset_reset"] = auto_offset_reset

    # Set other broker parameters
    if title:
        kwargs["title"] = title
    if include_in_schema is not None:
        kwargs["include_in_schema"] = include_in_schema
    if description:
        kwargs["description"] = description
    if group_id:
        kwargs["group_id"] = group_id

    # Default filter function
    if filter is None:
        filter = default_filter

    # Check callability directly without using isinstance
    if not callable(filter):
        raise TypeError("Filter must be a callable function")

    # Ensure channel name is URL-template compatible
    if channel and not re.match(r'^[a-zA-Z0-9_\-\.\/\{\}]+$', channel):
        fixed_channel = re.sub(r'[^a-zA-Z0-9_\-\.\/\{\}]', '_', channel)
        logger.warning(f"Fixed invalid channel name: {channel} → {fixed_channel}")
        channel = fixed_channel

    # Create the original subscriber with individual parameters
    original_subscriber = broker.subscriber(*topics, filter=filter, **kwargs)

    def wrapper(func):
        # Apply the original decorator
        subscriber = original_subscriber(func)

        # Extract payload class from function if not provided
        extracted_payload_class = payload_class or extract_payload_class(func)

        # Register channel info with multiple keys for robustness
        subscriber_name = f"{','.join(topics)}:{func.__name__}"
        single_topic_name = f"{next(iter(topics), '')}:{func.__name__}"
        func_name = func.__name__

        # Channel info to register with payload class
        channel_info = {
            "channel": channel,
            "filter_kwargs": filter_kwargs,
            "title": event_id or title,
            "description": description,
            "payload_class": extracted_payload_class  # Store the payload class
        }

        # Create a more unique key if event_id is provided
        unique_key = f"{event_id}:{func.__name__}" if event_id else None
        if unique_key:
            register_channel_info(unique_key, channel_info)
            logger.debug(f"Registered channel info with event-specific key: {unique_key}")

        # Register with multiple keys to increase chances of finding it
        register_channel_info(subscriber_name, channel_info)
        register_channel_info(single_topic_name, channel_info)
        register_channel_info(func_name, channel_info)

        logger.debug(f"Registered channel info for {func.__name__}: {channel_info}")

        # Return the original subscriber unchanged
        return subscriber

    return wrapper


# Import the filter functions by function, not by direct import
def _get_create_header_filter():
    from stufio.modules.events.consumers.kafka import create_header_filter
    return create_header_filter


# Create a custom decorator wrapper around the broker's subscriber method
def stufio_event_subscriber(
    event: Type[EventDefinition],
    event_id: Optional[str] = None,
    group_id: Optional[str] = None,
    broker: KafkaRegistrator = None,
    channel: Optional[str] = None,
    filter: Filter = None,
    filter_kwargs: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
    include_in_schema: bool = True,
    title: Optional[str] = None,
    track_metrics: bool = True,  # Add parameter to control metrics tracking
    # Add explicit consumer config params
    max_poll_interval_ms: Optional[int] = None,
    session_timeout_ms: Optional[int] = None,
    heartbeat_interval_ms: Optional[int] = None,
    auto_commit_interval_ms: Optional[int] = None,
    auto_offset_reset: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """Enhanced subscriber decorator that properly sets channel names for AsyncAPI schema."""

    # Get topic from event
    topic_name = event.get_topic_name()
    topics = [topic_name]

    # Try to get payload class from event definition
    payload_class = None
    if hasattr(event, "get_payload_class"):
        payload_class = event.get_payload_class()

    # Add this new line to create a unique event identifier
    event_id = event_id or f"{event.__name__}"

    # Default filter based on entity_type and action
    if filter is None:
        create_header_filter = _get_create_header_filter()
        filter_bytes = {
            "entity_type": event.entity_type.encode('utf-8'),
            "action": event.action.encode('utf-8')
        }
        filter = create_header_filter(**filter_bytes)
        filter_kwargs = filter_bytes

    # Default description from event
    if description is None:
        description = f"{event.description or 'Event handler'} for {event.entity_type} {event.action}"

    # Create a decorator function that gets the function name
    def decorator(func):
        # Now we have access to the function name
        func_name = func.__name__
        nonlocal event, topics, broker, channel, group_id, title, description, include_in_schema, filter, filter_kwargs, event_id, payload_class, track_metrics

        if group_id is None:
            group_id = f"{settings.events_KAFKA_GROUP_ID}.{event.entity_type}.{event.action}"

        # Default title from event class
        if title is None:
            title = f"{event.entity_type.title()}{event.action.title()}Event:{func_name}"

        # Create a valid URI template channel name from event details
        if not channel:
            entity_type = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', event.entity_type)
            action = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', event.action)
            channel = f"{settings.events_ASYNCAPI_PREFIX}.{entity_type}.{action}"

        logger.debug(f"Using channel name: {channel} for event {event.__name__} function {func_name}")

        # Apply metrics tracking if enabled
        if track_metrics:
            from ..decorators.metrics import track_handler_metrics
            # Extract module name from event definition
            module_name = event.entity_type
            # Apply metrics tracking decorator
            func = track_handler_metrics(module_name=module_name)(func)
            logger.debug(f"Applied metrics tracking decorator to {func_name}")

        # Important - DON'T wrap the handler function, just pass it directly
        # This preserves the argument signatures expected by FastStream
        return stufio_subscriber(
            *topics,
            broker=broker,
            channel=channel,
            filter=filter,
            filter_kwargs=filter_kwargs,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
            event_id=event_id,
            group_id=group_id,
            payload_class=payload_class,
            max_poll_interval_ms=max_poll_interval_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            auto_commit_interval_ms=auto_commit_interval_ms,
            auto_offset_reset=auto_offset_reset,
            **kwargs
        )(func)  # Apply directly to the original function

    return decorator
