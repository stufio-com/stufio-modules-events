from typing import Any, Dict, Union, Optional, Type, get_type_hints, get_args, get_origin
import re
import inspect
from faststream.asyncapi.schema import Channel, ChannelBinding, CorrelationId, Message, Operation
from faststream.asyncapi.schema.bindings import kafka
from faststream.asyncapi.utils import resolve_payloads
from faststream.kafka.subscriber.asyncapi import AsyncAPIDefaultSubscriber
from faststream.kafka.broker.registrator import KafkaRegistrator 
from faststream.broker.types import Filter, MsgType
from faststream.broker.utils import default_filter
from httpx import get
from stufio.modules.events.schemas.base import MessageHeader
from stufio.modules.events.schemas.event_definition import EventDefinition
from stufio.core.config import get_settings
from faststream.asyncapi.generate import get_app_schema as original_get_app_schema

settings = get_settings()


# Store global registry of custom channel information
channel_registry: Dict[str, Dict[str, Any]] = {}

import logging
logger = logging.getLogger(__name__)

# import debugpy

# # # Allow connections to the debugger from any host
# debugpy.listen(("0.0.0.0", 5678))
# logger.error("Waiting for debugger to attach...")
# debugpy.wait_for_client()

def register_channel_info(subscriber_name: str, channel_info: Dict[str, Any]) -> None:
    """Register channel information for a subscriber"""
    channel_registry[subscriber_name] = channel_info

# Patch AsyncAPIDefaultSubscriber class to use our channel info
original_get_schema = AsyncAPIDefaultSubscriber.get_schema


def get_channel_info(self, subscriber_names) -> Union[Dict[str, Any], None]:
    # Log what we're looking for
    logger.debug(f"Looking for subscriber in registry: {subscriber_names}")
    logger.debug(f"Available channels in registry: {list(channel_registry.keys())}")

    # Try to find the subscriber in our registry
    found_info = None
    for name in subscriber_names:
        if name in channel_registry:
            found_info = channel_registry[name]
            logger.debug(f"Found channel info for {name}")
            break

    # If we found custom channel info, use it
    if found_info:
        info = found_info
        channels = {}

        msg_payloads = []
        # Use explicit payload class if provided
        payload_class = info.get("payload_class")
        payloads = self.get_payloads()
        if payload_class:
            logger.debug(f"Using explicit payload class: {payload_class}")
            for payload_obj in payloads:
                if payload_class.__name__ == payload_obj[0].get("title") or f"[{payload_class.__name__}]" in payload_obj[0].get("title"):
                    logger.debug(f"Found matching payload class: {payload_obj}")
                    msg_payloads.append(payload_obj)

        if len(msg_payloads) == 0:
            logger.warning(f"Payload class {payload_class} not found in registry")
            msg_payloads = payloads

        payload = resolve_payloads(msg_payloads)

        # Create proper uri-template compatible channel name
        channel_name = info.get("channel")
        if channel_name:
            # Normalize channel name
            # if not channel_name.startswith('/'):
            #     channel_name = f"/{channel_name}"

            # Replace any invalid URI template characters
            channel_name = re.sub(r'[^a-zA-Z0-9_\-\.\/\{\}]', '_', channel_name)

            # Create the channel with proper schema using our payload
            channels[channel_name] = Channel(
                description=info.get("description", self.description),
                subscribe=Operation(
                    message=Message(
                        title=info.get(
                            "title", f"{self.call_name}:Message"
                        ),
                        payload=payload,  # Use our determined payload
                        correlationId=CorrelationId(
                            location="$message.header#/correlation_id"
                        ),
                        # Add message headers - uncomment and fix these
                        headers={
                            "entity_type": MessageHeader(
                                location="$message.header#/entity_type",
                                description="Entity type of the message",
                            ),
                            "action": MessageHeader(
                                location="$message.header#/action",
                                description="Action performed on the entity",
                            ),
                        },
                    ),
                ),
                bindings=ChannelBinding(
                    kafka=kafka.ChannelBinding(
                        topic=next(iter(self.topics), ""),
                        # Include filter metadata for documentation
                        x_filter=info.get("filter_kwargs"),
                    ),
                ),
            )
            logger.debug(f"Created custom channel schema for {channel_name}")
            return channels

    # Fall back to creating a valid channel name even without registry info
    channels = {}
    handler_name = self.title_ or f"{next(iter(self.topics), '')}:{self.call_name}"

    # Fix: Break into separate steps to avoid backslash in f-string
    processed_name = handler_name.lower()
    processed_name = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', processed_name)
    processed_name = processed_name.replace(':', '/')
    valid_channel_name = f"{processed_name}"

    # Create a channel with proper URI template format
    channels[valid_channel_name] = Channel(
        description=self.description,
        subscribe=Operation(
            message=Message(
                title=f"{handler_name}:Message",
                payload=resolve_payloads(self.get_payloads()),
                correlationId=CorrelationId(location="$message.header#/correlation_id"),
                # Add message headers - uncomment and fix these
                headers={
                    "entity_type": MessageHeader(
                        location="$message.header#/entity_type",
                        description="Entity type of the message",
                    ),
                    "action": MessageHeader(
                        location="$message.header#/action",
                        description="Action performed on the entity",
                    ),
                },
            ),
        ),
        bindings=ChannelBinding(
            kafka=kafka.ChannelBinding(
                topic=next(iter(self.topics), ""),
            ),
        ),
    )

    return channels


# Modify the patched_get_schema function to better handle channel naming
def patched_get_schema(self):
    """Patched get_schema method that uses our channel registry"""

    # # Try different subscriber name formats to match registry entries
    # subscriber_names = [
    #     f"{','.join(self.topics)}:{self.call_name}",  # Standard format
    #     f"{next(iter(self.topics), '')}:{self.call_name}",  # Single topic format
    #     self.call_name,  # Just function name
    # ]
    

    channels = {}
    
    if self.calls:
        for call in self.calls:
            call_subscriber_names = []
            call_name = getattr(call, "call_name", "")
            if call_name:
                call_subscriber_names.append(f"{next(iter(self.topics), '')}:{call_name}")
                call_subscriber_names.append(f"{','.join(self.topics)}:{call_name}")
                call_subscriber_names.append(call_name)

                channel_info = get_channel_info(self, call_subscriber_names)
                if channel_info:
                    for channel_name, channel in channel_info.items():
                        # Add the channel to the schema
                        channels[channel_name] = channel
                        logger.debug(f"Added custom channel schema for {channel_name}")

    return channels

# Apply our patch
AsyncAPIDefaultSubscriber.get_schema = patched_get_schema


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
        if first_param_name not in hints:
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
    **kwargs: Any,
) -> Any:
    """Enhanced subscriber decorator that properly sets channel names for AsyncAPI schema."""

    if broker is None:
        from stufio.modules.events.consumers.kafka import get_kafka_router
        broker = get_kafka_router()

    # Instead of setting a consumer_config dictionary, set individual parameters
    # Extract Kafka consumer settings from settings
    max_poll_interval_ms = getattr(settings, "events_KAFKA_MAX_POLL_INTERVAL_MS", 300000)
    session_timeout_ms = getattr(settings, "events_KAFKA_SESSION_TIMEOUT_MS", 60000)
    heartbeat_interval_ms = getattr(settings, "events_KAFKA_HEARTBEAT_INTERVAL_MS", 20000)
    auto_commit_interval_ms = getattr(settings, "events_KAFKA_AUTO_COMMIT_INTERVAL_MS", 5000)
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
        unique_key = f"{event_id}:{func.__name__}"
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
        nonlocal event, topics, broker, channel, group_id, title, description, include_in_schema, filter, filter_kwargs, event_id, payload_class

        if group_id is None:
            # group_id = f"{settings.events_KAFKA_GROUP_ID}.{event.entity_type}.{event.action}:{func_name}"
            # Use a more generic group ID
            group_id = f"{settings.events_KAFKA_GROUP_ID}.{event.entity_type}.{event.action}" 

        # Default title from event class
        if title is None:
            title = f"{event.entity_type.title()}{event.action.title()}Event:{func_name}"

        # Create a valid URI template channel name from event details
        if not channel:
            # Create a valid URI template (alphanumeric, underscores, hyphens, periods, and forward slashes)
            entity_type = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', event.entity_type)
            action = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', event.action)

            # Use the function name that we captured
            channel = f"{settings.events_ASYNCAPI_PREFIX}.{entity_type}.{action}" #/{func_name}"

        logger.debug(f"Using channel name: {channel} for event {event.__name__} function {func_name}")

        # Now call stufio_subscriber with the constructed channel
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
            payload_class=payload_class,  # Pass the payload class
            **kwargs
        )(func)  # Immediately apply the returned decorator to func

    return decorator


def get_patched_app_schema(router):
    """
    Get AsyncAPI schema with our custom patches applied.
    
    This ensures our channel registry is used for schema generation.
    """
    # Force applying our patch if not already applied
    if AsyncAPIDefaultSubscriber.get_schema != patched_get_schema:
        AsyncAPIDefaultSubscriber.get_schema = patched_get_schema
        
    # Log registered channels for debugging
    logger.info(f"Registered channels: {list(channel_registry.keys())}")
    
    # Now generate the schema using the original function
    return original_get_app_schema(router)
