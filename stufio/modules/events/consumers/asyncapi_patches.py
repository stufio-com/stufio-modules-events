import re
from typing import Any, Dict, Union
from faststream.asyncapi.generate import get_app_schema as original_get_app_schema
from faststream.asyncapi.utils import resolve_payloads
from faststream.asyncapi.schema import (
    Channel,
    ChannelBinding,
    CorrelationId,
    Message,
    Operation,
)
from faststream.asyncapi.schema.bindings import kafka
from faststream.asyncapi.utils import resolve_payloads
from faststream.kafka.subscriber.asyncapi import AsyncAPIDefaultSubscriber


import logging

from stufio.modules.events.schemas.base import MessageHeader
from stufio.modules.events.services.publisher_registry import get_publisher_channels
logger = logging.getLogger(__name__)


# Add a global registry for top-level schemas
top_level_schemas_registry: Dict[str, Any] = {}

# Store global registry of custom channel information
channel_registry: Dict[str, Dict[str, Any]] = {}


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
                if payload_class.__name__ == payload_obj[0].get(
                    "title"
                ) or f"[{payload_class.__name__}]" in payload_obj[0].get("title"):
                    logger.debug(f"Found matching payload class: {payload_obj}")
                    msg_payloads.append(payload_obj)

        if len(msg_payloads) == 0:
            logger.warning(f"Payload class {payload_class} not found in registry")
            msg_payloads = payloads

        payload = resolve_payloads(msg_payloads)

        # Create proper uri-template compatible channel name
        channel_name = info.get("channel")
        if channel_name:
            # Replace any invalid URI template characters
            channel_name = re.sub(r"[^a-zA-Z0-9_\-\.\/\{\}]", "_", channel_name)

            # Create the channel with proper schema using our payload
            channels[channel_name] = Channel(
                description=info.get("description", self.description),
                subscribe=Operation(
                    message=Message(
                        title=info.get("title", f"{self.call_name}:Message"),
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
    processed_name = re.sub(r"[^a-zA-Z0-9_\-\.]", "_", processed_name)
    processed_name = processed_name.replace(":", "/")
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

    channels = {}

    if self.calls:
        for call in self.calls:
            call_subscriber_names = []
            call_name = getattr(call, "call_name", "")
            if call_name:
                call_subscriber_names.append(
                    f"{next(iter(self.topics), '')}:{call_name}"
                )
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

# Then modify the get_patched_app_schema function to include these schemas
def get_patched_app_schema(router):
    """Get AsyncAPI schema with our custom patches applied."""
    # Force applying our patch if not already applied
    if AsyncAPIDefaultSubscriber.get_schema != patched_get_schema:
        AsyncAPIDefaultSubscriber.get_schema = patched_get_schema

    # Log registered channels for debugging
    logger.info(f"Registered channels: {list(channel_registry.keys())}")

    schema = original_get_app_schema(router)

    # Add publisher channels
    publisher_channels = get_publisher_channels()

    # Get the message components that were created
    message_components = getattr(get_patched_app_schema, "message_components", {})

    # Merge channels
    if hasattr(schema, "channels"):
        schema.channels.update(publisher_channels)

    # Make sure components exist
    if not hasattr(schema, "components"):
        from faststream.asyncapi.schema import Components

        schema.components = Components()

    # Make sure components.messages exists
    if not hasattr(schema.components, "messages"):
        schema.components.messages = {}

    # Add message components to components.messages
    schema.components.messages.update(message_components)

    # Add publisher channels to the schema
    # publisher_channels = get_publisher_channels()

    # Merge channels
    if hasattr(schema, "channels"):
        schema.channels.update(publisher_channels)

    # Add our top-level schemas to the components.schemas section
    if not hasattr(schema, "components"):
        from faststream.asyncapi.schema import Components

        schema.components = Components()

    if not hasattr(schema.components, "schemas"):
        schema.components.schemas = {}

    # Merge collected top-level schemas
    schema.components.schemas.update(top_level_schemas_registry)

    logger.warning(f"Generated AsyncAPI schema with {len(schema.channels)} channels")
    return schema
