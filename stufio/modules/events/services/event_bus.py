from typing import Dict, Any, Optional, Callable, Type, Union
from datetime import datetime, timedelta
import asyncio
import logging
import time
from uuid import UUID as UUID4, uuid4
from faststream.kafka.broker import KafkaBroker
import httpx
from pydantic import BaseModel
from ..utils.context import TaskContext
from ..schemas import (
    EventMessage,
    Entity,
    Actor,
    EventDefinition,
    ActorType,
    BaseEventMessage,
    BaseEventPayload,
)
from ..schemas.handler import PublishEventInfo
from .event_registry import EventRegistry
from stufio.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


class EventBus:
    """Central event bus for publishing and consuming events with Clickhouse support.
    
    Implemented as a singleton to ensure only one instance exists.
    """
    _instance: Optional['EventBus'] = None
    _initialized: bool = False

    kafka_client: Optional[KafkaBroker] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def initialize(self) -> None:
        """Initialize any async resources."""
        if self._initialized:
            return

        # Use lazy import to avoid circular reference
        from ..consumers.kafka import get_kafka_broker
        kafka_client = get_kafka_broker()
        if isinstance(kafka_client, KafkaBroker):
            self.kafka_client = kafka_client

        self.registry = EventRegistry()
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self._initialized = True

    async def publish_from_info(
        self,
        event_info: "PublishEventInfo",
        delay_ms: Optional[int] = None,
    ) -> Optional["BaseEventMessage"]:
        """
        Publish an event using a PublishEventInfo object.
        
        Requires an event_type (EventDefinition) to be specified.
        
        Args:
            event_info: Information about the event to publish
            delay_ms: Optional delay in milliseconds for delayed delivery
            
        Returns:
            The event message that was published, or None if publishing failed
        """

        if not isinstance(event_info, PublishEventInfo):
            try:
                # Try to convert dict to PublishEventInfo
                event_info = PublishEventInfo(**event_info)
            except Exception as e:
                logger.error(
                    f"❌ Failed to convert event_info to PublishEventInfo: {e}"
                )
                return None

        # Ensure event bus is initialized
        await self.initialize()

        try:
            # Get delay_ms from PublishEventInfo if available
            info_delay_ms = getattr(event_info, "delay_ms", None)
            effective_delay_ms = delay_ms if delay_ms is not None else info_delay_ms

            # Publish using the event definition
            return await self.publish_from_definition(
                event_def=event_info.event_type,
                entity_id=event_info.entity_id,
                actor_type=event_info.actor_type,
                actor_id=event_info.actor_id,
                payload=event_info.payload,
                correlation_id=str(event_info.correlation_id) if event_info.correlation_id else None,
                delay_ms=effective_delay_ms
            )
        except Exception as e:
            logger.exception(f"Error publishing event from info: {e}")
            return None

    async def publish_from_definition(
        self,
        event_def: Type[EventDefinition],
        entity_id: Optional[str] = None,
        actor_type: Optional[Union[str, ActorType]] = None,
        actor_id: Optional[str] = None,
        payload: Optional[Union[Dict[str, Any], BaseModel]] = None,
        correlation_id: Optional[str] = None,
        payload_class: Optional[Type[BaseEventPayload]] = None,  # Added parameter
        custom_headers: Optional[Dict[str, str]] = None,  # Added parameter
        delay_ms: Optional[int] = None,  # Added parameter for delayed delivery
    ) -> BaseEventMessage:
        """Publish an event using an EventDefinition class."""
        # Get event definition attributes
        event_attrs = event_def._event_attrs

        # Fix tuple attributes if present
        entity_type = event_attrs.get('entity_type')
        action = event_attrs.get('action')

        # Handle tuple values
        if isinstance(entity_type, tuple) and entity_type:
            entity_type = entity_type[0]
            logger.warning(f"Fixed tuple entity_type={entity_type} for {event_def.__name__}")

        if isinstance(action, tuple) and action:
            action = action[0]
            logger.warning(f"Fixed tuple action={action} for {event_def.__name__}")

        # Use the explicitly passed payload_class if provided
        if not payload_class:
            payload_class = event_def.get_payload_class()  # Use the safer method

        # Create appropriate message class
        message_class = event_attrs.get('message_class', BaseEventMessage[payload_class])

        logger.debug(f"Publishing {event_def.__name__} with payload_class: {payload_class.__name__}")

        # Validate required fields based on event definition
        if event_attrs.get('require_entity', True) and not entity_id:
            raise ValueError(f"Entity ID is required for event {event_attrs.get('name')}")

        # Process the payload with the correct type
        validated_payload = None
        if payload is not None:
            if isinstance(payload, BaseModel):
                # Already a model - check if it's the right type
                if not isinstance(payload, payload_class):
                    # Convert to the correct type
                    payload_dict = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
                    validated_payload = payload_class(**payload_dict)
                else:
                    validated_payload = payload
            else:
                # Dictionary - convert to the right payload class
                validated_payload = payload_class(**payload)

        # Determine topic using the flexible approach
        custom_topic = event_attrs.get('topic')
        is_high_volume = event_attrs.get('high_volume', False)

        # Check if delayed delivery is supported for this event type
        if delay_ms is not None:
            delivery_delay_enabled = event_attrs.get('delivery_delay_enabled', False)
            max_delay_ms = event_attrs.get('max_delivery_delay_ms', 604800000)  # 7 days default

            if not delivery_delay_enabled:
                logger.warning(f"Delay requested for {event_def.__name__} but delayed delivery not enabled for this event")
                delay_ms = None
            elif delay_ms > max_delay_ms:
                logger.warning(f"Requested delay {delay_ms}ms exceeds max delay {max_delay_ms}ms for {event_def.__name__}")
                delay_ms = max_delay_ms

        # Use the event-specific topic configuration
        return await self.publish(
            entity_type=str(entity_type) if entity_type else "unknown",
            entity_id=entity_id or "",
            action=str(action) if action else "unknown",
            actor_type=actor_type or ActorType.SYSTEM,
            actor_id=actor_id or "system",
            payload=validated_payload,
            correlation_id=correlation_id,
            message_class=message_class,  # Use the message class from the definition
            custom_topic=custom_topic,        # Pass custom topic
            is_high_volume=is_high_volume,     # Pass high volume flag
            custom_headers=custom_headers,  # Pass custom headers
            delay_ms=delay_ms  # Pass the delay parameter
        )

    async def publish(
        self, 
        entity_type: str,
        entity_id: str,
        action: str,
        actor_type: Union[str, ActorType],
        actor_id: str,
        payload: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        message_class: Type[BaseEventMessage] = BaseEventMessage,
        custom_topic: Optional[str] = None,
        is_high_volume: bool = False,
        custom_headers: Optional[Dict[str, str]] = None,
        delay_ms: Optional[int] = None,
    ) -> BaseEventMessage:
        """
        Publish an event to the event bus.
        
        Args:
            entity_type: Type of entity this event is about
            entity_id: ID of the entity this event is about
            action: Action that occurred (verb)
            actor_type: Type of actor that triggered the event
            actor_id: ID of the actor that triggered the event
            payload: Event payload data
            correlation_id: Correlation ID for tracking related events
            message_class: Class of the event message
            custom_topic: Optional custom topic name
            is_high_volume: Flag for high volume events
            custom_headers: Optional custom headers to include with the message
            delay_ms: Optional delay in milliseconds for delayed delivery
            
        Returns:
            The event message that was published
        """

        # Ensure Kafka client is initialized
        await self.initialize()

        # Convert actor_type to string if it's an enum
        if isinstance(actor_type, ActorType):
            actor_type = actor_type.value

        # Prepare payload and entity objects
        payload_obj = payload or {}
        entity_obj = Entity(type=entity_type, id=entity_id)
        actor_obj = Actor(type=actor_type, id=actor_id)

        # Get correlation ID from parameter or TaskContext
        corr_id = (
            UUID4(correlation_id)
            if correlation_id
            else TaskContext.get_correlation_id()
        )

        # Always add correlation_id to Kafka headers
        kafka_headers = custom_headers or {}
        kafka_headers["correlation_id"] = str(corr_id)
        kafka_headers["entity_type"] = entity_type
        kafka_headers["action"] = action

        # Create the typed event message
        event = message_class(
            event_id=uuid4(),
            correlation_id=corr_id,
            timestamp=datetime.utcnow(),
            entity=entity_obj,
            action=action,
            actor=actor_obj,
            payload=payload_obj
        )

        # Generate proper key for partitioning
        key_bytes = f"{entity_type}.{entity_id}".encode('utf-8') if entity_id else entity_type.encode('utf-8')

        if custom_topic:
            topic_name = custom_topic
        else:
            topic_name = f"{settings.events_KAFKA_TOPIC_PREFIX}"
            if is_high_volume:
                topic_name += f".{entity_type}.{action}"

        # Prepare headers for delayed delivery if needed
        if delay_ms is not None and delay_ms > 0:
            # Add delay information to headers
            kafka_headers["delivery_delay_ms"] = str(delay_ms)
            logger.info(f"Publishing {entity_type}.{action} with {delay_ms}ms delay to topic {topic_name}")

        # Publish to Kafka - add special debug log
        try:
            logger.debug(f"Publishing to Kafka with headers: {kafka_headers}")

            if delay_ms is not None and delay_ms > 0:
                await self.publish_with_delay(
                    message=event,
                    topic=topic_name,
                    key=key_bytes,
                    headers=kafka_headers,
                    correlation_id=str(event.correlation_id),
                    delay_ms=int(delay_ms),
                )
            else:
                # Use standard publish for non-delayed messages
                await self.kafka_client.publish(
                    message=event,
                    topic=topic_name,
                    key=key_bytes,
                    headers=kafka_headers,
                    correlation_id=str(event.correlation_id)
                )

            logger.info(f"Published event {event.event_id} to {topic_name}")

        except Exception as e:
            logger.error(f"Error publishing event to Kafka: {e}", exc_info=True)

        # Process in-memory handlers
        # try:
        #     await self._process_memory_handlers(event)
        # except Exception as e:
        #     logger.error(f"Error processing in-memory handlers: {e}", exc_info=True)

        # # Process HTTP subscribers asynchronously
        # asyncio.create_task(self._process_http_subscribers(event))

        return event

    async def publish_with_delay(
        self, message, topic, key: str, headers: dict, correlation_id: str, delay_ms: int
    ):
        """
        Publish a message to Kafka with optional delivery delay.

        This helper function handles delayed message delivery by setting the appropriate
        Kafka headers for delayed delivery. Note that this requires Kafka 2.7+ with
        delayed delivery support enabled on the broker.

        Args:
            broker: The Kafka broker instance
            message: The message to publish
            topic: The topic to publish to
            key: Optional message key for partitioning
            headers: Optional message headers
            correlation_id: Optional correlation ID
            delay_ms: Optional delay in milliseconds
        """
        # Prepare headers with delay info if needed
        final_headers = headers.copy() if headers else {}

        if (
            delay_ms is not None
            and delay_ms > 0
            and settings.events_KAFKA_DELAYED_TOPIC_ENABLED
        ):
            # Add delay timestamp to headers
            from time import time

            current_time_ms = int(time() * 1000)
            target_time_ms = current_time_ms + delay_ms

            # Add delivery time header
            final_headers["delivery_time"] = str(target_time_ms)
            final_headers["original_topic"] = topic
            final_headers["delay_ms"] = str(delay_ms)
            final_headers["correlation_id"] = (
                str(correlation_id) if correlation_id else None
            )

            # Log the delay information
            logger.info(
                f"Publishing message to {topic} with {delay_ms}ms delay, "
                + f"current_time={current_time_ms}, delivery_time={target_time_ms}"
            )

            topic = settings.events_KAFKA_DELAYED_TOPIC_NAME

        # Call the regular publish method with the updated headers
        return await self.kafka_client.publish(
            message=message,
            topic=topic,
            key=key,
            headers=final_headers,
            correlation_id=correlation_id,
        )

    async def close(self):
        """Close any async resources."""
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None


# Create global instance
event_bus = EventBus()

# Export a function to get the broker for external modules
def get_broker():
    """Get the Kafka broker instance.
    
    This is a convenience function to avoid direct access to the broker.
    Uses lazy import to avoid circular dependencies.
    """
    from ..consumers.kafka import get_kafka_broker
    return get_kafka_broker()

def get_event_bus():
    """Get the singleton event bus instance."""
    return event_bus
