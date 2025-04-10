from typing import Dict, Any, Optional, Callable, Type, Union

from pydantic import BaseModel
from ..schemas import (
    get_message_class,
    EventMessage,
    Entity,
    Actor,
    EventDefinition,
    ActorType,
    BaseEventMessage,
    BaseEventPayload,
)
from .event_registry import EventRegistry
from ..consumers import get_kafka_broker
import asyncio
import logging
import time
import uuid
import httpx
from datetime import datetime
from stufio.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


class EventBus:
    """Central event bus for publishing and consuming events with Clickhouse support.
    
    Implemented as a singleton to ensure only one instance exists.
    """
    _instance: Optional['EventBus'] = None
    _initialized: bool = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(EventBus, cls).__new__(cls)
        return cls._instance

    async def initialize(self) -> None:
        """Initialize any async resources."""
        if self._initialized:
            return

        self.kafka_client = get_kafka_broker()
        self.registry = EventRegistry()
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self._initialized = True

        self._initialized = True

    async def publish_from_definition(
        self,
        event_def: Type[EventDefinition],
        entity_id: Optional[str] = None,
        actor_type: Optional[Union[str, ActorType]] = None,
        actor_id: Optional[str] = None,
        payload: Optional[Union[Dict[str, Any], BaseModel]] = None,
        correlation_id: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
        payload_class: Optional[Type[BaseEventPayload]] = None,  # Added parameter
        custom_headers: Optional[Dict[str, str]] = None,  # Added parameter
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

        # Use the event-specific topic configuration
        return await self.publish(
            entity_type=entity_type,
            entity_id=entity_id or "",
            action=action,
            actor_type=actor_type or ActorType.SYSTEM,
            actor_id=actor_id or "system",
            payload=validated_payload,  # Use the validated payload with correct type
            correlation_id=correlation_id,
            metrics=metrics,
            message_class=message_class,  # Use the message class from the definition
            custom_topic=custom_topic,        # Pass custom topic
            is_high_volume=is_high_volume,     # Pass high volume flag
            custom_headers=custom_headers  # Pass custom headers
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
        metrics: Optional[Dict[str, Any]] = None,
        message_class: Type[BaseEventMessage] = BaseEventMessage,
        custom_topic: Optional[str] = None,
        is_high_volume: bool = False,
        custom_headers: Optional[Dict[str, str]] = None  # Added parameter
    ) -> BaseEventMessage:
        """Publish an event to Kafka and store in Clickhouse."""
        start_time = time.time()

        # Ensure Kafka client is initialized
        await self.initialize()

        # Convert actor_type to string if it's an enum
        if isinstance(actor_type, ActorType):
            actor_type = actor_type.value

        # Prepare payload and entity objects
        payload_obj = payload or {}
        entity_obj = Entity(type=entity_type, id=entity_id)
        actor_obj = Actor(type=actor_type, id=actor_id)

        # Convert correlation ID if needed
        corr_id = uuid.UUID(correlation_id) if correlation_id else uuid.uuid4()

        # Get the right message and payload classes for this event type
        # event_name = f"{entity_type}.{action}"  

        # Create the typed event message
        event = message_class(
            event_id=uuid.uuid4(),
            correlation_id=corr_id,
            timestamp=datetime.utcnow(),
            entity=entity_obj,
            action=action,
            actor=actor_obj,
            payload=payload_obj,
            metrics=metrics or {}
        )

        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)
        if hasattr(event, 'metrics') and hasattr(event.metrics, 'processing_time_ms'):
            event.metrics.processing_time_ms = processing_time

        # Determine which topic to use with flexible configuration
        kafka_headers = {}

        # Always add core headers
        kafka_headers["entity_type"] = entity_type  # Don't encode
        kafka_headers["action"] = action  # Don't encode

        # Add any additional headers you might need
        if hasattr(event, "tenant_id") and event.tenant_id:
            kafka_headers["tenant_id"] = str(event.tenant_id)  # Don't encode

        # Add trace context if available
        if correlation_id:
            kafka_headers["correlation_id"] = str(corr_id)  # Don't encode

        # Add custom headers if provided
        if custom_headers:
            kafka_headers.update(custom_headers)

        # Generate proper key for partitioning
        key_bytes = f"{entity_type}.{entity_id}".encode('utf-8') if entity_id else entity_type.encode('utf-8')

        if custom_topic:
            topic_name = custom_topic
        else:
            topic_name = f"{settings.events_KAFKA_TOPIC_PREFIX}"
            if is_high_volume:
                topic_name += f".{entity_type}.{action}"

        # Publish to Kafka - add special debug log
        try:
            logger.debug(f"Publishing to Kafka with headers: {kafka_headers}")
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
        try:
            await self._process_memory_handlers(event)
        except Exception as e:
            logger.error(f"Error processing in-memory handlers: {e}", exc_info=True)

        # Process HTTP subscribers asynchronously
        asyncio.create_task(self._process_http_subscribers(event))

        return event

    async def _ensure_event_registered(self, event_def: Dict[str, Any]) -> None:
        """Ensure event definition is registered in MongoDB."""
        try:
            # Get the event registry
            await self.registry.register_event(event_def)
        except Exception as e:
            logger.error(f"Error registering event definition: {e}", exc_info=True)

    async def _process_memory_handlers(self, event: EventMessage) -> None:
        """Process all in-memory handlers for this event."""
        handlers = self.registry.get_memory_handlers(event.entity.type, event.action)

        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"Error in event handler: {e}", exc_info=True)

    async def _process_http_subscribers(self, event: EventMessage) -> None:
        """Process all HTTP subscribers for this event."""
        try:
            # Get subscribers from the registry
            subscribers = await self.registry.get_matching_subscriptions(
                event.entity.type, event.action
            )

            # Send event to each subscriber
            for subscriber in subscribers:
                try:
                    resp = await self.http_client.post(
                        subscriber.callback_url,
                        json=event.model_dump(),
                        headers={"Content-Type": "application/json"}
                    )
                    resp.raise_for_status()
                except Exception as e:
                    logger.error(f"Failed to deliver event to {subscriber.callback_url}: {e}")
        except Exception as e:
            logger.error(f"Error processing HTTP subscribers: {e}", exc_info=True)

    def subscribe(self, entity_type: str, action: str, handler: Callable) -> None:
        """Subscribe to events with an in-memory handler function."""
        self.registry.subscribe(entity_type, action, handler)

    # async def replay_events(self, entity_type: Optional[str] = None,
    #                       action: Optional[str] = None, limit: int = 100) -> List[EventMessage]:
    #     """Replay events from Clickhouse for testing or recovery."""
    #     try:
    #         clickhouse_db = await ClickhouseDatabase()

    #         # Build query filters
    #         filters = {}
    #         if entity_type:
    #             filters["entity_type"] = entity_type

    #         if action:
    #             filters["action"] = action

    #         # Get events from Clickhouse
    #         event_logs = await crud_event_log.get_multi(
    #             filters=filters,
    #             sort="timestamp DESC",
    #             limit=limit
    #         )

    #         result = []
    #         for event_log in event_logs:
    #             event = EventMessage(
    #                 event_id=event_log.event_id,
    #                 correlation_id=event_log.correlation_id,
    #                 timestamp=event_log.timestamp,
    #                 entity=Entity(type=event_log.entity_type, id=event_log.entity_id),
    #                 action=event_log.action,
    #                 actor=Actor(type=event_log.actor_type, id=event_log.actor_id),
    #                 payload=EventPayload(**(event_log.payload or {})),
    #                 metrics=EventMetrics(**(event_log.metrics or {}))
    #             )
    #             result.append(event)

    #             # Process event handlers
    #             await self._process_memory_handlers(event)

    #         return result
    #     except Exception as e:
    #         logger.error(f"Error replaying events: {e}", exc_info=True)
    #         return []

# Create global instance
event_bus = EventBus()

def get_event_bus():
    """Get the singleton event bus instance."""
    return event_bus
