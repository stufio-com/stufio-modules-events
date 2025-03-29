from typing import Dict, Any, Optional, List, Callable, ClassVar, Type, Union
from .schemas import EventMessage, Entity, Actor, EventPayload, EventMetrics, EventLogCreate, EventDefinition, ActorType
from .event_registry import EventRegistry
from .kafka_client import KafkaClient
from .crud import crud_event_log, crud_event_definitions
import asyncio
import logging
import time
import uuid
import httpx
from datetime import datetime
from stufio.db.clickhouse import ClickhouseDatabase
from stufio.db.mongo import get_engine

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
        
    def __init__(self, kafka_client: Optional[KafkaClient] = None):
        """Initialize the event bus (only once).
        
        Args:
            kafka_client: Kafka client instance
        """
        if self._initialized:
            return
            
        self.kafka_client = kafka_client
        self.registry = EventRegistry()
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self._initialized = True
    
    async def initialize(self) -> None:
        """Initialize any async resources."""
        if not self.kafka_client:
            self.kafka_client = KafkaClient()
            await self.kafka_client.initialize()

    async def publish_from_definition(
        self,
        event_def: Type[EventDefinition],
        entity_id: Optional[str] = None,
        actor_type: Optional[Union[str, ActorType]] = None,
        actor_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> EventMessage:
        """Publish an event using an EventDefinition class.
        
        Args:
            event_def: Event definition class
            entity_id: ID of the entity (if required by event)
            actor_type: Type of actor (if required by event)
            actor_id: ID of actor (if required by event)
            payload: Event payload data
            correlation_id: Optional correlation ID for tracing
            metrics: Optional performance metrics
            
        Returns:
            The published event message
            
        Raises:
            ValueError: If required fields are missing
        """
        # Validate required fields based on event definition
        if event_def.require_entity and not entity_id:
            raise ValueError(f"Entity ID is required for event {event_def.name}")
            
        if event_def.require_actor and (not actor_type or not actor_id):
            raise ValueError(f"Actor type and ID are required for event {event_def.name}")
            
        if event_def.require_payload and not payload:
            raise ValueError(f"Payload is required for event {event_def.name}")
            
        # Set default actor type if not provided
        if not actor_type:
            actor_type = ActorType.SYSTEM
            
        # Set default actor ID if not provided
        if not actor_id and not event_def.require_actor:
            actor_id = "system"
            
        # Auto-register event definition if not already registered
        await self._ensure_event_registered(event_def)
        
        # Publish event with validated parameters
        return await self.publish(
            entity_type=event_def.entity_type,
            entity_id=entity_id or "",
            action=event_def.action,
            actor_type=actor_type,
            actor_id=actor_id or "",
            payload=payload,
            correlation_id=correlation_id,
            metrics=metrics
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
        metrics: Optional[Dict[str, Any]] = None
    ) -> EventMessage:
        """Publish an event to Kafka and store in Clickhouse."""
        start_time = time.time()

        # Ensure Kafka client is initialized
        if not self.kafka_client:
            await self.initialize()

        # Convert actor_type to string if it's an enum
        if isinstance(actor_type, ActorType):
            actor_type = actor_type.value

        # Create event message
        event = EventMessage(
            event_id=uuid.uuid4(),
            correlation_id=uuid.UUID(correlation_id) if correlation_id else uuid.uuid4(),
            timestamp=datetime.utcnow(),
            entity=Entity(type=entity_type, id=entity_id),
            action=action,
            actor=Actor(type=actor_type, id=actor_id),
            payload=EventPayload(**(payload or {})),
            metrics=EventMetrics(**(metrics or {}))
        )

        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)
        if event.metrics and not event.metrics.processing_time_ms:
            event.metrics.processing_time_ms = processing_time

        # Store event in Clickhouse
        try:
            clickhouse_db = await ClickhouseDatabase()
            event_log_data = EventLogCreate(
                event_id=event.event_id,
                correlation_id=event.correlation_id,
                timestamp=event.timestamp,
                entity_type=event.entity.type,
                entity_id=event.entity.id,
                action=event.action,
                actor_type=event.actor.type,
                actor_id=event.actor.id,
                payload=payload,
                metrics=metrics
            )
            await crud_event_log.create(clickhouse_db, obj_in=event_log_data)
        except Exception as e:
            logger.error(f"Error storing event in Clickhouse: {e}", exc_info=True)

        # Publish to Kafka
        topic_name = f"events.{entity_type}.{action}"
        await self.kafka_client.publish(topic_name, event.model_dump())

        # Process in-memory handlers
        await self._process_memory_handlers(event)

        # Process HTTP subscribers asynchronously
        asyncio.create_task(self._process_http_subscribers(event))

        return event
        
    async def _ensure_event_registered(self, event_def: Type[EventDefinition]) -> None:
        """Ensure event definition is registered in MongoDB."""
        try:
            engine = await get_engine()
            
            # Check if event definition already exists
            existing = await crud_event_definitions.get_by_name(
                db=engine,
                name=event_def.name
            )
            
            if not existing:
                # Register new event definition
                event_def_dict = event_def.to_dict()
                await crud_event_definitions.create(
                    db=engine,
                    obj_in=event_def_dict
                )
                logger.info(f"Registered event definition: {event_def.name}")
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

    async def replay_events(self, entity_type: Optional[str] = None, 
                          action: Optional[str] = None, limit: int = 100) -> List[EventMessage]:
        """Replay events from Clickhouse for testing or recovery."""
        try:
            clickhouse_db = await ClickhouseDatabase()

            # Build query filters
            filters = {}
            if entity_type:
                filters["entity_type"] = entity_type

            if action:
                filters["action"] = action

            # Get events from Clickhouse
            event_logs = await crud_event_log.get_multi(
                clickhouse_db, 
                filters=filters,
                sort="timestamp DESC",
                limit=limit
            )

            result = []
            for event_log in event_logs:
                event = EventMessage(
                    event_id=event_log.event_id,
                    correlation_id=event_log.correlation_id,
                    timestamp=event_log.timestamp,
                    entity=Entity(type=event_log.entity_type, id=event_log.entity_id),
                    action=event_log.action,
                    actor=Actor(type=event_log.actor_type, id=event_log.actor_id),
                    payload=EventPayload(**(event_log.payload or {})),
                    metrics=EventMetrics(**(event_log.metrics or {}))
                )
                result.append(event)

                # Process event handlers
                await self._process_memory_handlers(event)

            return result
        except Exception as e:
            logger.error(f"Error replaying events: {e}", exc_info=True)
            return []
            
# Create global instance 
event_bus = EventBus()
