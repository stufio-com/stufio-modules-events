"""Helper functions for publishing events from other modules."""
from typing import Dict, Any, Optional, Union, Type
from uuid import UUID
from .event_bus import event_bus, EventBus
from .schemas import EventDefinition, ActorType


async def publish_event(
    event_def: Type[EventDefinition],
    entity_id: Optional[str] = None,
    actor_type: Optional[Union[str, ActorType]] = None,
    actor_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[Union[str, UUID]] = None,
    metrics: Optional[Dict[str, Any]] = None
):
    """Publish an event using the global event bus.
    
    This is a convenience function for other modules to use.
    
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
    """
    # Convert UUID to string if needed
    corr_id_str = str(correlation_id) if correlation_id else None
    
    return await event_bus.publish_from_definition(
        event_def=event_def,
        entity_id=entity_id,
        actor_type=actor_type,
        actor_id=actor_id,
        payload=payload,
        correlation_id=corr_id_str,
        metrics=metrics
    )


def subscribe_to_event(entity_type: str, action: str, handler):
    """Subscribe to events with a handler function.
    
    Args:
        entity_type: Type of entity to subscribe to
        action: Action to subscribe to
        handler: Function to call when event is received
    """
    event_bus.subscribe(entity_type, action, handler)