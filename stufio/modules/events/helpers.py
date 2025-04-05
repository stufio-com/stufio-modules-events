"""Helper functions for publishing events from other modules."""
from typing import Dict, Any, Optional, Type, Union
from .schemas.event_definition import EventDefinition
from .schemas.base import ActorType, BaseEventPayload
from .event_bus import event_bus

async def publish_event(
    event_def: Type[EventDefinition],
    entity_id: Optional[str] = None,
    actor_type: Optional[Union[str, ActorType]] = None,
    actor_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None,
    payload_class: Optional[Type[BaseEventPayload]] = None  # Add this parameter
):
    """Helper function to publish an event from any module."""
    return await event_bus.publish_from_definition(
        event_def=event_def,
        entity_id=entity_id,
        actor_type=actor_type,
        actor_id=actor_id,
        payload=payload,
        correlation_id=correlation_id,
        metrics=metrics,
        payload_class=payload_class  # Pass it to publish_from_definition
    )


def subscribe_to_event(entity_type: str, action: str, handler):
    """Subscribe to events with a handler function.
    
    Args:
        entity_type: Type of entity to subscribe to
        action: Action to subscribe to
        handler: Function to call when event is received
    """
    event_bus.subscribe(entity_type, action, handler)