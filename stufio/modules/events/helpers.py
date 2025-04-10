"""Helper functions for publishing events from other modules."""
from typing import Dict, Any, List, Optional, Type, Union
from .schemas.event_definition import EventDefinition
from .schemas.base import ActorType, BaseEventPayload
from .services.event_bus import get_event_bus
from .services.event_registry import event_registry
from .services.consumer_registry import consumer_registry

async def publish_event(
    event_def: Type[EventDefinition],
    entity_id: Optional[str] = None,
    actor_type: Optional[Union[str, ActorType]] = None,
    actor_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None,
    payload_class: Optional[Type[BaseEventPayload]] = None,
    custom_headers: Optional[Dict[str, str]] = None  # Added parameter
):
    event_bus = get_event_bus()
    """Helper function to publish an event from any module."""
    return await event_bus.publish_from_definition(
        event_def=event_def,
        entity_id=entity_id,
        actor_type=actor_type,
        actor_id=actor_id,
        payload=payload,
        correlation_id=correlation_id,
        metrics=metrics,
        payload_class=payload_class,
        custom_headers=custom_headers  # Pass custom headers
    )


def subscribe_to_event(entity_type: str, action: str, handler):
    """Subscribe to events with a handler function.

    Args:
        entity_type: Type of entity to subscribe to
        action: Action to subscribe to
        handler: Function to call when event is received
    """
    event_bus = get_event_bus()
    event_bus.subscribe(entity_type, action, handler)


def register_module_events(
    module_name: str, events: List[Type[EventDefinition]]
) -> None:
    """Register all events from a module."""
    event_registry.register_module_events(module_name, events)
