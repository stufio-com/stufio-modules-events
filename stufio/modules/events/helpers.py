"""Helper functions for publishing events from other modules."""
from typing import Dict, Any, List, Optional, Type, Union

from faststream.kafka.fastapi import KafkaMessage
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
    payload_class: Optional[Type[BaseEventPayload]] = None,
    custom_headers: Optional[Dict[str, str]] = None,
    delay_ms: Optional[int] = None
):
    """Helper function to publish an event from any module.
    
    Args:
        event_def: Event definition class
        entity_id: ID of the entity this event is about
        actor_type: Type of actor that triggered the event
        actor_id: ID of the actor that triggered the event
        payload: Event payload data
        correlation_id: Correlation ID for tracking related events
        payload_class: Optional class to validate the payload
        custom_headers: Optional custom headers to include with the message
        delay_ms: Optional delay in milliseconds for delayed event delivery
    """
    event_bus = get_event_bus()
    
    return await event_bus.publish_from_definition(
        event_def=event_def,
        entity_id=entity_id,
        actor_type=actor_type,
        actor_id=actor_id,
        payload=payload,
        correlation_id=correlation_id,
        payload_class=payload_class,
        custom_headers=custom_headers,
        delay_ms=delay_ms
    )

def register_module_events(
    module_name: str, events: List[Type[EventDefinition]]
) -> None:
    """Register all events from a module."""
    event_registry.register_module_events(module_name, events)

# Helper functions for cleaner code
def extract_event_metadata(msg: KafkaMessage) -> tuple[str, str]:
    """Extract entity_type and action from message headers or topic."""
    entity_type = "unknown"
    action = "unknown"

    # Try headers first
    if hasattr(msg, "raw_message") and hasattr(msg.raw_message, "headers"):
        headers = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                  v.decode('utf-8') if isinstance(v, bytes) else v 
                  for k, v in getattr(msg.raw_message, "headers", [])}

        if "entity_type" in headers:
            entity_type = str(headers["entity_type"])
            
        if "action" in headers:
            action = str(headers["action"])

    # Fall back to topic parsing
    if entity_type == "unknown" or action == "unknown":
        topic = getattr(msg.raw_message, "topic", "") if hasattr(msg, "raw_message") and hasattr(msg.raw_message, "topic") else ""
        topic_parts = topic.split(".")
        if len(topic_parts) >= 3:
            if entity_type == "unknown":
                entity_type = topic_parts[-2]
            if action == "unknown":
                action = topic_parts[-1]

    return entity_type, action


def extract_headers_safely(msg) -> Dict[str, str]:
    """Extract headers from a message safely, handling various formats."""
    headers = {}
    try:
        # Get raw headers from different possible locations
        raw_headers = None
        
        # Try all possible locations for headers
        if hasattr(msg, "headers") and msg.headers is not None:
            raw_headers = msg.headers
        elif hasattr(msg, "raw_message") and hasattr(msg.raw_message, "headers"):
            raw_headers = msg.raw_message.headers or []
        elif hasattr(msg, "_message") and hasattr(msg._message, "headers"):
            raw_headers = msg._message.headers or []
        
        if not raw_headers:
            return {}
            
        # Process based on type
        if isinstance(raw_headers, dict):
            # Dictionary format
            for k, v in raw_headers.items():
                headers[k.decode('utf-8') if isinstance(k, bytes) else k] = (
                    v.decode('utf-8') if isinstance(v, bytes) else v
                )
        elif hasattr(raw_headers, "__iter__"):
            # Iterable format (list, tuple, etc)
            for item in raw_headers:
                if hasattr(item, "__len__") and len(item) == 2:
                    # Tuple format (key, value)
                    k, v = item
                    headers[k.decode('utf-8') if isinstance(k, bytes) else k] = (
                        v.decode('utf-8') if isinstance(v, bytes) else v
                    )
                elif hasattr(item, "key") and hasattr(item, "value"):
                    # Object format with key/value attributes
                    k = item.key
                    v = item.value
                    headers[k.decode('utf-8') if isinstance(k, bytes) else k] = (
                        v.decode('utf-8') if isinstance(v, bytes) else v
                    )
                
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Error extracting headers: {e}", exc_info=True)
        
    return headers
