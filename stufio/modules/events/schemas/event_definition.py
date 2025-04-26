from typing import Dict, Any, Optional, Type, ClassVar, List, Union, TypeVar, Generic, get_args, get_origin
import logging
from pydantic import BaseModel, validator
from stufio.core.config import get_settings
from ..utils.context import TaskContext

from .payloads import BaseEventPayload
from .messages import BaseEventMessage, get_message_class
from .base import ActorType

# Type variable for better type hinting
P = TypeVar('P', bound='BaseEventPayload')

logger = logging.getLogger(__name__)

class EventDefinitionMeta(type):
    """Metaclass for EventDefinition to handle class attribute setup."""

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)

        # Skip processing for the base EventDefinition class itself
        if name == "EventDefinition" and namespace.get("__module__", "").endswith("event_definition"):
            return cls

        # Initialize event attributes dictionary
        cls._event_attrs = {}

        # First extract class attributes into _event_attrs
        for key, value in namespace.items():
            if not key.startswith("__") and not callable(value):
                # Fix for tuple values - convert to strings if needed
                if key in ["name", "entity_type", "action", "description"] and isinstance(value, tuple):
                    if value:  # Check if tuple has elements
                        value = value[0]  # Take first element

                cls._event_attrs[key] = value

            if key == '__orig_bases__':
                bases = value

        # Extract payload class from Generic type parameter - this is the critical part
        extracted_payload_class = None
        for base in bases:
            try:
                origin = get_origin(base)
                if origin is EventDefinition:
                    args = get_args(base)
                    if args and len(args) > 0:
                        # Fix: Check if args[0] is a tuple - if so, use the first element
                        payload_type = args[0]
                        if isinstance(payload_type, tuple):
                            logger.warning(f"Multiple type arguments found for {name}, using first one")
                            payload_type = payload_type[0]  # Use first element of tuple

                        if issubclass(payload_type, BaseEventPayload):
                            extracted_payload_class = payload_type
                            logger.info(f"✓ Successfully extracted payload class {payload_type.__name__} for {name}")
                            break
            except Exception as e:
                logger.error(f"Error examining base class {base} for {name}: {e}")

        # Set payload_class in _event_attrs with explicit priority rules
        if extracted_payload_class:
            cls._event_attrs["payload_class"] = extracted_payload_class
        elif "payload_class" not in cls._event_attrs:
            cls._event_attrs["payload_class"] = BaseEventPayload
            logger.debug(f"Using default BaseEventPayload for {name}")

        # Fix: Add proper error handling for logging the payload class name
        payload_class = cls._event_attrs.get("payload_class")
        try:
            if payload_class:
                if isinstance(payload_class, tuple):
                    logger.warning(f"Payload class for {name} is a tuple: {payload_class}")
                    # Use first element if it's a tuple
                    if len(payload_class) > 0:
                        payload_class = payload_class[0]
                        cls._event_attrs["payload_class"] = payload_class

                logger.info(f"Final payload_class for {name}: {payload_class.__name__}")
        except Exception as e:
            logger.error(f"Error getting payload class name for {name}: {e}")

        # Set message_class based on payload_class
        if payload_class and "message_class" not in cls._event_attrs:
            cls._event_attrs["message_class"] = BaseEventMessage[payload_class]

        if "topic" not in cls._event_attrs:
            topic_prefix = getattr(
                get_settings(), "events_KAFKA_TOPIC_PREFIX", "stufio.events"
            )
            if cls.topic is not None:
                # Use custom topic if defined
                cls._event_attrs["topic"] = cls.topic
            else:
                if "high_volume" in cls._event_attrs or cls.high_volume:
                    # Use dedicated topic for high-volume events
                    if topic_prefix:
                        topic_prefix += "."
                    cls._event_attrs["topic"] = (
                        f"{topic_prefix}{cls.entity_type}.{cls.action}"
                    )
                else:
                    cls._event_attrs["topic"] = topic_prefix
                    
        if "retention_days" not in cls._event_attrs:
            cls._event_attrs["retention_days"] = cls.retention_days

        # Add to global registry if it's a concrete class
        if name != "EventDefinition" and name.endswith("Event"):
            ALL_EVENTS.append(cls)

        return cls


class EventDefinition(Generic[P], metaclass=EventDefinitionMeta):
    """Base class for defining events using class attributes and generics."""
    _event_attrs: ClassVar[Dict[str, Any]] = {}

    # These class variables will be defined by subclasses
    name: ClassVar[str]
    entity_type: ClassVar[str] 
    action: ClassVar[str]
    require_actor: ClassVar[bool] = False
    require_entity: ClassVar[bool] = False
    require_payload: ClassVar[bool] = False
    description: ClassVar[Optional[str]] = None

    # New topic-related attributes
    topic: ClassVar[Optional[str]] = None  # Custom topic name (overrides default)
    high_volume: ClassVar[bool] = False    # Flag for high-volume events
    partitions: ClassVar[int] = 0          # Custom partition count (0 = use default)
    retention_days: ClassVar[int] = 0      # Custom retention period (0 = use default)

    @classmethod 
    def get_topic_name(cls) -> str:
        """Get the topic name for this event."""
        # Metaclass already computed the topic
        return cls._event_attrs.get("topic", "")

    @classmethod
    def get_payload_class(cls) -> Type[BaseEventPayload]:
        """Get the payload class with fallback mechanism."""
        payload_class = cls._event_attrs.get("payload_class")

        # Handle case where payload_class is a tuple
        if isinstance(payload_class, tuple):
            if len(payload_class) > 0:
                payload_class = payload_class[0]
                # Update for future calls
                cls._event_attrs["payload_class"] = payload_class
            else:
                payload_class = BaseEventPayload

        # Safety check - if we still don't have the right payload class
        if payload_class is BaseEventPayload:
            # Try one more time to extract from Generic args
            for base in cls.__bases__:
                origin = get_origin(base)
                if origin is EventDefinition:
                    args = get_args(base)
                    if args and len(args) > 0:
                        payload_type = args[0]
                        # Handle tuple case
                        if isinstance(payload_type, tuple) and len(payload_type) > 0:
                            payload_type = payload_type[0]

                        if issubclass(payload_type, BaseEventPayload):
                            logger.info(f"Late extraction of payload class {payload_type.__name__} for {cls.__name__}")
                            cls._event_attrs["payload_class"] = payload_type
                            return payload_type

        return payload_class

    @classmethod
    async def publish(cls, 
                     entity_id: Optional[str] = None,
                     actor_type: Optional[Union[str, ActorType]] = None,
                     actor_id: Optional[str] = None,
                     payload: Optional[Union[Dict[str, Any], BaseEventPayload, BaseModel]] = None,
                     correlation_id: Optional[str] = None,
                     metrics: Optional[Dict[str, Any]] = None):
        """Publish an event with the current event definition."""
        # Import inside function to avoid circular imports
        from ..services.event_bus import get_event_bus
        event_bus = get_event_bus()

        # Get event attributes
        name = cls._event_attrs.get("name")
        entity_type = cls._event_attrs.get("entity_type")
        action = cls._event_attrs.get("action")

        if not name or not entity_type or not action:
            raise ValueError(f"Event definition {cls.__name__} is missing required attributes")


        # Get payload class with fallback
        payload_class = cls.get_payload_class()

        # Log what we're doing
        logger.debug(f"Publishing {cls.__name__} with payload_class: {payload_class.__name__}")

        # Process the payload
        processed_payload = None
        if payload is not None:
            if isinstance(payload, BaseModel):
                # Already a model - check if it's the right type
                if payload_class and not isinstance(payload, payload_class):
                    # Convert to the right type if possible
                    try:
                        if hasattr(payload, "model_dump"):
                            data = payload.model_dump()  # Pydantic v2
                        else:
                            data = payload.dict()  # Pydantic v1
                        processed_payload = payload_class(**data)
                    except Exception as e:
                        logger.error(f"Error converting payload to {payload_class.__name__}: {e}")
                        # Fall back to original payload
                        processed_payload = payload
                else:
                    processed_payload = payload
            elif isinstance(payload, dict) and payload_class:
                # Convert dict to model
                try:
                    processed_payload = payload_class(**payload)
                except Exception as e:
                    logger.error(f"Error creating {payload_class.__name__} from dict: {e}")
                    # Create basic payload as fallback
                    processed_payload = BaseEventPayload(extra=payload)
            else:
                processed_payload = payload

        if not correlation_id:
            # Get correlation ID from TaskContext
            correlation_id = str(TaskContext.get_correlation_id())

        # Publish directly with event_bus
        return await event_bus.publish_from_definition(
            event_def=cls,
            entity_id=entity_id,
            actor_type=actor_type,
            actor_id=actor_id,
            payload=processed_payload,
            correlation_id=correlation_id,  # Use correlation ID from TaskContext
            metrics=metrics,
            payload_class=payload_class  
        )


def event(**kwargs):
    """Decorator for backward compatibility with existing event definitions.

    While new code can use direct class definitions, this decorator maintains
    compatibility with existing implementations.
    """

    def decorator(cls):
        # Update _event_attrs with any provided kwargs
        cls._event_attrs = getattr(cls, "_event_attrs", {}).copy()
        for key, value in kwargs.items():
            cls._event_attrs[key] = value
        return cls

    return decorator


class EventDefinitionValid(BaseModel):
    """Validation model for event definitions."""

    name: str
    entity_type: str
    action: str
    description: Optional[str] = None
    module_name: Optional[str] = None
    version: Optional[str] = None
    require_actor: bool = False
    require_entity: bool = True
    require_payload: bool = False
    payload_schema: Optional[Dict[str, Any]] = None
    payload_example: Optional[Dict[str, Any]] = None

    @validator("name")
    def validate_name(cls, v):
        if not v:
            raise ValueError("Event name cannot be empty")
        if not "." in v:
            raise ValueError("Event name must include a dot (namespace.event)")
        return v

    class Config:
        extra = "allow"


# List of all defined events
ALL_EVENTS: List[Type[EventDefinition]] = []
