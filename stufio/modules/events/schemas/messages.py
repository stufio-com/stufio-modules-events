from datetime import datetime
from typing import Dict, Any, Optional, Type, Generic, TypeVar, Union, get_type_hints
from uuid import uuid4
from pydantic import BaseModel, Field, UUID4

from .base import Entity, Actor, EventMetrics, ActorType
from .payloads import BaseEventPayload

# Generic type for payload
P = TypeVar('P', bound=BaseEventPayload)

class BaseEventMessage(BaseModel, Generic[P]):
    """Base class for all event messages with strongly typed payload."""
    event_id: UUID4 = Field(default_factory=uuid4, description="Unique event identifier")
    correlation_id: Optional[UUID4] = Field(default=None, description="Correlation ID for tracking related events")
    timestamp: datetime = Field(..., description="Event timestamp")
    entity: Entity = Field(..., description="Entity involved in the event")
    action: str = Field(..., description="Action performed on the entity")
    actor: Actor = Field(..., description="Actor who performed the action")
    payload: P = Field(..., description="Event payload data")
    metrics: Optional[EventMetrics] = Field(None, description="Event performance metrics")

def get_message_class(event_name: str, payload_class: Type[BaseEventPayload]) -> Type[BaseEventMessage]:
    """Get the appropriate message class for an event name."""
    # Simply use BaseEventMessage with the payload type
    return BaseEventMessage[payload_class]