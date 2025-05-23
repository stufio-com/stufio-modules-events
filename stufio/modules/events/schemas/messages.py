from datetime import datetime
from typing import Dict, Any, Optional, Type, Generic, TypeVar, Union, get_type_hints
from uuid import uuid4
from pydantic import BaseModel, Field, UUID4

from .base import Entity, Actor, ActorType
from .payloads import BaseEventPayload
from ..utils.context import TaskContext

# Generic type for payload
P = TypeVar('P', bound=BaseEventPayload)

class BaseEventMessage(BaseModel, Generic[P]):
    """Base class for all event messages with strongly typed payload."""
    event_id: UUID4 = Field(default_factory=uuid4, description="Unique event identifier")
    correlation_id: Optional[UUID4] = Field(default=None, description="Correlation ID for tracking related events")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    entity: Entity = Field(..., description="Entity involved in the event")
    action: str = Field(..., description="Action performed on the entity")
    actor: Actor = Field(..., description="Actor who performed the action")
    payload: P = Field(..., description="Event payload data")

    def __init__(self, **data):
        # Get correlation_id from context if not provided
        if isinstance(data["correlation_id"], str):
            data["correlation_id"] = UUID4(data["correlation_id"])
        else:
            # If None provided, get from context
            data["correlation_id"] = TaskContext.get_correlation_id()

        super().__init__(**data)

