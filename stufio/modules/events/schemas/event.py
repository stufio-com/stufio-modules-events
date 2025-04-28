from datetime import datetime
from typing import Dict, Optional, Any
from uuid import uuid4
from pydantic import BaseModel, Field, UUID4, model_validator

from .base import Entity, Actor, EventMetrics, EventSubscription
from .event_definition import EventDefinition

class EventPayload(BaseModel):
    """Generic event payload."""
    before: Optional[Dict[str, Any]] = Field(None, description="State before the event (optional)")
    after: Optional[Dict[str, Any]] = Field(None, description="State after the event (optional)")
    extra: Optional[Dict[str, Any]] = Field(None, description="Additional metadata (optional)")

class EventMessage(BaseModel):
    """Legacy event message format."""
    event_id: UUID4 = Field(default_factory=uuid4, description="Unique event identifier")
    correlation_id: Optional[UUID4] = Field(default=None, description="Correlation ID for tracking related events")
    timestamp: datetime = Field(..., description="Event timestamp")
    entity: Entity = Field(..., description="Entity involved in the event")
    action: str = Field(..., description="Action performed on the entity")
    actor: Actor = Field(..., description="Actor who performed the action")
    payload: EventPayload = Field(..., description="Event payload data")

    @model_validator(mode='after')
    def ensure_payload_structure(self):
        # If payload is provided but not as EventPayload, convert it
        if isinstance(self.payload, dict):
            self.payload = EventPayload(**self.payload)
        return self

class EventSubscriptionCreate(EventSubscription):
    pass

class EventSubscriptionUpdate(EventSubscription):
    pass

class EventLogCreate(BaseModel):
    """Schema for creating event logs."""
    event_id: UUID4
    correlation_id: Optional[UUID4] = None
    timestamp: datetime
    entity_type: str
    entity_id: str
    action: str
    actor_type: str
    actor_id: str
    # Explicitly define as strings
    payload: Optional[str] = None  # JSON string
    metrics: Optional[str] = None  # JSON string

class EventLogUpdate(EventLogCreate):
    pass

class EventLogInDB(EventLogCreate):
    """Schema for event logs stored in the database."""
    id: UUID4 = Field(default_factory=uuid4)
    processed: bool
    processing_attempts: int
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    pass

class EventLogResponse(BaseModel):
    """Schema for event log responses."""
    id: UUID4
    event_id: UUID4
    correlation_id: Optional[UUID4] = None
    timestamp: datetime
    entity_type: str
    entity_id: str
    action: str
    actor_type: str
    actor_id: str
    payload: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None
    processed: bool
    processing_attempts: int
    error_message: Optional[str] = None
    created_at: datetime


class EventDefinitionBase(BaseModel):
    """Schema for returning event definitions."""

    name: str
    entity_type: str
    action: str
    require_actor: bool = False
    require_entity: bool = True
    require_payload: bool = False
    require_metrics: bool = False
    descr: Optional[str] = None
    payload_schema: Optional[Dict[str, Any]] = None
    payload_example: Optional[Dict[str, Any]] = None
    module_name: str


class EventDefinitionCreate(EventDefinitionBase):
    """Schema for creating event definitions."""
    pass


class EventDefinitionUpdate(EventDefinitionBase):
    """Schema for updating event definitions."""
    pass


class EventDefinitionResponse(EventDefinitionBase):
    """Schema for returning event definitions."""
    name: str
    entity_type: str
    action: str
    require_actor: bool = False
    require_entity: bool = True
    require_payload: bool = False
    require_metrics: bool = False
    descr: Optional[str] = None
    payload_schema: Optional[Dict[str, Any]] = None
    payload_example: Optional[Dict[str, Any]] = None
    module_name: str


class EventDefinitionInDB(EventDefinitionBase):
    """Schema for event definitions stored in the database."""
    created_at: datetime
    updated_at: datetime
    pass
