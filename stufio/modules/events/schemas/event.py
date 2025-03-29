from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Any, List, Union, ClassVar
from uuid import uuid4
from pydantic import BaseModel, Field, UUID4, validator, model_validator


class ActorType(str, Enum):
    USER = "user"
    ADMIN = "admin"
    SYSTEM = "system"
    SERVICE = "service"


class Entity(BaseModel):
    type: str = Field(..., description="Type of entity (e.g., user, product, order)")
    id: str = Field(..., description="Entity ID")


class Actor(BaseModel):
    type: ActorType = Field(..., description="Type of actor performing the action")
    id: str = Field(..., description="Actor ID")


class EventPayload(BaseModel):
    before: Optional[Dict[str, Any]] = Field(None, description="State before the event (optional)")
    after: Optional[Dict[str, Any]] = Field(None, description="State after the event (optional)")
    extra: Optional[Dict[str, Any]] = Field(None, description="Additional metadata (optional)")


class EventMetrics(BaseModel):
    processing_time_ms: Optional[int] = Field(None, description="Processing time in milliseconds")
    db_time_ms: Optional[int] = Field(None, description="Database operation time in milliseconds")
    api_time_ms: Optional[int] = Field(None, description="External API time in milliseconds")
    queue_time_ms: Optional[int] = Field(None, description="Time spent in queue")
    custom_metrics: Optional[Dict[str, Any]] = Field(None, description="Custom service-specific metrics")


class EventMessage(BaseModel):
    event_id: UUID4 = Field(default_factory=uuid4, description="Unique event identifier")
    correlation_id: Optional[UUID4] = Field(default=None, description="Correlation ID for tracking related events")
    timestamp: datetime = Field(..., description="Event timestamp")
    entity: Entity = Field(..., description="Entity involved in the event")
    action: str = Field(..., description="Action performed on the entity")
    actor: Actor = Field(..., description="Actor who performed the action")
    payload: Optional[EventPayload] = Field(None, description="Event payload data")
    metrics: Optional[EventMetrics] = Field(None, description="Event performance metrics")

    @model_validator(mode='after')
    def ensure_payload_structure(self):
        # If payload is provided but not as EventPayload, convert it
        if isinstance(self.payload, dict):
            self.payload = EventPayload(**self.payload)
        return self


class EventDefinition(BaseModel):
    id: Optional[UUID4] = Field(default_factory=uuid4)
    name: str = Field(..., description="Unique event name")
    entity_type: str = Field(..., description="Entity type this event applies to")
    action: str = Field(..., description="Action this event represents")
    description: Optional[str] = Field(None, description="Event description")
    sample_payload: Optional[Dict[str, Any]] = Field(None, description="Sample event payload")
    module_name: str = Field(..., description="Module that owns this event definition")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class EventSubscription(BaseModel):
    id: Optional[UUID4] = Field(default_factory=uuid4)
    entity_type: Optional[str] = Field(None, description="Entity type to subscribe to (None for all)")
    action: Optional[str] = Field(None, description="Action to subscribe to (None for all)")
    module_name: str = Field(..., description="Module subscribing to events")
    callback_url: str = Field(..., description="URL to call when event is triggered")
    enabled: bool = Field(default=True, description="Whether subscription is active")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

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
    payload: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None

class EventLogUpdate(EventLogCreate):
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


class EventDefinition(BaseModel):
    """Base class for defining events."""
    # Class variables to be defined by subclasses
    name: str
    entity_type: str
    action: str 
    require_actor: bool = False
    require_entity: bool = True
    require_payload: bool = True
    require_metrics: bool = False
    # Optional description of the event
    description: Optional[str] = None
    payload_schema: Optional[Dict[str, Any]] = None
    
    # Optional documentation for expected payload
    payload_example: Optional[Dict[str, Any]] = None
    
    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Convert the event definition to a dictionary."""
        return {
            "name": cls.name,
            "entity_type": cls.entity_type,
            "action": cls.action,
            "description": cls.description,
            "payload_schema": cls.payload_schema,
            "payload_example": cls.payload_example,
            "module_name": cls.__module__.split(".")[2]  # Extract module name from full path
        }
