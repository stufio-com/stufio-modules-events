from datetime import datetime
from enum import Enum
from uuid import UUID as UUID4, uuid4
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, ConfigDict, Field

class ActorType(str, Enum):
    """Type of actor performing an action."""
    ANONYMOUS = "guest"
    USER = "user"
    ADMIN = "admin"
    SYSTEM = "system"
    SERVICE = "service"

class Entity(BaseModel):
    """Entity involved in an event."""
    type: str = Field(..., description="Type of entity (e.g., user, product, order)")
    id: str = Field(..., description="Entity ID")

class Actor(BaseModel):
    """Actor who performed an action."""
    type: ActorType = Field(..., description="Type of actor performing the action")
    id: str = Field(..., description="Actor ID")

class EventMetrics(BaseModel):
    """Performance metrics for event processing."""
    processing_time_ms: Optional[int] = Field(None, description="Processing time in milliseconds")
    total_time_ms: Optional[int] = Field(None, description="Total request time in milliseconds")
    response_size_bytes: Optional[int] = Field(None, description="Size of response in bytes")
    
    # Database specific metrics
    mongodb: Optional[Dict[str, Any]] = Field(None, description="MongoDB operation metrics")
    clickhouse: Optional[Dict[str, Any]] = Field(None, description="ClickHouse query metrics") 
    redis: Optional[Dict[str, Any]] = Field(None, description="Redis operation metrics")
    
    # Legacy fields
    db_time_ms: Optional[int] = Field(None, description="Total database operation time in milliseconds")
    api_time_ms: Optional[int] = Field(None, description="External API time in milliseconds")
    queue_time_ms: Optional[int] = Field(None, description="Time spent in queue")
    custom_metrics: Optional[Dict[str, Any]] = Field(None, description="Custom service-specific metrics")

class EventSubscription(BaseModel):
    """Model for event subscriptions."""
    id: Optional[UUID4] = Field(default_factory=uuid4)
    entity_type: Optional[str] = Field(None, description="Entity type to subscribe to (None for all)")
    action: Optional[str] = Field(None, description="Action to subscribe to (None for all)")
    module_name: str = Field(..., description="Module subscribing to events")
    callback_url: str = Field(..., description="URL to call when event is triggered")
    enabled: bool = Field(default=True, description="Whether subscription is active")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class BaseEventPayload(BaseModel):
    """Base class for all event payloads."""
    extra: Optional[Dict[str, Any]] = Field(
        None, description="Additional metadata (optional)"
    )

    class Config:
        extra = "ignore"


class MessageHeader(BaseModel):
    """A class to represent a header in a message.
    Attributes:
        description : optional description of the header
        location : location of the header
    """
    description: Optional[str] = None
    location: str

    model_config = ConfigDict(extra="allow")
