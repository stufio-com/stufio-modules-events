from datetime import datetime
from typing import Any, Dict, Optional, List
from uuid import uuid4
from odmantic import Field, EmbeddedModel
from odmantic.index import Index
from pydantic import ConfigDict, UUID4
from stufio.db.clickhouse_base import ClickhouseBase
from stufio.db.mongo_base import MongoBase, datetime_now_sec

def datetime_now():
    return datetime.utcnow()

class EventLogModel(ClickhouseBase):
    """Clickhouse model for storing event logs."""

    id: UUID4 = Field(
        default_factory=uuid4, json_schema_extra={"primary_field": True}
    )
    event_id: UUID4
    correlation_id: Optional[UUID4] = None
    timestamp: datetime
    entity_type: str
    entity_id: str
    action: str
    actor_type: str
    actor_id: str
    payload: Optional[str] = None  # JSON string
    created_at: datetime = Field(default_factory=datetime_now_sec)

    model_config = {"table_name": "event_logs", "from_attributes": True}

class EventDefinitionModel(MongoBase):
    """MongoDB model for storing event definitions."""
    name: str = Field(index=True, unique=True)
    entity_type: str
    action: str
    description: Optional[str] = None
    payload_schema: Optional[Dict[str, Any]] = None
    payload_example: Optional[Dict[str, Any]] = None
    module_name: str
    require_actor: bool = False
    require_entity: bool = True
    created_at: datetime = Field(default_factory=datetime_now)
    updated_at: datetime = Field(default_factory=datetime_now)

    model_config = ConfigDict(
        collection="event_definitions",
        indexes=[
            Index("name", unique=True),
            Index("entity_type", "action"),
            Index("module_name"),
        ],
    )

class EventSubscriptionModel(MongoBase):
    """MongoDB model for storing event subscriptions."""
    entity_type: Optional[str] = None  # None means subscribe to all entity types
    action: Optional[str] = None  # None means subscribe to all actions
    module_name: str
    callback_url: str
    enabled: bool = True
    created_at: datetime = Field(default_factory=datetime_now)
    updated_at: datetime = Field(default_factory=datetime_now)
    
    model_config = ConfigDict(
        collection="event_subscriptions",
        indexes=[
            Index("entity_type", "action"),
            Index("module_name"),
        ],
    )
