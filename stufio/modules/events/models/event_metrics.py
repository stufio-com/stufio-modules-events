from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import Field, UUID4
from stufio.db.clickhouse_base import ClickhouseBase


def datetime_now():
    return datetime.utcnow()


class EventMetricsModel(ClickhouseBase):
    """Clickhouse model for storing event metrics."""
    event_id: str  # String representation of UUID
    correlation_id: Optional[str] = None  # String representation of UUID
    tenant: str  # Application name
    source_type: str  # 'consumer' or 'api'
    consumer_name: Optional[str] = None
    module_name: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime_now)
    started_at: datetime
    completed_at: datetime
    duration_ms: int  # Duration in milliseconds
    latency_ms: int = 0  # Processing latency in milliseconds
    success: bool = True
    error_message: Optional[str] = None
    
    # Database metrics
    mongodb_queries: int = 0
    mongodb_time_ms: int = 0
    mongodb_slow_queries: int = 0
    
    clickhouse_queries: int = 0
    clickhouse_time_ms: int = 0
    clickhouse_slow_queries: int = 0
    
    redis_operations: int = 0
    redis_time_ms: int = 0
    redis_slow_operations: int = 0
    
    custom_metrics: Optional[str] = None  # JSON string of custom metrics

    model_config = {"table_name": "event_metrics", "from_attributes": True}