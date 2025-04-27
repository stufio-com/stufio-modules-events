from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class EventMetricsBase(BaseModel):
    """Base schema for event metrics data"""
    event_id: str
    correlation_id: Optional[str] = None
    tenant: str
    source_type: str
    consumer_name: Optional[str] = None
    module_name: Optional[str] = None


class EventMetricsCreate(EventMetricsBase):
    """Schema for creating event metrics"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    started_at: datetime
    completed_at: datetime
    duration_ms: int
    latency_ms: int = 0
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

    custom_metrics: Optional[str] = None


class EventMetricsUpdate(EventMetricsCreate):
    """Schema for returning event metrics"""
    pass


class EventMetricsResponse(EventMetricsCreate):
    """Schema for returning event metrics"""
    pass


class CustomMetrics(BaseModel):
    """Schema for custom metrics"""
    handler: Optional[Dict[str, Any]] = None
    # Additional providers can be added as needed
