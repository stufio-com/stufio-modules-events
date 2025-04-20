from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional, Union


class TopicPartitionInfo(BaseModel):
    """Information about a topic partition."""
    id: int
    leader: int
    replicas: List[int]
    isr: List[int]
    begin_offset: Optional[int] = None
    end_offset: Optional[int] = None
    message_count: Optional[int] = None


class TopicConfig(BaseModel):
    """Configuration for a Kafka topic."""
    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    config: Dict[str, str] = {}
    description: Optional[str] = None
    source_event: Optional[str] = None


class TopicConfigUpdate(BaseModel):
    """Update configuration for a Kafka topic."""
    config: Dict[str, str] = Field(
        ...,
        description="Topic configuration properties to update",
        example={
            "cleanup.policy": "delete",
            "retention.ms": "604800000",
            "segment.bytes": "1073741824"
        }
    )


class TopicInfo(BaseModel):
    """Basic information about a topic."""
    name: str
    num_partitions: int
    partitions: List[TopicPartitionInfo]
    config: Dict[str, str]
    cleanup_policy: str
    retention_days: float


class TopicMetrics(BaseModel):
    """Detailed metrics for a Kafka topic."""
    name: str
    num_partitions: int
    partitions: List[TopicPartitionInfo]
    total_messages: int
    config: Dict[str, str]
    retention_days: float
    cleanup_policy: str
    estimated_size: str
    last_updated: str


class TopicListResponse(BaseModel):
    """Response model for listing topics."""
    topics: List[TopicInfo]


class TopicRotationRequest(BaseModel):
    """Request model for topic log rotation."""
    topic_name: str


class TopicRotationResponse(BaseModel):
    """Response model for topic log rotation."""
    status: str
    topic: str
    message: str


class CleanupResponse(BaseModel):
    """Response model for topic cleanup operation."""
    status: str
    empty_topics_found: int
    deleted_topics: List[str]


class TopicHealthResponse(BaseModel):
    """Response model for topic health status."""
    topics: Dict[str, str]


class TopicInitializationResponse(BaseModel):
    """Response model for topic initialization."""
    status: str
    message: str
    topics_created: Optional[int] = None