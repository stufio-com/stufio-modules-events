from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from typing import List, Dict, Any, Optional
from stufio.api import deps
from ..schemas.topic import (
    TopicInfo,
    TopicMetrics,
    TopicConfigUpdate,
    TopicListResponse,
    TopicRotationResponse,
    CleanupResponse,
    TopicHealthResponse,
    TopicInitializationResponse
)
from ..crud.crud_topics import crud_kafka_topics
from ..consumers.topic_initializer import initialize_kafka_topics

router = APIRouter()

@router.get("", response_model=List[TopicInfo])
async def list_kafka_topics(
    current_user: str = Depends(deps.get_current_active_superuser)
) -> List[TopicInfo]:
    """List all Kafka topics and their configurations."""
    topics = await crud_kafka_topics.list_topics()
    return topics


@router.post("/cleanup", response_model=CleanupResponse)
async def cleanup_empty_topics(
    background_tasks: BackgroundTasks,
    prefix: Optional[str] = None,
    older_than_days: int = 30,
    current_user: str = Depends(deps.get_current_active_superuser)
) -> Dict[str, Any]:
    """Delete empty topics that haven't been used in a while."""
    # Use background task as this can be time-consuming
    async def cleanup_task():
        return await crud_kafka_topics.cleanup_empty_topics(
            prefix=prefix,
            older_than_days=older_than_days
        )
    
    background_tasks.add_task(cleanup_task)
    
    return {
        "status": "pending",
        "empty_topics_found": 0,
        "deleted_topics": [],
        "message": f"Cleanup of empty topics scheduled (prefix: {prefix or 'all'}, older than {older_than_days} days)"
    }

@router.post("/initialize", response_model=TopicInitializationResponse)
async def initialize_topics(
    background_tasks: BackgroundTasks,
    current_user: str = Depends(deps.get_current_active_superuser)
) -> TopicInitializationResponse:
    """Re-initialize all Kafka topics based on event definitions."""
    async def init_task():
        return await initialize_kafka_topics()
    
    background_tasks.add_task(init_task)
    
    return TopicInitializationResponse(
        status="pending",
        message="Topic initialization scheduled"
    )

@router.get("/health", response_model=TopicHealthResponse)
async def get_topics_health(
    current_user: str = Depends(deps.get_current_active_superuser)
) -> TopicHealthResponse:
    """Get health status of all topics."""
    return await crud_kafka_topics.get_topics_health()


@router.get("/{topic_name}", response_model=TopicMetrics)
async def get_topic_metrics(
    topic_name: str, current_user: str = Depends(deps.get_current_active_superuser)
) -> TopicMetrics:
    """Get detailed metrics for a specific topic."""
    metrics = await crud_kafka_topics.get_topic_metrics(topic_name)
    if isinstance(metrics, dict) and "error" in metrics:
        raise HTTPException(status_code=404, detail=metrics["error"])
    return metrics


@router.put("/{topic_name}", response_model=Dict[str, Any])
async def update_topic_config(
    topic_name: str,
    config: TopicConfigUpdate,
    current_user: str = Depends(deps.get_current_active_superuser),
) -> Dict[str, Any]:
    """Update configuration for a specific topic."""
    result = await crud_kafka_topics.update_topic_config(
        topic_name=topic_name, config=config.config
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/{topic_name}/rotate", response_model=TopicRotationResponse)
async def rotate_topic_logs(
    topic_name: str,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(deps.get_current_active_superuser),
) -> Dict[str, str]:
    """Force log rotation for a topic."""

    # Use background task to avoid blocking the request
    async def rotate_logs_task():
        return await crud_kafka_topics.rotate_topic_logs(topic_name=topic_name)

    background_tasks.add_task(rotate_logs_task)

    return {
        "status": "pending",
        "topic": topic_name,
        "message": "Topic log rotation scheduled",
    }
