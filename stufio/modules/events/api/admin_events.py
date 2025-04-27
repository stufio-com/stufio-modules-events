from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from ..utils.context import TaskContext
from ..schemas import (
    EventSubscription,
    EventLogResponse,
    EventDefinitionResponse,
)
from ..models import EventDefinitionModel, EventSubscriptionModel
from ..services.event_bus import get_event_bus
from ..schemas.event_definition import get_event_definition_class

# New model for the updated API
class EventPublishRequest(BaseModel):
    """Request model for publishing an event through the API."""
    event_type: str  # Event definition class name
    entity_id: str
    actor_type: Optional[str] = "system"
    actor_id: Optional[str] = "system"
    payload: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None
    correlation_id: Optional[str] = None

router = APIRouter()


@router.post("/publish", response_model=Dict[str, Any])
async def publish_event_with_definition(
    event_request: EventPublishRequest,
    request: Request
):
    """
    Publish an event using an event definition.
    
    This is the preferred method for publishing events as it ensures
    proper typing and validation through predefined event definitions.
    """
    event_bus = get_event_bus()
    
    # Get correlation_id from request.state if not provided
    if not event_request.correlation_id:
        event_request.correlation_id = getattr(
            request.state, "correlation_id", 
            str(TaskContext.get_correlation_id())
        )
    
    # Look up the event definition class by name
    try:
        event_def_class = get_event_definition_class(event_request.event_type)
        if not event_def_class:
            raise ValueError(f"Event definition '{event_request.event_type}' not found")
    except Exception as e:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid event type: {str(e)}"
        )
    
    # Publish using the event definition
    try:
        event = await event_bus.publish_from_definition(
            event_def=event_def_class,
            entity_id=event_request.entity_id,
            actor_type=event_request.actor_type,
            actor_id=event_request.actor_id,
            payload=event_request.payload,
            correlation_id=event_request.correlation_id,
            metrics=event_request.metrics
        )
        
        return {"status": "success", "event_id": str(event.event_id)}
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to publish event: {str(e)}"
        )


@router.post("/definition", response_model=EventDefinitionResponse)
async def register_event_definition(
    definition: Any,
    request: Request,
):
    """Register a new event definition."""
    # if not hasattr(request.app.state, "event_bus"):
    #     raise HTTPException(status_code=500, detail="Event bus not initialized")

    event_bus = get_event_bus()

    registry = event_bus.registry

    try:
        return await registry.register_event_definition(definition)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register event definition: {str(e)}")


@router.post("/subscription", response_model=EventSubscription)
async def register_subscription(
    subscription: EventSubscription,
    request: Request,
):
    """Register a new event subscription."""
    # if not hasattr(request.app.state, "event_bus"):
    #     raise HTTPException(status_code=500, detail="Event bus not initialized")

    event_bus = get_event_bus()

    registry = event_bus.registry

    try:
        return await registry.register_subscription(subscription)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register subscription: {str(e)}")


@router.get("/definitions", response_model=List[EventDefinitionResponse])
async def list_event_definitions(request: Request):
    """List all event definitions."""
    # if not hasattr(request.app.state, "event_bus"):
    #     raise HTTPException(status_code=500, detail="Event bus not initialized")

    event_bus = get_event_bus()

    engine = event_bus.registry.engine
    definitions = await engine.find(EventDefinitionModel)

    result = []
    for definition in definitions:
        result.append({
            "id": str(definition.id),
            "name": definition.name,
            "entity_type": definition.entity_type,
            "action": definition.action,
            "description": definition.description,
            "module_name": definition.module_name
        })

    return result


@router.get("/subscriptions", response_model=List[EventSubscription])
async def list_subscriptions(request: Request):
    """List all event subscriptions."""
    # if not hasattr(request.app.state, "event_bus"):
    #     raise HTTPException(status_code=500, detail="Event bus not initialized")

    event_bus = get_event_bus()

    engine = event_bus.registry.engine
    subscriptions = await engine.find(EventSubscriptionModel)

    result = []
    for subscription in subscriptions:
        result.append(subscription.dict())

    return result


@router.get("/logs", response_model=List[EventLogResponse])
async def list_event_logs(
    entity_type: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = 100
):
    """List event logs with optional filtering."""
    from ..crud import crud_event_log

    # Build query filters
    filters = {}
    if entity_type:
        filters["entity_type"] = entity_type

    if action:
        filters["action"] = action

    # Get events from Clickhouse
    logs = await crud_event_log.get_multi(
        filters=filters, sort="timestamp DESC", limit=limit
    )

    # Convert to response models
    result = []
    for log in logs:
        result.append(EventLogResponse(
            id=log.id,
            event_id=log.event_id,
            correlation_id=log.correlation_id,
            timestamp=log.timestamp,
            entity_type=log.entity_type,
            entity_id=log.entity_id,
            action=log.action,
            actor_type=log.actor_type,
            actor_id=log.actor_id,
            payload=log.payload,
            metrics=log.metrics,
            processed=log.processed,
            processing_attempts=log.processing_attempts,
            error_message=log.error_message,
            created_at=log.created_at
        ))

    return result


# @router.post("/replay", response_model=Dict[str, Any])
# async def replay_events(
#     request: Request,
#     background_tasks: BackgroundTasks,
#     entity_type: Optional[str] = None,
#     action: Optional[str] = None,
#     limit: int = 50,
# ):
#     """Replay events for testing or recovery purposes."""
#     if not hasattr(request.app.state, "event_bus"):
#         raise HTTPException(status_code=500, detail="Event bus not initialized")

#     event_bus = request.app.state.event_bus

#     # Run replay in background task to avoid blocking
#     async def do_replay():
#         await event_bus.replay_events(entity_type, action, limit)

#     background_tasks.add_task(do_replay)

#     return {
#         "status": "success",
#         "message": f"Replaying up to {limit} events in the background"
#     }
