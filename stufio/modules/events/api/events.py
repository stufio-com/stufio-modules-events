from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List, Dict, Any, Optional
from ..schemas import (
    EventSubscription,
    EventLogResponse,
    EventDefinitionResponse,
)
from ..models import EventDefinitionModel, EventSubscriptionModel


router = APIRouter()

@router.post("/publish", response_model=Dict[str, Any])
async def publish_event(
    event_data: Dict[str, Any],
    request: Request
):
    """Publish a new event."""
    # if not hasattr(request.app.state, "event_bus"):
    #     raise HTTPException(status_code=500, detail="Event bus not initialized")
    
    from ..event_bus import event_bus
    # Extract required fields
    entity_type = event_data.get("entity", {}).get("type")
    entity_id = event_data.get("entity", {}).get("id")
    action = event_data.get("action")
    actor_type = event_data.get("actor", {}).get("type")
    actor_id = event_data.get("actor", {}).get("id")
    payload = event_data.get("payload")
    correlation_id = event_data.get("correlation_id")
    metrics = event_data.get("metrics")
    
    # Validate required fields
    if not all([entity_type, entity_id, action, actor_type, actor_id]):
        raise HTTPException(status_code=400, detail="Missing required event fields")
    
    # Publish the event
    event = await event_bus.publish(
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        actor_type=actor_type,
        actor_id=actor_id,
        payload=payload,
        correlation_id=correlation_id,
        metrics=metrics
    )
    
    return {"status": "success", "event_id": str(event.event_id)}


@router.post("/definition", response_model=EventDefinitionResponse)
async def register_event_definition(
    definition: Any,
    request: Request,
):
    """Register a new event definition."""
    # if not hasattr(request.app.state, "event_bus"):
    #     raise HTTPException(status_code=500, detail="Event bus not initialized")

    from ..event_bus import event_bus

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

    from ..event_bus import event_bus

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

    from ..event_bus import event_bus

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

    from ..event_bus import event_bus

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
