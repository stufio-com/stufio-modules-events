from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from typing import List, Dict, Any, Optional
from motor.core import AgnosticDatabase
from ..schemas import EventDefinition, EventSubscription, EventMessage, EventLogResponse
from ..models import EventDefinitionModel, EventSubscriptionModel
from clickhouse_connect.driver.asyncclient import AsyncClient
from stufio.api.deps import get_db, get_clickhouse
from datetime import datetime

router = APIRouter()


@router.post("/publish", response_model=Dict[str, Any])
async def publish_event(
    event_data: Dict[str, Any],
    request: Request,
    db: AgnosticDatabase = Depends(get_db)
):
    """Publish a new event."""
    if not hasattr(request.app.state, "event_bus"):
        raise HTTPException(status_code=500, detail="Event bus not initialized")
    
    event_bus = request.app.state.event_bus
    
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


@router.post("/definition", response_model=Dict[str, Any])
async def register_event_definition(
    definition: EventDefinition,
    request: Request,
    db: AgnosticDatabase = Depends(get_db)
):
    """Register a new event definition."""
    if not hasattr(request.app.state, "event_bus"):
        raise HTTPException(status_code=500, detail="Event bus not initialized")
    
    registry = request.app.state.event_bus.registry
    
    try:
        db_definition = await registry.register_event_definition(definition)
        return {
            "status": "success",
            "id": str(db_definition.id),
            "name": db_definition.name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register event definition: {str(e)}")


@router.post("/subscription", response_model=Dict[str, Any])
async def register_subscription(
    subscription: EventSubscription,
    request: Request,
    db: AgnosticDatabase = Depends(get_db)
):
    """Register a new event subscription."""
    if not hasattr(request.app.state, "event_bus"):
        raise HTTPException(status_code=500, detail="Event bus not initialized")
    
    registry = request.app.state.event_bus.registry
    
    try:
        db_subscription = await registry.register_subscription(subscription)
        return {
            "status": "success",
            "id": str(db_subscription.id)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register subscription: {str(e)}")


@router.get("/definitions", response_model=List[Dict[str, Any]])
async def list_event_definitions(
    request: Request,
    db: AgnosticDatabase = Depends(get_db)
):
    """List all event definitions."""
    if not hasattr(request.app.state, "event_bus"):
        raise HTTPException(status_code=500, detail="Event bus not initialized")
    
    engine = request.app.state.event_bus.registry.engine
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


@router.get("/subscriptions", response_model=List[Dict[str, Any]])
async def list_subscriptions(
    request: Request,
    db: AgnosticDatabase = Depends(get_db)
):
    """List all event subscriptions."""
    if not hasattr(request.app.state, "event_bus"):
        raise HTTPException(status_code=500, detail="Event bus not initialized")
    
    engine = request.app.state.event_bus.registry.engine
    subscriptions = await engine.find(EventSubscriptionModel)
    
    result = []
    for subscription in subscriptions:
        result.append({
            "id": str(subscription.id),
            "entity_type": subscription.entity_type,
            "action": subscription.action,
            "module_name": subscription.module_name,
            "callback_url": subscription.callback_url,
            "enabled": subscription.enabled
        })
    
    return result


@router.get("/logs", response_model=List[EventLogResponse])
async def list_event_logs(
    entity_type: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = 100,
    clickhouse_db: AsyncClient = Depends(get_clickhouse)
):
    """List event logs with optional filtering."""
    from ..event_bus import event_log_crud
    
    # Build query filters
    filters = {}
    if entity_type:
        filters["entity_type"] = entity_type
    
    if action:
        filters["action"] = action
    
    # Get events from Clickhouse
    logs = await event_log_crud.get_multi(
        clickhouse_db, 
        filters=filters,
        sort="timestamp DESC",
        limit=limit
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


@router.post("/replay", response_model=Dict[str, Any])
async def replay_events(
    request: Request,
    background_tasks: BackgroundTasks,
    entity_type: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = 50,
):
    """Replay events for testing or recovery purposes."""
    if not hasattr(request.app.state, "event_bus"):
        raise HTTPException(status_code=500, detail="Event bus not initialized")
    
    event_bus = request.app.state.event_bus
    
    # Run replay in background task to avoid blocking
    async def do_replay():
        await event_bus.replay_events(entity_type, action, limit)
    
    background_tasks.add_task(do_replay)
    
    return {
        "status": "success",
        "message": f"Replaying up to {limit} events in the background"
    }