# Stufio Events Module

This module provides event-driven architecture support for the Stufio framework, allowing modules to publish and subscribe to events.

## Features
- Define events with a standard schema
- Publish events to Kafka topics
- Subscribe to events with handler functions
- Store event logs in ClickHouse for auditing and replay
- Support for HTTP webhooks for external subscribers

## Event Schema

Events follow a standard schema:

```json
{
  "event_id": "uuid",
  "correlation_id": "uuid", 
  "timestamp": "ISO8601 timestamp",
  "entity": {
    "type": "order|user|product|other_entity_name",
    "id": "object id"
  },
  "action": "create|update|delete|read|login|logout",
  "actor": {
    "type": "user|admin|system",
    "id": "actor's id"
  },
  "payload": {
    "before": {},
    "after": {},
    "extra": {}
  },
  "metrics": {
    "processing_time_ms": 135,
    "db_time_ms": 12,
    "api_time_ms": 85,
    "queue_time_ms": 5,
    "custom_metrics": {}
  }
}
```

## Defining Events

Create event definitions in your module's events.py file:

```python
from stufio.modules.events.schemas import EventDefinition

class OrderCreatedEvent(EventDefinition):
    name = "order.created"
    entity_type = "order"
    action = "created"
    require_actor = True
    require_entity = True
    description = "Triggered when a new order is created"
    payload_example = {
        "order_id": "12345",
        "total": 100.0,
        "items": [
            {"product_id": "prod1", "quantity": 2, "price": 50.0}
        ],
        "customer_id": "cust1"
    }
```

## Publishing Events

```python
from stufio.modules.events.helpers import publish_event
from stufio.modules.events.schemas import ActorType
from .events import OrderCreatedEvent

async def create_order(user_id, order_data):
    # ... create order in database
    
    await publish_event(
        OrderCreatedEvent,
        entity_id=order_id,
        actor_type=ActorType.USER,
        actor_id=user_id,
        payload={
            "after": order_data,
            "extra": {"source": "web_checkout"}
        }
    )
```

## Subscribing to Events

```python
from stufio.modules.events.helpers import subscribe_to_event

def setup_event_handlers():
    subscribe_to_event("order", "created", handle_order_created)
    
async def handle_order_created(event):
    # Process the event
    order_id = event.entity.id
    order_data = event.payload.after
    # ... handle the event
```

## Advanced Features

- Event replay for testing and recovery
- HTTP webhooks for external subscribers
- Correlation IDs for request tracing
- Performance metrics collection
```

