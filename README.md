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
from stufio.modules.events.schemas.base import ActorType
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


# Example Usage

```python
# Example in another module (e.g., orders module)
from typing import Dict, Any, Optional, List
from pydantic import BaseModel
from stufio.modules.events.schemas.payloads import BaseEventPayload
from stufio.modules.events.schemas.messages import BaseEventMessage
from stufio.modules.events.schemas.event_definition import EventDefinition, event

# Define payload classes for your events
class OrderItemPayload(BaseModel):
    product_id: str
    quantity: int
    price: float
    
class OrderPayload(BaseModel):
    order_id: str
    customer_id: str
    total_amount: float
    items: List[OrderItemPayload]
    status: str

class OrderCreatedPayload(BaseEventPayload):
    """Payload for order.created events."""
    after: OrderPayload

# Define message class for your events
class OrderCreatedMessage(BaseEventMessage[OrderCreatedPayload]):
    """Message for order.created events."""
    pass

# Define the event using the decorator
@event(
    name="order.created",
    entity_type="order",
    action="created",
    require_actor=True,
    require_entity=True,
    description="Triggered when a new order is created",
    message_class=OrderCreatedMessage,
    payload_class=OrderCreatedPayload,
    payload_example={
        "after": {
            "order_id": "12345",
            "customer_id": "user123",
            "total_amount": 99.99,
            "items": [
                {"product_id": "prod1", "quantity": 2, "price": 49.99}
            ],
            "status": "pending"
        }
    }
)
class OrderCreatedEvent(EventDefinition):
    """Event triggered when a new order is created."""
    pass

# Using the event in your code
from stufio.modules.events.helpers import publish_event

async def create_order(user_id: str, order_data: Dict[str, Any]) -> str:
    # Business logic to create the order
    order_id = "12345"  # Generated ID
    
    # Prepare the strongly-typed payload
    order_payload = OrderCreatedPayload(
        after=OrderPayload(
            order_id=order_id,
            customer_id=user_id,
            total_amount=order_data["total"],
            items=[
                OrderItemPayload(
                    product_id=item["product_id"],
                    quantity=item["quantity"],
                    price=item["price"]
                )
                for item in order_data["items"]
            ],
            status="pending"
        )
    )
    
    # Publish the event with the strongly-typed payload
    await publish_event(
        OrderCreatedEvent,
        entity_id=order_id,
        actor_type="user",
        actor_id=user_id,
        payload=order_payload.model_dump()
    )
    
    return order_id
```
