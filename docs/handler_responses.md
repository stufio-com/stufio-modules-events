# Event Handler Response System

This document describes the standardized event handler response system in the Stufio Events framework.

## Overview

Event handlers can now return a standardized `HandlerResponse` object that enables:

1. **Metrics collection** - Track performance and business metrics automatically
2. **Result passing** - Return data from your handler in a structured way
3. **Event chaining** - Publish follow-up events as a response to the current event

## Handler Response Schema

The base schema for handler responses is:

```python
class HandlerResponse(BaseModel, Generic[T]):
    # Optional result data to return
    result: Optional[T] = None
    
    # Metrics collected during execution
    metrics: Optional[HandlerMetrics] = None
    
    # Events to publish as a result of this handler
    publish: Optional[List[PublishEventInfo]] = None
```

### Returning Metrics

Every handler can return metrics data that will be automatically collected:

```python
# Method 1: Use the add_metrics helper
return HandlerResponse().add_metrics({
    "items_processed": 42,
    "processing_time_ms": 135
})

# Method 2: Direct initialization
return HandlerResponse(
    metrics={"items_processed": 42, "processing_time_ms": 135}
)
```

### Returning Results

Your handler can return data that will be passed back to the caller:

```python
# Return a result with no metrics
return HandlerResponse.with_result({"status": "completed"})

# Return both result and metrics
return HandlerResponse(
    result={"status": "completed"},
    metrics={"items_processed": 42}
)
```

### Publishing Follow-up Events

The most powerful feature is the ability to publish additional events:

```python
return HandlerResponse(
    publish=[
        PublishEventInfo(
            event_type=NotificationEvent,
            entity_id="user-123",
            payload={"user_id": "123", "message": "Task completed"},
            correlation_id=event.correlation_id  # Maintain correlation chain
        ),
        PublishEventInfo(
            event_type=AuditEvent,
            entity_id="audit-456",
            payload={"action": "user_task_completed", "details": {...}}
        )
    ]
)
```

## Integration with Metrics System

All events automatically collect and store:

1. MongoDB, ClickHouse, and Redis database metrics
2. Custom metrics returned by your handler
3. Processing time and success/failure indicators

### Custom Metrics Providers

You can create custom metrics providers to track application-specific metrics:

1. Create a metrics provider class that extends `BaseMetricsProvider`
2. Register it using the `@register_metrics_provider` decorator
3. Use it in your application code to track metrics

The metrics system will automatically discover providers from:

- Stufio framework modules
- Your application modules registered in the module registry
- Custom providers in your application's metrics modules

Example metrics provider implementation:

```python
from stufio.modules.events.metrics.registry import register_metrics_provider
from stufio.modules.events.metrics.providers import BaseMetricsProvider

@register_metrics_provider
class MyServiceMetricsProvider(BaseMetricsProvider):
    provider_name = "my_service"  # Required unique name
    
    def __init__(self, **config):
        super().__init__(**config)
        self.reset_state()
    
    def reset_state(self):
        # Initialize metrics storage for this request context
        self._context = contextvars.ContextVar('my_service_metrics', default={
            "api_calls": 0,
            "processing_time_ms": 0,
            "errors": 0
        })
    
    def track_api_call(self, endpoint: str, duration_ms: int, error: bool = False):
        metrics = self._context.get()
        metrics["api_calls"] += 1
        metrics["processing_time_ms"] += duration_ms
        if error:
            metrics["errors"] += 1
        self._context.set(metrics)
    
    async def get_metrics(self) -> Dict[str, Any]:
        metrics = self._context.get()
        return {
            "api_calls": metrics["api_calls"],
            "processing_time_ms": metrics["processing_time_ms"],
            "errors": metrics["errors"],
            "avg_time_ms": metrics["processing_time_ms"] / metrics["api_calls"] if metrics["api_calls"] > 0 else 0
        }
    
    async def reset_metrics(self) -> None:
        self.reset_state()
```

### Where to Place Custom Metrics Providers

For best organization, place your metrics providers in one of these locations:

1. **Recommended**: Create a `metrics` submodule in your application module:
   ```
   your_module/
     __init__.py
     metrics/
       __init__.py
       my_provider.py
   ```

2. In your module's main package:
   ```
   your_module/
     __init__.py  # Import and register metrics providers
     metrics.py   # Define your providers here
   ```

The metrics registry will automatically discover providers in these locations.

## Example Usage

Here's a complete example of a handler that processes an order and publishes follow-up events:

```python
@stufio_event_subscriber(event=OrderCreatedEvent)
async def handle_order_created(event: BaseEventMessage[OrderPayload]) -> HandlerResponse:
    """Process a new order."""
    try:
        # Process the order
        order = event.payload
        order_id = await orders.create_order(order)
        
        # Return response with metrics and follow-up events
        return HandlerResponse(
            # Return the processed order ID
            result={"order_id": order_id},
            
            # Track metrics
            metrics={
                "success": True,
                "order_total": order.total_amount,
                "items_count": len(order.items)
            },
            
            # Publish follow-up events
            publish=[
                # Notify fulfillment system
                PublishEventInfo(
                    event_type=OrderFulfillmentEvent,
                    entity_id=order_id,
                    payload={
                        "order_id": order_id,
                        "shipping_address": order.shipping_address
                    },
                    correlation_id=event.correlation_id
                ),
                # Notify user
                PublishEventInfo(
                    event_type=UserNotificationEvent,
                    entity_id=order.user_id,
                    payload={
                        "user_id": order.user_id,
                        "message": f"Order #{order_id} received"
                    },
                    correlation_id=event.correlation_id
                )
            ]
        )
    except Exception as e:
        # Track failure metrics
        return HandlerResponse().add_metrics({
            "success": False,
            "error": str(e)
        })
```

## Legacy Support

For backward compatibility, handlers can still return:

1. **Dictionary with metrics** - `{"metrics": {...}}` 
2. **Dictionary with result** - `{"result": {...}}`
3. **Dictionary with publish** - `{"publish": [{"event_type": EventClass, "payload": ...}]}`
4. **Direct values** - These are treated as the result

## Best Practices

1. Always include success/failure in your metrics
2. Maintain correlation IDs across event chains 
3. Use type annotations for better IDE support
4. Handle exceptions and return appropriate metrics
5. Place metrics providers in dedicated modules for better organization
6. Use the automatic discovery system instead of manual registration
7. Follow a consistent metrics structure across your application