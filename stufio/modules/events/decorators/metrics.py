import time
import asyncio
import functools
import json
from typing import Any, Callable, Dict, Optional, Union
from uuid import UUID
from datetime import datetime

from ..utils.context import TaskContext
from ..utils.timestamps import parse_event_timestamp
from ..schemas.messages import BaseEventMessage
from ..schemas.handler import HandlerResponse
from stufio.core.config import get_settings

# Import the CRUD class for event metrics
from ..crud import crud_event_metrics
from ..schemas.event_metrics import EventMetricsCreate

# Import new metrics system
from ..metrics.registry import get_all_metrics, reset_all_metrics


settings = get_settings()

async def save_event_metrics(
    event_id: Union[str, UUID],
    correlation_id: Union[str, UUID, None],
    source_type: str,  # 'consumer' or 'api'
    consumer_name: Optional[str] = None,
    module_name: Optional[str] = None,
    event_timestamp: Optional[float] = None,
    started_at: float = None,
    completed_at: float = None,
    success: bool = True,
    error_message: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None
) -> None:
    """Save event metrics using CRUD pattern."""
    try:
        # Get application name from settings
        tenant = getattr(settings, "APP_NAME", "unknown")

        # Get metrics from all registered providers
        provider_metrics = await get_all_metrics()

        # Format values
        duration_ms = int((completed_at - started_at) * 1000)

        # Calculate latency (time from event creation to processing)
        latency_ms = 0
        if event_timestamp:
            latency_ms = int((started_at - event_timestamp) * 1000) if started_at > event_timestamp else 0

        # Extract standard metrics from providers
        mongodb_metrics = provider_metrics.get("mongodb", {})
        mongodb_queries = mongodb_metrics.get("queries", 0)
        mongodb_time_ms = int(mongodb_metrics.get("time_ms", 0))
        mongodb_slow_queries = mongodb_metrics.get("slow_queries", 0)

        clickhouse_metrics = provider_metrics.get("clickhouse", {})
        clickhouse_queries = clickhouse_metrics.get("queries", 0)
        clickhouse_time_ms = int(clickhouse_metrics.get("time_ms", 0))
        clickhouse_slow_queries = clickhouse_metrics.get("slow_queries", 0)

        redis_metrics = provider_metrics.get("redis", {})
        redis_operations = redis_metrics.get("operations", 0)
        redis_time_ms = int(redis_metrics.get("time_ms", 0))
        redis_slow_operations = redis_metrics.get("slow_operations", 0)

        # Prepare custom metrics with any additional providers or custom metrics
        all_metrics = {}

        # Add metrics from handler if specified
        if metrics:
            all_metrics["handler"] = metrics

        # Add metrics from other providers (excluding standard ones)
        for provider_name, provider_data in provider_metrics.items():
            if provider_name not in ("mongodb", "clickhouse", "redis"):
                all_metrics[provider_name] = provider_data

        # Serialize custom metrics to JSON
        custom_metrics_json = None
        if all_metrics:
            try:
                custom_metrics_json = json.dumps(all_metrics)
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"❌ Failed to serialize custom metrics: {e}")

        # Convert timestamps to datetime
        started_at_dt = datetime.fromtimestamp(started_at)
        completed_at_dt = datetime.fromtimestamp(completed_at)

        # Create event metrics object
        event_metrics = EventMetricsCreate(
            event_id=str(event_id),
            correlation_id=str(correlation_id) if correlation_id else None,
            tenant=tenant,
            source_type=source_type,
            consumer_name=consumer_name,
            module_name=module_name,
            timestamp=datetime.utcnow(),
            started_at=started_at_dt,
            completed_at=completed_at_dt,
            duration_ms=duration_ms,
            latency_ms=latency_ms,
            success=success,
            error_message=error_message,
            mongodb_queries=mongodb_queries,
            mongodb_time_ms=mongodb_time_ms,
            mongodb_slow_queries=mongodb_slow_queries,
            clickhouse_queries=clickhouse_queries,
            clickhouse_time_ms=clickhouse_time_ms,
            clickhouse_slow_queries=clickhouse_slow_queries,
            redis_operations=redis_operations,
            redis_time_ms=redis_time_ms,
            redis_slow_operations=redis_slow_operations,
            custom_metrics=custom_metrics_json
        )

        # Save using the CRUD class
        await crud_event_metrics.create_metrics(event_metrics)
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"❌ Failed to save event metrics: {e}", exc_info=True)


def extract_metrics_from_result(result: Any) -> Dict[str, Any]:
    """
    Extract metrics from the handler result, handling various result types.
    
    Args:
        result: The result returned by the handler function
        
    Returns:
        Dict with metrics, or empty dict if no metrics found
    """
    # Case 1: Result is a HandlerResponse
    if isinstance(result, HandlerResponse):
        # Get metrics from HandlerResponse
        if result.handler_metrics:
            # Convert to dict if needed
            if hasattr(result.handler_metrics, "dict"):
                return result.handler_metrics.dict()
            elif hasattr(result.handler_metrics, "model_dump"):
                return result.handler_metrics.model_dump()
            elif isinstance(result.handler_metrics, dict):
                return result.handler_metrics
            
        # If no metrics in handler_metrics, check headers
        if result.headers:
            return dict(result.headers)
        return {}
    
    # Case 2: Result is a dict with 'metrics' key
    if isinstance(result, dict) and "metrics" in result:
        return result["metrics"]
    
    # No metrics found
    return {}

def _extract_event(*args, **kwargs) -> Optional[BaseEventMessage]:
    """Extract event from args or kwargs."""
    # Check args for event
    for arg in args:
        if isinstance(arg, BaseEventMessage):
            return arg

    # Check kwargs for event
    for arg in kwargs.values():
        if isinstance(arg, BaseEventMessage):
            return arg

    return None


def track_handler_metrics(module_name: str = "unknown"):
    """Decorator to track metrics for event handlers."""
    def decorator(func: Callable):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Get event from args - typically first argument for handler
            event = _extract_event(*args, **kwargs)

            if not event:
                return await func(*args, **kwargs)

            # Reset all metrics before handler execution
            await reset_all_metrics()

            # Extract IDs and timestamp for tracking
            event_id = getattr(event, "event_id", None) if event else None
            correlation_id = (
                getattr(event, "correlation_id", TaskContext.get_correlation_id())
                if event
                else TaskContext.get_correlation_id()
            )

            # Extract event timestamp for latency calculation
            event_timestamp = None
            if event and hasattr(event, "timestamp"):
                event_timestamp = parse_event_timestamp(event.timestamp)

            # Get function details
            consumer_name = func.__qualname__
            consumer_module_name = module_name or func.__module__

            # Timing
            start_time = time.time()
            success = True
            error_msg = None

            try:
                # Execute the handler
                result = await func(*args, **kwargs)

                # Extract metrics from the result
                custom_metrics = extract_metrics_from_result(result)

                return result
            except Exception as e:
                success = False
                error_msg = str(e)
                raise
            finally:
                end_time = time.time()

                # Save metrics in the background
                if event_id:
                    asyncio.create_task(
                        save_event_metrics(
                            event_id=event_id,
                            correlation_id=correlation_id,
                            source_type="consumer",
                            consumer_name=consumer_name,
                            module_name=consumer_module_name,
                            event_timestamp=event_timestamp,
                            started_at=start_time,
                            completed_at=end_time,
                            success=success,
                            error_message=error_msg,
                            metrics=custom_metrics,
                        )
                    )

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Get event from args
            event = _extract_event(*args, **kwargs)
            if not event:
                return func(*args, **kwargs)

            # Extract IDs and timestamp for tracking
            event_id = getattr(event, "event_id", None) if event else None
            correlation_id = (
                getattr(event, "correlation_id", TaskContext.get_correlation_id())
                if event
                else TaskContext.get_correlation_id()
            )

            # Extract event timestamp for latency calculation
            event_timestamp = None
            if event and hasattr(event, "timestamp"):
                event_timestamp = parse_event_timestamp(event.timestamp)

            # Get function details
            consumer_name = func.__qualname__
            consumer_module_name = module_name or func.__module__

            # Reset metrics - need to run in event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(reset_all_metrics())
            except Exception:
                pass

            # Timing
            start_time = time.time()
            success = True
            error_msg = None

            try:
                # Execute the handler
                result = func(*args, **kwargs)

                # Extract metrics from the result
                custom_metrics = extract_metrics_from_result(result)

                return result
            except Exception as e:
                success = False
                error_msg = str(e)
                raise
            finally:
                end_time = time.time()

                # Save metrics directly (can't create background task in sync code)
                if event_id:
                    # Use asyncio event loop to save metrics if available
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            loop.create_task(
                                save_event_metrics(
                                    event_id=event_id,
                                    correlation_id=correlation_id,
                                    source_type="consumer",
                                    consumer_name=consumer_name,
                                    module_name=consumer_module_name,
                                    event_timestamp=event_timestamp,
                                    started_at=start_time,
                                    completed_at=end_time,
                                    success=success,
                                    error_message=error_msg,
                                    metrics=custom_metrics,
                                )
                            )
                    except Exception:
                        pass

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper

        return sync_wrapper

    return decorator
