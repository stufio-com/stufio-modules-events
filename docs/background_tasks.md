# Background Tasks with Correlation ID Support

This module provides utilities for running background tasks with correlation ID propagation, which enables consistent request tracing and logging across asynchronous operations.

## Overview

When working with asynchronous tasks, maintaining context information such as correlation IDs is essential for proper request tracing and debugging. The background tasks module provides simple utilities to:

1. Run background tasks with a consistent correlation ID
2. Propagate existing correlation IDs to child tasks
3. Handle errors in background tasks gracefully

## Core Components

- `TaskContext`: Manages the correlation ID context
- `run_background_task()`: Executes a task with correlation ID context
- `create_background_task()`: Creates an asyncio.Task with correlation ID context

## Usage Examples

### Basic Usage

```python
from stufio.modules.events.services.background_tasks import create_background_task

async def my_handler():
    # This will generate a new correlation ID if one doesn't exist
    task = create_background_task(process_data, data_item)
    return {"status": "processing"}

async def process_data(data):
    # The correlation ID from the parent context is automatically propagated
    # All logging and events in this task will have the same correlation ID
    result = await compute_result(data)
    await save_result(result)
```

### Using an Explicit Correlation ID

```python
from stufio.modules.events.services.background_tasks import create_background_task
from uuid import uuid4

async def my_api_handler(request_id: str):
    # Use the request ID as the correlation ID for better tracing
    task = create_background_task(
        process_request,
        request_data,
        correlation_id=request_id
    )
    return {"status": "processing", "request_id": request_id}
```

### Getting the Correlation ID Within a Task

```python
from stufio.modules.events.utils.context import TaskContext

async def process_data():
    # Get the correlation ID that was propagated to this task
    correlation_id = TaskContext.get_correlation_id()
    
    # Use it in logs or other context
    logger.info(f"Processing data with correlation ID: {correlation_id}")
    
    # The same correlation ID will be available in any async functions called from here
    await do_more_processing()
```

### Error Handling

Background tasks automatically log exceptions, but you can add custom error handling:

```python
from stufio.modules.events.services.background_tasks import run_background_task

async def my_error_handling_task():
    try:
        await run_background_task(
            risky_operation,
            important_data
        )
    except Exception as e:
        # Additional error handling beyond the automatic logging
        await notify_admin(f"Background task failed: {e}")
```

## Best Practices

1. **Always use create_background_task()** instead of asyncio.create_task() for better tracing
2. **Prefer passing correlation_id** explicitly when you have a meaningful ID (like user request ID)
3. **Access correlation ID with TaskContext.get_correlation_id()** within your task functions
4. **Add appropriate error handling** for critical background operations
5. **Keep background tasks focused** on a single responsibility