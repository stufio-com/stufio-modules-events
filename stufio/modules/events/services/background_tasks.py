"""Background task processing with correlation ID support."""

import asyncio
import logging
from typing import Any, Callable, Coroutine, Optional
from ..utils.context import TaskContext

logger = logging.getLogger(__name__)

async def run_background_task(
    task: Callable[..., Coroutine[Any, Any, Any]],
    *args,
    correlation_id: Optional[str] = None,
    **kwargs
) -> None:
    """
    Run a background task with correlation ID context.
    
    Args:
        task: The coroutine to run in the background
        correlation_id: Optional correlation ID to associate with the task
        *args: Positional arguments to pass to the task
        **kwargs: Keyword arguments to pass to the task
    """
    try:
        # Run the task with correlation ID context
        await TaskContext.run_task(
            task(*args, **kwargs),
            correlation_id=correlation_id
        )
    except Exception as e:
        logger.error(f"Error in background task: {e}", exc_info=True)

def create_background_task(
    task: Callable[..., Coroutine[Any, Any, Any]],
    *args,
    correlation_id: Optional[str] = None,
    **kwargs
) -> asyncio.Task:
    """
    Create a background task with correlation ID context.
    
    Args:
        task: The coroutine to run in the background
        correlation_id: Optional correlation ID to associate with the task
        *args: Positional arguments to pass to the task
        **kwargs: Keyword arguments to pass to the task
        
    Returns:
        asyncio.Task: The created task
    """
    return asyncio.create_task(
        run_background_task(
            task,
            *args,
            correlation_id=correlation_id,
            **kwargs
        )
    )