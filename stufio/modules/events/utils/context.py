"""Context management utilities for event tracking."""

from contextvars import ContextVar
from typing import Optional, Union, Any, Coroutine
from uuid import UUID as UUID4, uuid4
import asyncio

class TaskContext:
    """Context manager for managing task-level context."""

    _context = ContextVar("task_context", default={})

    @classmethod
    def get_correlation_id(cls) -> UUID4:
        """Get the correlation ID for the current task.
        
        If no correlation ID exists in the current context, generates one
        and stores it in the context to ensure the same ID is used throughout
        the request lifecycle.
        """
        context = cls._context.get()
        corr_id = context.get("correlation_id")
        
        if corr_id is None:
            # Only generate new UUID if one doesn't exist
            corr_id = uuid4()
            context["correlation_id"] = corr_id
            cls._context.set(context)
        elif isinstance(corr_id, str):
            # Convert string to UUID if needed
            try:
                corr_id = UUID4(corr_id)
                context["correlation_id"] = corr_id
                cls._context.set(context)
            except ValueError:
                # If invalid UUID string, use existing or generate new
                if "correlation_id" in context:
                    corr_id = context["correlation_id"]
                else:
                    corr_id = uuid4()
                    context["correlation_id"] = corr_id
                    cls._context.set(context)
                
        return corr_id

    @classmethod
    def set_correlation_id(cls, correlation_id: Union[str, UUID4]) -> None:
        """Set the correlation ID for the current task."""
        context = cls._context.get()
        
        # Convert string to UUID if needed
        if isinstance(correlation_id, str):
            try:
                correlation_id = UUID4(correlation_id)
            except ValueError:
                # If invalid UUID string, get existing or generate new
                correlation_id = cls.get_correlation_id()
                
        context["correlation_id"] = correlation_id
        cls._context.set(context)

    @classmethod
    def clear(cls) -> None:
        """Clear the current task context."""
        cls._context.set({})

    @classmethod
    def get_context(cls) -> dict:
        """Get the current task context."""
        return cls._context.get().copy()

    @classmethod
    def set_context(cls, **kwargs) -> None:
        """Set multiple context values at once."""
        context = cls._context.get()
        context.update(kwargs)
        cls._context.set(context)
        
    @classmethod
    async def run_task(
        cls, 
        coro: Coroutine[Any, Any, Any], 
        *, 
        correlation_id: Optional[Union[str, UUID4]] = None
    ) -> Any:
        """
        Run a coroutine with the specified correlation ID in its context.
        
        This ensures that the correlation ID is propagated to the task and
        is available through get_correlation_id() within the task.
        
        Args:
            coro: The coroutine to run
            correlation_id: Optional correlation ID to associate with the task.
                            If None, an existing correlation ID will be used or
                            a new one will be generated.
                            
        Returns:
            The result of the coroutine execution
        """
        # Save the current context
        old_context = cls.get_context()
        
        try:
            # Set up the new context with the correlation ID if provided
            if correlation_id is not None:
                cls.set_correlation_id(correlation_id)
            else:
                # Use existing correlation ID or generate one if needed
                cls.get_correlation_id()
                
            # Run the coroutine with the established context
            return await coro
        finally:
            # Restore the original context
            cls._context.set(old_context)