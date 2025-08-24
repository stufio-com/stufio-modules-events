import logging
import os
from typing import Any, Dict, List, Set
from contextlib import AsyncExitStack

# Import the classes we need to patch
from faststream.broker.subscriber.usecase import SubscriberUsecase
from faststream.utils.context.repository import context
from stufio.modules.events import HandlerResponse

logger = logging.getLogger(__name__)

# Track patched instances to avoid duplicate patching
_PATCHED_INSTANCES: Set[int] = set()
_PATCH_APPLIED = False
original_process_message = None

# Check if we're in testing mode to avoid aggressive patching
IS_TESTING = os.getenv('TESTING', '').lower() in ('1', 'true', 'yes') or 'pytest' in os.getenv('_', '')


# Define the improved process_message method
async def improved_process_message(self, msg: Any) -> Any:
    """
    Improved process_message that calls all matching handlers and handles
    missing handlers gracefully.
    """
    if not self.running:
        return None

    cache: Dict[Any, Any] = {}
    parsing_errors = []
    results = []
    suitable_handlers_found = False

    async with AsyncExitStack() as stack:
        # Setup common context
        stack.enter_context(self.lock)

        # Add extra context
        for k, v in self.extra_context.items():
            stack.enter_context(context.scope(k, v))

        stack.enter_context(context.scope("handler_", self))

        # Setup middlewares once
        middlewares = []
        for base_m in self._broker_middlewares:
            middleware = base_m(msg)
            middlewares.append(middleware)
            await middleware.__aenter__()
            stack.push_async_exit(middleware.__aexit__)

        # Add this right after setting up the context for message:
        try:
            # Apply correlation ID middleware first
            from stufio.modules.events.consumers.kafka import correlation_id_middleware
            await correlation_id_middleware(msg)
        except Exception as e:
            logger.warning(f"Error applying correlation_id_middleware: {e}")

        # Check all handlers
        for h in self.calls:
            try:
                message = await h.is_suitable(msg, cache)

                if message is not None:
                    suitable_handlers_found = True

                    # Set up watcher context for this specific handler
                    watcher_context = await stack.enter_async_context(
                        self.watcher(message, **self.extra_watcher_options)
                    )

                    # Add message-specific context
                    with context.scope("log_context", self.get_log_context(message)), \
                         context.scope("message", message):

                        # Call the handler and get result
                        # HandlerResponse is now a subclass of FastStreamResponse,
                        # so FastStream's ensure_response will work correctly
                        result_msg = await h.call(
                            message=message,
                            _extra_middlewares=(m.consume_scope for m in middlewares[::-1])
                        )

                        # Set correlation ID if needed
                        if not result_msg.correlation_id:
                            result_msg.correlation_id = message.correlation_id

                        if isinstance(result_msg, HandlerResponse):
                            result_msg.after_consume(
                                msg, getattr(h, "call_name", "unknown")
                            )

                        # Collect result
                        results.append(result_msg)

            except Exception as e:
                parsing_errors.append(e)
                logger.warning(f"Error in handler {getattr(h, 'call_name', 'unknown')}: {e}")

        # Handle the case where no suitable handlers were found
        if not suitable_handlers_found:
            # Extract headers for better logging
            headers = {}
            try:
                if hasattr(msg, "raw_message") and hasattr(msg.raw_message, "headers"):
                    headers = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                              v.decode('utf-8') if isinstance(v, bytes) else v 
                              for k, v in (msg.raw_message.headers or [])}
            except Exception:
                pass

            # Log instead of raising exception
            logger.info(f"No matching handler for message. Headers: {headers}")

            # If there were parsing errors, log them at debug level
            if parsing_errors:
                logger.debug(f"Parsing errors encountered: {parsing_errors}")

    # Return results: either list of results or None
    if results:
        if len(results) == 1:
            return results[0]  # For backward compatibility

        return results

    # Use standard ensure_response for empty responses
    from faststream.broker.response import ensure_response
    return ensure_response(None)  # Return empty response

def apply_faststream_patches():
    """Apply patches to FastStream to improve message handling."""
    global _PATCH_APPLIED, original_process_message
    
    # Skip patching in testing environments to avoid teardown issues
    if IS_TESTING:
        logger.debug("Skipping FastStream patches in testing environment")
        return
    
    # Only patch once
    if _PATCH_APPLIED:
        logger.debug("FastStream patches already applied, skipping")
        return
        
    try:
        logger.info("Applying FastStream patches to improve message handling")
        
        # Store original method for unpatching if needed
        original_process_message = SubscriberUsecase.process_message
        
        # Patch the class method
        SubscriberUsecase.process_message = improved_process_message
        
        # Also patch any existing instances (but be careful in testing)
        patch_existing_instances()
        
        _PATCH_APPLIED = True
        logger.info("Successfully patched FastStream SubscriberUsecase.process_message")
    except Exception as e:
        logger.error(f"Failed to apply FastStream patches: {e}")
        # Reset state on failure
        _PATCH_APPLIED = False

def patch_existing_instances():
    """Find and patch any existing SubscriberUsecase instances."""
    import gc
    
    # Get all SubscriberUsecase instances with error handling for weak references
    objects = []
    try:
        for obj in gc.get_objects():
            try:
                if isinstance(obj, SubscriberUsecase) and id(obj) not in _PATCHED_INSTANCES:
                    objects.append(obj)
            except (ReferenceError, TypeError):
                # Skip objects that have been garbage collected or have weak references
                continue
    except Exception as e:
        logger.debug(f"Error during object collection: {e}")
        return
    
    patched_count = 0
    for obj in objects:
        try:
            # Skip already patched instances
            if hasattr(obj, 'process_message') and obj.process_message.__name__ == "improved_process_message":
                continue
                
            # Replace the method on the instance
            obj.process_message = improved_process_message.__get__(obj, SubscriberUsecase)
            
            # Track that we've patched this instance
            _PATCHED_INSTANCES.add(id(obj))
            patched_count += 1
        except (ReferenceError, AttributeError, TypeError):
            # Skip objects that can't be patched (e.g., already being cleaned up)
            continue
        
    logger.info(f"Patched {patched_count} existing SubscriberUsecase instances")

def reset_faststream_patches():
    """Reset FastStream patches if needed."""
    global _PATCH_APPLIED, original_process_message
    
    try:
        if not original_process_message:
            logger.debug("No patches to reset")
            return
        
        if not _PATCH_APPLIED:
            logger.debug("Patches not applied, nothing to reset")
            return
            
        # Restore original method
        SubscriberUsecase.process_message = original_process_message
        
        # Clear tracked instances
        _PATCHED_INSTANCES.clear()
        
        _PATCH_APPLIED = False
        logger.info("Reset FastStream patches")
    except Exception as e:
        logger.debug(f"Error during patch reset: {e}")
        # Still try to reset the state
        _PATCH_APPLIED = False
        _PATCHED_INSTANCES.clear()


def apply_all_patches():
    """Apply all patches to improve FastStream behavior."""
    # Apply FastStream message handling patches
    apply_faststream_patches()
