import logging
from typing import Any, Dict, List, Set
from contextlib import AsyncExitStack

# Import the classes we need to patch
from faststream.broker.subscriber.usecase import SubscriberUsecase
from faststream.utils.context.repository import context

logger = logging.getLogger(__name__)

# Track patched instances to avoid duplicate patching
_PATCHED_INSTANCES: Set[int] = set()
_PATCH_APPLIED = False
original_process_message = None


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
                        
                        # Handle response publishing if needed - USE SAFE APPROACH
                        # First get publishers from handler
                        publishers = []
                        
                        # Safely try to get response publishers
                        try:
                            # Check if get_response_publisher method exists
                            if hasattr(self, "get_response_publisher"):
                                # Public method version
                                pubs = self.get_response_publisher(message)
                                if pubs:
                                    publishers.extend(pubs)
                            elif hasattr(self, "_get_response_publisher"):
                                # Private method version
                                pubs = self._get_response_publisher(message)
                                if pubs:
                                    publishers.extend(pubs)
                            elif hasattr(self, "__get_response_publisher"):
                                # Double underscore private method
                                pubs = self.__get_response_publisher(message)
                                if pubs:
                                    publishers.extend(pubs)
                        except Exception as e:
                            logger.debug(f"Error getting response publishers: {e}")
                        
                        # Add handler-specific publishers
                        if hasattr(h, "handler") and hasattr(h.handler, "_publishers"):
                            publishers.extend(h.handler._publishers)
                        
                        # Publish responses
                        for p in publishers:
                            try:
                                await p.publish(
                                    result_msg.body,
                                    **result_msg.as_publish_kwargs(),
                                    _extra_middlewares=[m.publish_scope for m in middlewares[::-1]]
                                )
                            except Exception as e:
                                logger.warning(f"Error publishing response: {e}")
                        
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
    
    # Only patch once
    if _PATCH_APPLIED:
        logger.info("FastStream patches already applied, skipping")
        return
        
    logger.info("Applying FastStream patches to improve message handling")
    
    # Store original method for unpatching if needed
    original_process_message = SubscriberUsecase.process_message
    
    # Patch the class method
    SubscriberUsecase.process_message = improved_process_message
    
    # Also patch any existing instances
    patch_existing_instances()
    
    _PATCH_APPLIED = True
    logger.info("Successfully patched FastStream SubscriberUsecase.process_message")

def patch_existing_instances():
    """Find and patch any existing SubscriberUsecase instances."""
    import gc
    
    # Get all SubscriberUsecase instances
    objects = [obj for obj in gc.get_objects() 
               if isinstance(obj, SubscriberUsecase) 
               and id(obj) not in _PATCHED_INSTANCES]
    
    for obj in objects:
        # Skip already patched instances
        if obj.process_message.__name__ == "improved_process_message":
            continue
            
        # Replace the method on the instance
        obj.process_message = improved_process_message.__get__(obj, SubscriberUsecase)
        
        # Track that we've patched this instance
        _PATCHED_INSTANCES.add(id(obj))
        
    logger.info(f"Patched {len(objects)} existing SubscriberUsecase instances")

def reset_faststream_patches():
    """Reset FastStream patches if needed."""
    global _PATCH_APPLIED, original_process_message
    if not original_process_message:
        logger.info("No patches to reset")
        return
    
    if not _PATCH_APPLIED:
        return
        
    # Restore original method
    SubscriberUsecase.process_message = original_process_message
    
    # Clear tracked instances
    _PATCHED_INSTANCES.clear()
    
    _PATCH_APPLIED = False
    logger.info("Reset FastStream patches")


def apply_all_patches():
    """Apply all patches to improve FastStream behavior."""
    # Apply FastStream message handling patches
    apply_faststream_patches()
