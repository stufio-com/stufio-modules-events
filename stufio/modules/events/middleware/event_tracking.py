from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
import time
import logging
import uuid
import asyncio

logger = logging.getLogger(__name__)


class EventMiddleware(BaseHTTPMiddleware):
    """Middleware to track API requests as events."""

    async def dispatch(self, request: Request, call_next):
        # Generate request ID as correlation ID
        correlation_id = str(uuid.uuid4())
        request.state.correlation_id = correlation_id

        # Start timing
        start_time = time.time()
        
        # Process the request
        response = await call_next(request)
        
        # Calculate processing time
        process_time = time.time() - start_time
        process_time_ms = int(process_time * 1000)
        
        # Skip tracking for certain paths
        path = request.url.path
        if (path.startswith("/docs") or
            path.startswith("/openapi") or
            path.startswith("/redoc") or
            path.startswith("/metrics") or
            path.startswith("/favicon")):
            return response
        
        # Don't track OPTIONS requests
        if request.method == "OPTIONS":
            return response
        
        # Track request as event if app has event_bus and it's not an events endpoint
        if (hasattr(request.app.state, "event_bus") and not path.startswith("/api/events/")):
            # Extract user ID from request if available
            user_id = "anonymous"
            if hasattr(request.state, "user") and hasattr(request.state.user, "id"):
                user_id = str(request.state.user.id)
            
            # Create event asynchronously to avoid blocking response
            asyncio.create_task(self._track_request(
                request=request,
                response=response,
                correlation_id=correlation_id,
                user_id=user_id,
                path=path,
                process_time_ms=process_time_ms
            ))
        
        # Return the response
        return response

    async def _track_request(self, request: Request, response: Response, 
                            correlation_id: str, user_id: str, path: str, 
                            process_time_ms: int):
        """Track the request as an event."""
        try:
            # Get event bus from app state
            event_bus = request.app.state.event_bus
            
            try:
                # Extract data for event
                method = request.method
                status_code = response.status_code
                
                # Create a more RESTful representation of the action
                if method == "GET":
                    action = "read"
                elif method == "POST":
                    action = "create"
                elif method == "PUT" or method == "PATCH":
                    action = "update"
                elif method == "DELETE":
                    action = "delete"
                else:
                    action = method.lower()
                
                # Extract the entity type from the URL path
                # This is a simple implementation - you might want to improve this 
                # based on your API structure
                path_parts = path.strip("/").split("/")
                entity_type = "api"
                entity_id = "endpoint"
                
                if len(path_parts) >= 2:
                    entity_type = path_parts[1]  # Often the resource type
                    if len(path_parts) >= 3:
                        entity_id = path_parts[2]  # Often the resource ID
                
                # Publish the API request event
                await event_bus.publish(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    action=action,
                    actor_type="user", 
                    actor_id=user_id,
                    payload={
                        "extra": {
                            "method": method,
                            "path": path,
                            "status_code": status_code,
                        }
                    },
                    correlation_id=correlation_id,
                    metrics={
                        "processing_time_ms": process_time_ms
                    }
                )
            finally:
                pass
                
        except Exception as e:
            logger.error(f"Error tracking request event: {e}", exc_info=True)