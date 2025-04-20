from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
import time
import logging
import uuid
import json
import asyncio
import traceback
from ..schemas.base import ActorType
from ..schemas.payloads import SystemErrorPayload, APIRequestPayload
from ..events import SystemErrorEvent, APIRequestEvent
from ..schemas.error import ErrorLogCreate  
from ..crud import crud_error_log

logger = logging.getLogger(__name__)


class EventMiddleware(BaseHTTPMiddleware):
    """Middleware to track API requests as events."""
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract the real client IP from request headers"""
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            # Get the first IP if multiple are provided
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    async def dispatch(self, request: Request, call_next):
        # Generate request ID as correlation ID
        correlation_id = str(uuid.uuid4())
        request.state.correlation_id = correlation_id

        # Start timing
        start_time = time.time()
        
        # Track response and any exceptions
        response = None
        has_error = False
        
        try:
            # Process the request, capture any exceptions
            response = await call_next(request)
            # return response  # Return response immediately after successful processing
            
        # except RuntimeError as e:
        #     # Special handling for "No response returned" errors
        #     if "No response returned" in str(e):
        #         logger.error(f"No response returned from endpoint handler: {request.url.path}")
        #         response = JSONResponse(
        #             status_code=500, 
        #             content={"detail": "The application encountered a stream error"}
        #         )
        #         has_error = True
        #     else:
        #         # Re-raise other RuntimeErrors
        #         raise
                
        except Exception as e:
            # Log the error
            logger.exception(f"Error in middleware chain: {e}")
            
            # Set flag to indicate error condition
            has_error = True
            
            # Create a fallback response
            response = JSONResponse(
                status_code=500,
                content={"detail": "Internal server error"}
            )
            
            # Create a detailed error log
            try:
                user_id = "anonymous"
                if hasattr(request.state, "user") and hasattr(request.state.user, "id"):
                    user_id = str(request.state.user.id)
                else: 
                    # Extract user ID from request if available
                    client_ip = self._get_client_ip(request)
                    user_id = f"anon-{client_ip}"
                    
                # Calculate processing time
                process_time = time.time() - start_time
                process_time_ms = int(process_time * 1000)
                
                # Log detailed error information
                error_stack = traceback.format_exc()
                
                # Extract request data (safely)
                request_data = None
                if request.method in ["POST", "PUT", "PATCH"]:
                    try:
                        body = await request.body()
                        if body:
                            request_data = json.dumps({
                                "body": body.decode('utf-8')[:1000],  # Limit size
                                "headers": dict(request.headers),
                                "query_params": dict(request.query_params)
                            })
                    except Exception:
                        pass
                else:
                    # Convert to JSON string for non-body requests as well
                    request_data = json.dumps({
                        "headers": dict(request.headers),
                        "query_params": dict(request.query_params)
                    })
                
                # Create error log entry
                error_log = ErrorLogCreate(
                    correlation_id=uuid.UUID(correlation_id),
                    error_type="middleware_exception",
                    severity="critical",
                    source="middleware",
                    error_message=str(e),
                    error_stack=error_stack,
                    request_path=request.url.path,
                    request_method=request.method,
                    status_code=500,
                    request_data=request_data,
                    actor_id=user_id
                )
                
                # Save error log to ClickHouse directly
                asyncio.create_task(crud_error_log.create(error_log))
                
                # Track as API request for consistency
                path = request.url.path
                asyncio.create_task(self._track_request(
                    request=request,
                    response=response,
                    correlation_id=correlation_id,
                    user_id=user_id,
                    path=path,
                    process_time_ms=process_time_ms,
                    error_exception=e
                ))
                    
            except Exception as tracking_error:
                logger.error(f"Failed to track middleware error: {tracking_error}")
                
        finally:
            # If we got here with an error and a valid response, return it 
            if has_error and response is not None:
                # Calculate processing time for tracking
                process_time = time.time() - start_time
                process_time_ms = int(process_time * 1000)
                
                # Extract user ID
                user_id = "anonymous"
                if hasattr(request.state, "user") and hasattr(request.state.user, "id"):
                    user_id = str(request.state.user.id)
                else:
                    client_ip = self._get_client_ip(request)
                    user_id = f"anon-{client_ip}"
                    
                # Track error asynchronously - don't await this
                path = request.url.path
                asyncio.create_task(self._track_request(
                    request=request,
                    response=response,
                    correlation_id=correlation_id,
                    user_id=user_id,
                    path=path,
                    process_time_ms=process_time_ms
                ))
                
                return response

        # This code shouldn't be reached in normal flow, but just in case
        if response is None:
            return JSONResponse(
                status_code=500,
                content={"detail": "Unexpected middleware state - no response"}
            )
            
        return response

    async def _track_request(self, request: Request, response: Response, 
                            correlation_id: str, user_id: str, path: str, 
                            process_time_ms: int, error_exception: Exception = None):
        """Track the request as an event using strongly typed APIRequestEvent."""
        try:
            # Extract data for event
            method = request.method
            status_code = response.status_code
            
            # Extract the entity type from the URL path
            path_parts = path.strip("/").split("/")
            entity_id = "endpoint"
            
            if len(path_parts) >= 2:
                # First part after / is usually the API version or 'api'
                if len(path_parts) >= 3:
                    entity_id = path_parts[2]  # Often the resource type
            
            # Check if response has an error status code
            error_info = None
            if status_code >= 400:
                # Try to extract error details from response body
                error_info = {
                    "status_code": status_code,
                    "type": "http_error",
                }
                
                # Try to get error details from response
                try:
                    # For JSON responses, extract the body
                    response_body = None
                    if hasattr(response, "body"):
                        try:
                            body = getattr(response, "body", None)
                            if body:
                                response_body = json.loads(body.decode('utf-8'))
                        except Exception as e:
                            logger.debug(f"Could not access response body: {e}")
                    if isinstance(response_body, dict) and "detail" in response_body:
                        error_info["message"] = response_body["detail"]
                except Exception as e:
                    logger.debug(f"Could not extract error details: {e}")
            
            # Add headers and query params (sanitized) to request events
            headers = {k: v for k, v in request.headers.items() 
                      if k.lower() not in ('authorization', 'cookie')}

            # Extract user agent and remote IP
            user_agent = request.headers.get("user-agent")
            remote_ip = request.client.host if request.client else None
            
            # Safely get response size - don't access response.body directly
            response_size = None
            if hasattr(response, "body"):
                try:
                    body = getattr(response, "body", None)
                    if body is not None:
                        response_size = len(body)
                except Exception:
                    pass
            
            # Create API request payload with strong typing
            payload = APIRequestPayload(
                method=method,
                path=path,
                status_code=status_code,
                headers=headers,
                query_params=dict(request.query_params),
                error=error_info,
                duration_ms=process_time_ms,
                response_size_bytes=response_size,
                user_id=user_id,
                user_agent=user_agent,
                remote_ip=remote_ip
            )
            
            # Metrics for performance tracking
            metrics = {
                "total_time_ms": process_time_ms,
                "response_size_bytes": response_size or 0,
            }
            
            # Publish the API request event using the strongly typed event
            await APIRequestEvent.publish(
                entity_id=entity_id,
                actor_type=ActorType.USER,
                actor_id=user_id,
                payload=payload,
                correlation_id=correlation_id,
                metrics=metrics
            )
            
            # For error status codes, also publish a dedicated error event
            if status_code >= 500:
                await SystemErrorEvent.publish(
                    entity_id="api_error",
                    actor_type=ActorType.SYSTEM,
                    actor_id="middleware",
                    payload=SystemErrorPayload(
                        error_type="http_error",
                        error_message=error_info.get("message", "Server error") if error_info else "Server error",
                        severity="error",
                        correlation_id=correlation_id,
                    ),
                )
        except Exception as e:
            logger.error(f"Critical error in event tracking: {e}", exc_info=True)

            # Inside error handling:
            await SystemErrorEvent.publish(
                entity_id="api_error",
                actor_type=ActorType.SYSTEM,
                actor_id="middleware",
                payload=SystemErrorPayload(
                    error_type="http_error",
                    error_message=error_info.get("message", "Server error"),
                    severity="error"
                ),
                correlation_id=correlation_id
            )
