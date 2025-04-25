"""
Event tracking middleware for Stufio framework.

This middleware provides efficient request tracking, error handling,
and event publishing through a single component.
"""

import asyncio
import json
import traceback
import logging
import time

from fastapi import Request, Response
from fastapi.responses import JSONResponse
import uuid

from ..utils.context import TaskContext
from .base import BaseStufioMiddleware
from ..schemas.base import ActorType
from ..schemas.payloads import SystemErrorPayload, APIRequestPayload
from ..events import SystemErrorEvent, APIRequestEvent
from ..schemas.error import ErrorLogCreate
from ..crud import crud_error_log
from stufio.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


class EventTrackingMiddleware(BaseStufioMiddleware):
    """
    Middleware for tracking API events and handling errors.

    This middleware tracks API requests, errors, and publishes events to Kafka
    without any dependency on the activity module. It also collects database
    metrics when enabled.
    """

    def __init__(self, app, **kwargs):
        super().__init__(app, **kwargs)
        self.track_api_events = kwargs.get("track_api_events", True)
        self.track_errors = kwargs.get("track_errors", True)
        self.include_request_body = kwargs.get("include_request_body", False)
        self.include_auth_status = kwargs.get("include_auth_status", True)
        self.track_db_metrics = getattr(settings, "DB_METRICS_ENABLE", False)

        # Start metrics collection background task if enabled
        if self.track_db_metrics:
            self._start_metrics_collection()

    def _start_metrics_collection(self):
        """Start the database metrics collection if enabled"""
        try:
            from stufio.db.metrics import start_metrics_collection
            # Use asyncio.create_task to start the background task
            asyncio.create_task(start_metrics_collection())
            logger.info("Started database metrics collection in EventTrackingMiddleware")
        except ImportError:
            logger.warning("Database metrics module not available")
        except Exception as e:
            logger.error(f"Failed to start database metrics collection: {e}", exc_info=True)

    async def _handle_exception(
        self, request: Request, exception: Exception
    ) -> Response:
        """Handle exceptions and publish appropriate error events."""
        # Create a fallback response
        response = JSONResponse(
            status_code=500, content={"detail": "Internal server error"}
        )

        if self.track_errors:
            try:
                # Extract user ID from request if available
                user_id, _ = await self._extract_user_id(request)
                if not user_id:
                    client_ip = self._get_client_ip(request)
                    user_id = f"anon-{client_ip}"

                # Generate correlation ID
                correlation_id = getattr(
                    request.state, "correlation_id", str(TaskContext.get_correlation_id())
                )

                # Extract error details
                error_stack = traceback.format_exc()

                # Extract request data safely
                request_data = await self._extract_request_data(request)

                # Create error log entry
                error_log = ErrorLogCreate(
                    correlation_id=uuid.UUID(correlation_id),
                    error_type="middleware_exception",
                    severity="critical",
                    source="middleware",
                    error_message=str(exception),
                    error_stack=error_stack,
                    request_path=request.url.path,
                    request_method=request.method,
                    status_code=500,
                    request_data=request_data,
                    actor_id=user_id,
                )

                # Async task to save error log to database
                asyncio.create_task(crud_error_log.create(error_log))

                # Publish system error event
                asyncio.create_task(
                    SystemErrorEvent.publish(
                        entity_id="api_error",
                        actor_type=ActorType.SYSTEM,
                        actor_id="middleware",
                        payload=SystemErrorPayload(
                            error_type="http_error",
                            error_message=str(exception),
                            severity="critical",
                            stacktrace=error_stack,
                        ),
                        correlation_id=correlation_id,
                    )
                )
            except Exception as e:
                logger.error(f"Failed to track error: {e}", exc_info=True)

        return response

    async def _extract_request_data(self, request: Request) -> str:
        """Extract request data for error logging."""
        try:
            data = {}

            # Extract headers (sensitive ones excluded)
            data["headers"] = {
                k: v
                for k, v in request.headers.items()
                if k.lower() not in ("authorization", "cookie")
            }

            # Extract query parameters
            data["query_params"] = dict(request.query_params)

            # Extract body for certain request types
            if self.include_request_body and request.method in ["POST", "PUT", "PATCH"]:
                try:
                    body = await request.body()
                    if body:
                        # Truncate body to avoid storing large amounts of data
                        data["body"] = body.decode("utf-8")[:1000]
                except Exception:
                    pass

            return json.dumps(data)
        except Exception:
            return "{}"

    async def _pre_process(self, request: Request) -> None:
        """Set up any pre-request processing."""
        # Add DB metrics tracking for this request if enabled
        if self.track_db_metrics:
            try:
                # Import metrics module and reset request-specific counters
                from stufio.db.metrics import reset_request_metrics
                # Store initial state and timestamp
                request.state.db_metrics_start_time = time.time()
                # Reset per-request metrics counters
                await reset_request_metrics()
            except ImportError:
                logger.debug("Database metrics module not available")
            except Exception as e:
                logger.error(f"Error initializing database metrics: {e}", exc_info=True)

    async def _post_process(
        self, request: Request, response: Response, process_time: float
    ) -> None:
        """
        Track request metrics and publish events after processing.
        """
        # Collect database metrics if enabled
        db_metrics = {}
        if self.track_db_metrics:
            try:
                # Import metrics module
                from stufio.db.metrics import get_request_metrics

                # Get metrics for this specific request
                request_metrics = await get_request_metrics()

                # Extract detailed metrics
                clickhouse_metrics = request_metrics["clickhouse"]
                mongo_metrics = request_metrics["mongo"]
                redis_metrics = request_metrics["redis"]

                # Populate metrics with detailed database information - use structured format
                db_metrics = {
                    "mongodb": {
                        "queries": mongo_metrics["queries"],
                        "time_ms": int(mongo_metrics["time_ms"]),
                        "slow_queries": mongo_metrics.get("slow_queries", 0),
                        "operation_types": mongo_metrics.get("operation_types", {}),
                        "collection_stats": mongo_metrics.get("collection_stats", {})
                    },
                    "clickhouse": {
                        "queries": clickhouse_metrics["queries"], 
                        "time_ms": int(clickhouse_metrics["time_ms"]),
                        "slow_queries": clickhouse_metrics.get("slow_queries", 0),
                        "query_types": clickhouse_metrics.get("query_types", {})
                    },
                    "redis": {
                        "operations": redis_metrics["operations"],
                        "time_ms": int(redis_metrics["time_ms"]),
                        "slow_operations": redis_metrics.get("slow_operations", 0),
                        "command_types": redis_metrics.get("command_types", {})
                    }
                }
            except ImportError:
                logger.debug("Database metrics module not available")
            except Exception as e:
                logger.error(f"Error collecting database metrics: {e}", exc_info=True)

        if not self.track_api_events:
            return

        try:
            # Extract basic request info
            path = request.url.path
            method = request.method
            client_ip = self._get_client_ip(request)
            user_agent = request.headers.get("user-agent", "")
            status_code = response.status_code
            correlation_id = getattr(
                request.state, "correlation_id", str(TaskContext.get_correlation_id())
            )

            # Extract user ID and auth status
            user_id, is_authenticated = await self._extract_user_id(request)
            if not user_id:
                # Use anonymous identifier with IP for non-authenticated users
                user_id = f"anon-{client_ip}"

            # Convert to milliseconds for metrics
            process_time_ms = int(process_time * 1000)

            # Track API request event
            asyncio.create_task(
                self._track_api_request(
                    request=request,
                    response=response,
                    user_id=user_id,
                    client_ip=client_ip,
                    user_agent=user_agent,
                    path=path,
                    method=method,
                    status_code=status_code,
                    process_time_ms=process_time_ms,
                    correlation_id=correlation_id,
                    is_authenticated=is_authenticated,
                    db_metrics=db_metrics
                )
            )

        except Exception as e:
            logger.error(f"Error in event tracking: {e}", exc_info=True)

    async def _track_api_request(
        self,
        request: Request,
        response: Response,
        user_id: str,
        client_ip: str,
        user_agent: str,
        path: str,
        method: str,
        status_code: int,
        process_time_ms: int,
        correlation_id: str,
        is_authenticated: bool = False,
        db_metrics: dict = None
    ):
        """Track API request as an APIRequestEvent."""
        try:
            # Convert ObjectId to string if necessary
            if hasattr(user_id, "__str__") and not isinstance(user_id, str):
                user_id = str(user_id)

            # Ensure correlation_id is a valid UUID
            try:
                if correlation_id:
                    correlation_id = str(uuid.UUID(correlation_id))
                else:
                    correlation_id = str(uuid.uuid4())
            except ValueError:
                # If correlation_id is not a valid UUID, generate a new one
                correlation_id = str(uuid.uuid4())

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
                # Add error info
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
                                response_body = json.loads(body.decode("utf-8"))
                        except Exception:
                            pass
                    if isinstance(response_body, dict) and "detail" in response_body:
                        error_info["message"] = response_body["detail"]
                except Exception:
                    pass

            # Add headers and query params (sanitized)
            headers = {
                k: v
                for k, v in request.headers.items()
                if k.lower() not in ("authorization", "cookie")
            }

            # Safely get response size
            response_size = None
            if hasattr(response, "body"):
                try:
                    body = getattr(response, "body", None)
                    if body is not None:
                        response_size = len(body)
                except Exception:
                    pass

            # Create API request payload
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
                remote_ip=client_ip,
                is_authenticated=is_authenticated,
            )

            # Metrics for performance tracking
            metrics = {
                "total_time_ms": process_time_ms,
                "response_size_bytes": response_size or 0,
            }

            # Add database metrics if available
            if db_metrics:
                metrics.update(db_metrics)

            # Publish the API request event
            await APIRequestEvent.publish(
                entity_id=entity_id,
                actor_type=ActorType.USER if is_authenticated else ActorType.ANONYMOUS,
                actor_id=user_id,
                payload=payload,
                correlation_id=correlation_id,  # Now guaranteed to be a valid UUID string
                metrics=metrics,
            )

            # For server errors, also publish a dedicated error event
            if status_code >= 500:
                await SystemErrorEvent.publish(
                    entity_id="api_error",
                    actor_type=ActorType.SYSTEM,
                    actor_id="middleware",
                    payload=SystemErrorPayload(
                        error_type="http_error",
                        error_message=(
                            error_info.get("message", "Server error")
                            if error_info
                            else "Server error"
                        ),
                        severity="error",
                    ),
                    correlation_id=correlation_id,
                )
        except Exception as e:
            logger.error(f"Error publishing API event: {e}", exc_info=True)

    async def shutdown(self):
        """Clean up resources when the application shuts down."""
        if self.track_db_metrics:
            try:
                from stufio.db.metrics import stop_metrics_collection
                await stop_metrics_collection()
                logger.info("Stopped database metrics collection in EventTrackingMiddleware")
            except ImportError:
                pass
            except Exception as e:
                logger.error(f"Error stopping metrics collection: {e}", exc_info=True)
