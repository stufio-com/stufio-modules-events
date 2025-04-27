"""
Base middleware components for Stufio framework.

This module provides base classes for middleware components that can be extended
by other modules. It standardizes common functionality like request tracking,
IP extraction, and performance measurement.
"""
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import time
import uuid
import logging
from typing import Optional, Tuple, List, Any
from stufio.core.config import get_settings
from ..utils.context import TaskContext

settings = get_settings()
logger = logging.getLogger(__name__)

# Common paths that are typically excluded from tracking/rate limiting
DEFAULT_EXCLUDED_PATHS = [
    "/metrics",
    "/health",
    "/asyncapi",
    "/asyncapi.json",
    "/docs",
    "/redoc",
    f"{settings.API_V1_STR}/openapi.json",
]


class BaseStufioMiddleware(BaseHTTPMiddleware):
    """Base middleware class for Stufio framework."""

    def __init__(
        self, 
        app: ASGIApp, 
        excluded_paths: Optional[List[str]] = None
    ):
        super().__init__(app)
        # Paths to exclude from middleware processing
        self.excluded_paths = excluded_paths or DEFAULT_EXCLUDED_PATHS

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process a request through the middleware chain."""
        # First, clear any existing context to ensure isolation
        TaskContext.clear()

        # Check if request has a correlation-id header
        corr_id_header = request.headers.get("x-correlation-id")

        # Generate or use existing correlation ID
        if corr_id_header:
            try:
                TaskContext.set_correlation_id(corr_id_header)
            except ValueError:
                TaskContext.set_correlation_id(uuid.uuid4())
        else:
            TaskContext.set_correlation_id(uuid.uuid4())

        correlation_id = str(TaskContext.get_correlation_id())

        # Store in request state for middleware access
        request.state.correlation_id = correlation_id

        # Skip processing for excluded paths
        if self._should_skip_path(request):
            try:
                response = await call_next(request)
                # Even for skipped paths, set the correlation ID header
                if response:
                    response.headers["X-Correlation-ID"] = correlation_id
                return response
            except Exception as e:
                logger.exception(f"❌ Error in middleware for excluded path: {e}")
                # Create fallback response
                return Response(
                    content=f"Internal server error: {str(e)}",
                    status_code=500,
                    headers={"X-Correlation-ID": correlation_id},
                )

        # Start timing
        start_time = time.time()

        # Pre-processing hook for subclasses
        await self._pre_process(request)

        # Process the request and catch exceptions
        response = None
        try:
            response = await call_next(request)
        except RuntimeError as e:
            if "No response returned" in str(e):
                logger.error(f"Middleware error - No response returned: {e}")
                response = Response(
                    content="No response was returned from the application",
                    status_code=500,
                    headers={"X-Correlation-ID": correlation_id}
                )
            else:
                logger.exception(f"❌ Runtime error in middleware: {e}")
                response = await self._handle_exception(request, e)
        except Exception as e:
            logger.exception(f"❌ Error in middleware chain: {e}")
            # Handle exception and generate response
            response = await self._handle_exception(request, e)
        finally:
            # Calculate request processing time
            process_time = time.time() - start_time

            # Ensure we have a response object
            if response is None:
                logger.error("No response was returned from middleware chain")
                response = Response(
                    content="No response was returned from the application",
                    status_code=500,
                    headers={"X-Correlation-ID": correlation_id}
                )

            # Post-processing hook for subclasses - only if we have a valid response
            if response is not None:
                try:
                    await self._post_process(
                        request=request, 
                        response=response,
                        process_time=process_time
                    )
                except Exception as e:
                    logger.error(f"❌ Error in post-processing: {e}", exc_info=True)

            # Ensure correlation ID is set in the response
            if response and hasattr(response, "headers"):
                response.headers["X-Correlation-ID"] = correlation_id

            return response

    def _should_skip_path(self, request: Request) -> bool:
        """Determine if processing should be skipped for this path."""
        return request.url.path in self.excluded_paths

    def _get_client_ip(self, request: Request) -> str:
        """Extract the real client IP from request headers."""
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            # Get the first IP if multiple are provided
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _normalize_path(self, path: str) -> str:
        """
        Normalize a path by replacing dynamic segments with placeholders.
        
        Examples:
            /api/v1/domains/expired/by-date/2025-03-13/with-analysis
            becomes
            /api/v1/domains/expired/by-date/{date}/with-analysis
            
            /api/v1/users/123/profile
            becomes
            /api/v1/users/{int}/profile
        """
        import re
        normalized_path = path
        normalized_path = re.sub(r"/\d{4}-\d{2}-\d{2}", "/{date}", normalized_path)
        normalized_path = re.sub(r"/\d+", "/{int}", normalized_path)
        return normalized_path

    async def _extract_user_id(self, request: Request) -> Tuple[Optional[str], bool]:
        """
        Extract user ID from request if authenticated.
        
        Returns:
            Tuple containing (user_id, is_authenticated)
        """
        from stufio.api import deps

        user_id = None
        is_authenticated = False

        auth_header = request.headers.get("authorization")
        if (
            auth_header
            and auth_header.startswith("Bearer ")
            and request.url.path not in [settings.API_V1_STR + "/login/claim"]
        ):
            token = auth_header.replace("Bearer ", "")
            try:
                token_data = deps.get_token_payload(token)
                user_id = token_data.sub
                is_authenticated = True
            except Exception as e:
                logger.debug(f"Error extracting user from token: {e}")

        # Convert user_id to string if it's an ObjectId
        if user_id is not None and not isinstance(user_id, str):
            user_id = str(user_id)

        return user_id, is_authenticated

    def _ensure_string_id(self, obj_id: Any) -> str:
        """Convert various ID types to strings safely."""
        if obj_id is None:
            return ""
        if hasattr(obj_id, "__str__"):
            return str(obj_id)
        return ""

    async def _pre_process(self, request: Request) -> None:
        """
        Pre-processing hook to be implemented by subclasses.
        Called before the request is processed.
        """
        pass

    async def _post_process(
        self, 
        request: Request, 
        response: Response,
        process_time: float
    ) -> None:
        """
        Post-processing hook to be implemented by subclasses.
        Called after the request is processed.
        """
        pass

    async def _handle_exception(
        self, 
        request: Request, 
        exception: Exception
    ) -> Response:
        """
        Handle exceptions raised during request processing.
        To be implemented or extended by subclasses.
        
        Returns:
            A fallback response
        """
        from fastapi.responses import JSONResponse
        # Default implementation returns a 500 response
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )
