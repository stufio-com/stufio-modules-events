import logging
from typing import Optional
from fastapi import FastAPI, APIRouter, Response
from faststream.asyncapi.schema import Info, Schema

from stufio.core.config import get_settings

logger = logging.getLogger(__name__)

def initialize_asyncapi_docs(
    app: FastAPI, 
    schema: Optional[Schema] = None,
    prefix: str = "/asyncapi"
) -> Optional[APIRouter]:
    """Initialize AsyncAPI documentation if configured.
    
    Args:
        app: FastAPI application
        router: Optional router to attach docs to
        schema: Optional AsyncAPI schema object (if not provided, generated from router)
        prefix: URL prefix for AsyncAPI docs
        
    Returns:
        The AsyncAPI router or None if disabled
    """
    settings = get_settings()

    try:
        # Create a dedicated router for AsyncAPI docs with the specified prefix
        docs_router = APIRouter(prefix=prefix)

        if schema is None:
            logger.error("No schema could be generated or provided")
            return None

        # Customize info object
        schema.info = Info(
            title="Stufio Events API",
            version="1.0.0",
            description="Event-driven microservices API documentation"
        )

        # Add AsyncAPI HTML endpoint - now using the correct syntax for router decorators
        @docs_router.get("")
        async def get_asyncapi_ui():
            logger.warning("AsyncAPI HTML endpoint is generated.")
            # Check if AsyncAPI docs are enabled
            if not settings.events_ASYNCAPI_DOCS_ENABLED:
                logger.info("AsyncAPI documentation is disabled via configuration")
                return Response(status_code=404)

            # Import our custom functions instead of the originals
            from stufio.modules.events.consumers.asyncapi_site import get_asyncapi_html
            html_content = get_asyncapi_html(schema)
            return Response(content=html_content, media_type="text/html")

        # Add AsyncAPI JSON endpoint
        @docs_router.get(".json")
        async def get_asyncapi_json():
            logger.warning("AsyncAPI JSON endpoint is generated.")
            return schema.to_jsonable()

        @docs_router.get(".yaml")
        async def get_asyncapi_yaml():
            logger.warning("AsyncAPI YML endpoint is generated.")
            return schema.to_yaml()

        # Include the docs router in the app
        app.include_router(docs_router)

        logger.info(f"Successfully initialized AsyncAPI documentation at {prefix}")
        return docs_router

    except Exception as e:
        logger.error(f"Failed to initialize AsyncAPI documentation: {e}", exc_info=True)
        return None
