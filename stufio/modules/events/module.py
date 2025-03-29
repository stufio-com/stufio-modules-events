from fastapi import FastAPI
from typing import List, Any, Tuple, Optional
import logging

from stufio.core.module_registry import ModuleInterface
from .api import api_router
from .models import EventDefinitionModel, EventSubscriptionModel, EventLogModel
from .kafka_client import KafkaClient
from .event_bus import event_bus, EventBus
from .middleware import EventMiddleware
from .__version__ import __version__

logger = logging.getLogger(__name__)

class EventsModule(ModuleInterface):
    """Events module for event-driven architecture support."""

    __version__ = __version__

    def __init__(self, kafka_bootstrap_servers: Optional[List[str]] = None):
        """Initialize the events module.
        
        Args:
            kafka_bootstrap_servers: List of Kafka bootstrap servers
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_client = None

    def register_routes(self, app: FastAPI) -> None:
        """Register this module's routes with the FastAPI app."""
        app.include_router(api_router, prefix=self._routes_prefix)

    async def initialize(self, app: FastAPI) -> None:
        """Initialize Kafka client and event bus.
        
        This should be called during app startup.
        """
        # Initialize Kafka client
        self.kafka_client = KafkaClient(bootstrap_servers=self.kafka_bootstrap_servers)
        await self.kafka_client.initialize()

        # Initialize the shared event_bus with our Kafka client
        EventBus(self.kafka_client)  # This will update the singleton instance
        await event_bus.initialize()

        # Store in app state for access from endpoints
        app.state.event_bus = event_bus
        event_registry = event_bus.registry

        # Register all event definitions from the events.py modules
        await event_registry.register_all_modules()
        logger.info("Event registry initialized")

    def get_models(self) -> List[Any]:
        """Return this module's database models."""
        return [EventDefinitionModel, EventSubscriptionModel, EventLogModel]

    # For backwards compatibility
    def register(self, app: FastAPI) -> None:
        """Legacy registration method."""
        self.register_routes(app)

    def get_middlewares(self) -> List[Tuple]:
        """Return middleware classes for this module.

        Returns:
            List of (middleware_class, args, kwargs) tuples
        """
        return [(EventMiddleware, [], {})]

    async def cleanup(self) -> None:
        """Cleanup resources when app is shutting down."""
        if self.kafka_client:
            await self.kafka_client.close()

        if hasattr(event_bus, 'http_client'):
            await event_bus.http_client.aclose()
