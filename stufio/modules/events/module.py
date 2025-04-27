from typing import List, Any, Tuple
import logging
import asyncio
import inspect
import importlib

from stufio.core.module_registry import ModuleInterface
from stufio.core.stufioapi import StufioAPI
from .schemas.event_definition import EventDefinition
from .api import router
from .middleware import EventTrackingMiddleware
from .services.consumer_registry import consumer_registry
from .__version__ import __version__
from .helpers import register_module_events

logger = logging.getLogger(__name__)


class KafkaModuleMixin:
    """Mixin for modules that need Kafka support."""

    # Kafka components
    _kafka_brokers = []
    _require_start_stop = False

    def register(self: "ModuleInterface", app: StufioAPI) -> None:
        """Register this module with the FastAPI app, including Kafka consumers."""
        # First call the parent class's register method
        super().register(app)

        # Then register Kafka consumers
        try:
            self.register_kafka_consumers(app, self.name)
        except Exception as e:
            logger.error(f"Error registering Kafka consumers for module {self.name}: {str(e)}", exc_info=True)

    def register_kafka_consumers(
        self: "ModuleInterface", app: StufioAPI, module_name: str
    ) -> None:
        """
        Register Kafka consumers for this module.

        Args:
            app: StufioAPI instance
            module_name: The module name (e.g., 'events', 'activity')
        """
        if getattr(app.app_settings, "events_KAFKA_ENABLED", False):
            try:
                logger.info(f"Registering Kafka consumers for {module_name} module")

                # Store the Kafka broker reference for lifecycle management, if available
                try:
                    # Use module_path from ModuleInfo via the property
                    base_module_path = self.module_path
                    if base_module_path:
                        module_path = f"{base_module_path}.consumers"
                    else:
                        logger.error(f"Module path not available for {module_name}")
                        return

                    __import__(module_path, fromlist=["kafka_broker"])

                    brokers = consumer_registry.get_module_brokers(module_name)
                    if brokers:
                        for broker in brokers.values():
                            if broker:
                                self._kafka_brokers.append(broker)
                                logger.info(f"Registered Kafka broker {broker} for {module_name}")
                                self._require_start_stop = True
                    else:
                        logger.debug(f"No customer kafka brokers found for {module_name}")

                except ImportError as e:
                    logger.error(
                        f"❌ Failed to import consumers module for {module_name}: {e}: {module_path}"
                    )
                    logger.debug(f"Attempted import path: {module_path}")

                logger.info(f"Kafka consumer registration complete for {module_name}")
            except Exception as e:
                logger.error(
                    f"❌ Failed to register Kafka consumers for {module_name}: {e}",
                    exc_info=True,
                )

    async def on_startup(self: "ModuleInterface", app: "StufioAPI") -> None:
        """Called when the application starts up."""
        await super().on_startup(app)
        await self.start_kafka(app)

    async def on_shutdown(self: "ModuleInterface", app: "StufioAPI") -> None:
        """Called when the application shuts down."""
        await super().on_shutdown(app)
        await self.stop_kafka(app)

    async def start_kafka(self: "ModuleInterface", app: StufioAPI) -> None:
        """Start the Kafka broker connection."""
        if len(self._kafka_brokers) and self._require_start_stop:
            try:
                for broker in self._kafka_brokers:
                    # Check if the broker is already started
                    if not getattr(broker, "_started", False):
                        logger.info(f"Starting Kafka broker {broker}")
                        await broker.start()
                        broker._started = True
                    else:
                        logger.info(f"Kafka broker {broker} is already started")
            except Exception as e:
                logger.error(f"❌ Failed to start Kafka: {str(e)}", exc_info=True)

    async def stop_kafka(self: "ModuleInterface", app: StufioAPI) -> None:
        """Stop the Kafka broker connection with proper cleanup."""
        if len(self._kafka_brokers) and self._require_start_stop:
            for broker in self._kafka_brokers:
                try:
                    # Check if the broker is already closed
                    if getattr(broker, "_closed", False):
                        logger.info(f"Kafka broker {broker} is already closed")
                        continue

                    logger.info(f"Stopping Kafka broker {broker}")

                    #  Use the main broker shutdown method
                    if hasattr(broker, "shutdown"):
                        # FastStream 0.2.x+ uses shutdown() method
                        logger.info("Calling broker shutdown()")
                        await broker.shutdown()
                    elif hasattr(broker, "close"):
                        # Older versions use close()
                        logger.info("Calling broker close()")
                        await broker.close()
                    else:
                        logger.warning("No shutdown or close method found for Kafka broker")

                    # Set the closed flag to prevent further access
                    broker._closed = True

                except Exception as e:
                    logger.error(f"Error stopping Kafka: {str(e)}", exc_info=True)


class EventsModuleMixin:
    """Mixin for modules that use the events system."""

    _events = []

    def register(self: "ModuleInterface", app: StufioAPI) -> None:
        """Register this module with events system."""
        # First call the parent class's register method
        super().register(app)

        self.register_module_events(app)

    def register_module_events(self: "ModuleInterface", app: StufioAPI) -> None:
        """Register events for this module."""
        # Then register events
        try:
            # Look for ModuleInterface implementation using module_path from ModuleInfo
            if hasattr(self, "_module_info") and self._module_info:
                module = self._module_info.get_module()
            else:
                if not self.module_path:
                    logger.error(f"Module path not available for {self.name}")
                    return
                module = importlib.import_module(self.module_path)
                
            # Clear existing events before registering new ones
            self._events = []
                
            # Look for EventDefinition classes
            for name, cls in inspect.getmembers(module):
                if inspect.isclass(cls) and issubclass(cls, EventDefinition) and cls != EventDefinition:
                    self._events.append(cls)

            if len(self._events) == 0:
                logger.warning(f"No events found in {self.name} module")
            else:
                # Register all events in one place
                register_module_events(self.name, self._events)
                logger.info(f"Registered {self.name} module events: {self._events}")

        except Exception as e:
            logger.error(
                f"Error registering events for module {self.name}: {str(e)}",
                exc_info=True,
            )


class EventsModule(KafkaModuleMixin, EventsModuleMixin, ModuleInterface):
    """Events module for event-driven architecture support."""

    version = __version__
    _last_message_times = {}

    def register_routes(self, app: StufioAPI) -> None:
        """Register this module's routes with the FastAPI app."""
        app.include_router(router, prefix=self.routes_prefix)

    def get_middlewares(self) -> List[Tuple]:
        """Return middleware classes for this module.

        Returns:
            List of (middleware_class, args, kwargs) tuples
        """

        return [(EventTrackingMiddleware, [], {})]

    # Start the monitor in on_startup
    async def on_startup(self, app: "StufioAPI") -> None:
        """Called when the application starts up."""

        from .patches import apply_all_patches

        # Apply FastStream patches before starting Kafka
        apply_all_patches()

        # Call parent method for Kafka consumer registration
        await super().on_startup(app)

        consumer_registry.register_with_app(app)

    async def on_shutdown(self, app: "StufioAPI") -> None:
        """Called when the application shuts down."""
        try:
            # Set a timeout for the normal shutdown
            try:
                await asyncio.wait_for(self.stop_kafka(app), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Kafka shutdown timed out, attempting forced shutdown")

            # Then apply the force shutdown as a backup
            try:
                from .consumers.kafka import force_shutdown_kafka
                await force_shutdown_kafka()
            except Exception as e:
                logger.error(f"Error during forced Kafka shutdown: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Error during Kafka shutdown: {e}", exc_info=True)
