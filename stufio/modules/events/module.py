from typing import List, Any, Tuple
import logging
import asyncio
from datetime import datetime

from stufio.core.module_registry import ModuleInterface
from stufio.core.stufioapi import StufioAPI
from .api import router
from .models import EventDefinitionModel, EventSubscriptionModel, EventLogModel
from .middleware import EventMiddleware
from .consumer_registry import consumer_registry
from .__version__ import __version__

logger = logging.getLogger(__name__)


class KafkaModuleMixin:
    """Mixin for modules that need Kafka support."""

    # Kafka components
    _kafka_router = None
    _kafka_broker = None
    _require_start_stop = True

    def register_kafka_consumers(self, app: StufioAPI, module_name: str) -> None:
        """
        Register Kafka consumers for this module.

        Args:
            app: StufioAPI instance
            module_name: The module name (e.g., 'events', 'activity')
        """
        if getattr(app.app_settings, "events_KAFKA_ENABLED", False) and getattr(
            app.app_settings, "events_APP_CONSUME_ROUTES", False
        ):
            try:
                logger.info(f"Registering Kafka consumers for {module_name} module")

                # Use the registry to discover and register consumers
                consumer_registry.register_consumers(app, module_name)

                # Store the Kafka broker reference for lifecycle management, if available
                try:
                    # Try to get the broker from the module's kafka.py
                    module_path = f"stufio.modules.{module_name}.consumers.kafka"
                    module = __import__(module_path, fromlist=["kafka_broker"])
                    self._kafka_broker = getattr(module, "kafka_broker", None)
                    self._kafka_router = getattr(module, "kafka_router", None)
                except ImportError:
                    # If not found in the module, try to get from events module
                    if module_name != "events":
                        try:
                            from stufio.modules.events.consumers.kafka import (
                                kafka_broker,
                                kafka_router,
                            )

                            self._kafka_broker = kafka_broker
                            self._kafka_router = kafka_router
                        except ImportError:
                            logger.warning(
                                f"Could not find Kafka broker for {module_name}"
                            )

                logger.info(f"Kafka consumer registration complete for {module_name}")
            except Exception as e:
                logger.error(
                    f"Failed to register Kafka consumers for {module_name}: {e}",
                    exc_info=True,
                )

    async def start_kafka(self, app: StufioAPI) -> None:
        """Start the Kafka broker connection."""
        if (
            self._kafka_broker
            and getattr(app.app_settings, "events_KAFKA_ENABLED", False)
            and getattr(app.app_settings, "events_APP_CONSUME_ROUTES", False)
            and self._require_start_stop
        ):
            try:
                logger.info(
                    f"Starting Kafka broker connection to {app.app_settings.events_KAFKA_BOOTSTRAP_SERVERS}"
                )
                await self._kafka_broker.start()
                logger.info("Kafka broker started successfully")

                # Send a test message in debug mode
                if getattr(app, "debug", False):
                    await self._send_test_message()
            except Exception as e:
                logger.error(f"Failed to start Kafka: {str(e)}", exc_info=True)

    async def stop_kafka(self, app: StufioAPI) -> None:
        """Stop the Kafka broker connection with proper cleanup."""
        if self._kafka_broker:
            try:
                logger.info("Stopping Kafka broker connection")

                # 4. Use the main broker shutdown method
                if hasattr(self._kafka_broker, "shutdown"):
                    # FastStream 0.2.x+ uses shutdown() method
                    logger.info("Calling broker shutdown()")
                    await self._kafka_broker.shutdown()
                elif hasattr(self._kafka_broker, "close"):
                    # Older versions use close()
                    logger.info("Calling broker close()")
                    await self._kafka_broker.close()
                else:
                    logger.warning("No shutdown or close method found for Kafka broker")
                        
                # Reset broker references to avoid duplicate shutdown attempts
                broker = self._kafka_broker
                self._kafka_broker = None
                self._kafka_router = None
                
                # Set a flag on the broker to indicate it's been closed (prevents further access)
                if hasattr(broker, "_closed"):
                    broker._closed = True

                logger.info("Kafka broker stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Kafka: {str(e)}", exc_info=True)

    async def _send_test_message(self) -> None:
        """Send a test message to Kafka."""
        try:
            from datetime import datetime

            test_message = {
                "event": "startup",
                "timestamp": datetime.utcnow().isoformat(),
                "module": getattr(self, "module_name", "unknown"),
            }
            await self._kafka_broker.publish(test_message, "test")
            logger.info("Sent test message to Kafka")
        except Exception as e:
            logger.warning(f"Failed to send test message: {str(e)}")


class EventsModule(ModuleInterface, KafkaModuleMixin):
    """Events module for event-driven architecture support."""

    version = __version__
    _last_message_times = {}

    def register_routes(self, app: StufioAPI) -> None:
        """Register this module's routes with the FastAPI app."""
        app.include_router(router, prefix=self.routes_prefix)

    def get_models(self) -> List[Any]:
        """Return this module's database models."""
        return [EventDefinitionModel, EventSubscriptionModel, EventLogModel]

    def get_middlewares(self) -> List[Tuple]:
        """Return middleware classes for this module.

        Returns:
            List of (middleware_class, args, kwargs) tuples
        """
        return [(EventMiddleware, [], {})]

    def track_handler(self, handler_name: str = None) -> None:
        """Track the last time a handler was called.
        
        Args:
            handler_name: Optional name of the handler. If None, we'll try to detect
                        the caller function name automatically.
        """
        if handler_name is None:
            # Get the caller's function name using inspect
            frame = inspect.currentframe()
            try:
                caller_frame = frame.f_back
                if caller_frame:
                    # Get function name from caller's frame
                    handler_name = caller_frame.f_code.co_name

                    # For more detailed tracking, you can include module name
                    module_name = caller_frame.f_globals.get('__name__', '')
                    if module_name:
                        handler_name = f"{module_name}.{handler_name}"
                else:
                    handler_name = "unknown_handler"
            finally:
                # Always delete frame references to prevent reference cycles
                del frame

        if handler_name:
            self._last_message_times[handler_name] = datetime.now()
            logger.info(f"Handler {handler_name} called at {self._last_message_times[handler_name]}")
        else:
            logger.warning("Handler name is None, unable to track call time")

    async def monitor_consumer_health(self) -> None:
        """Monitor consumer health periodically"""
        while True:
            now = datetime.now()
            for handler_name, last_time in self._last_message_times.items():
                if (now - last_time).total_seconds() > 60:  # No messages for 1 minute
                    logger.warning(
                        f"Consumer {handler_name} hasn't received messages for {(now - last_time).total_seconds()} seconds"
                    )

            await asyncio.sleep(30)  # Check every 30 seconds

    # Start the monitor in on_startup
    async def on_startup(self, app: "StufioAPI") -> None:
        """Called when the application starts up."""
        from .patches import apply_faststream_patches

        # Apply FastStream patches before starting Kafka
        apply_faststream_patches()

        # Start health monitoring
        # asyncio.create_task(self.monitor_consumer_health())

        # Call parent method for Kafka consumer registration
        await super().on_startup(app)

    async def on_shutdown(self, app: "StufioAPI") -> None:
        """Called when the application shuts down."""
        try:
            # First try normal shutdown with timeout
            import asyncio

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
