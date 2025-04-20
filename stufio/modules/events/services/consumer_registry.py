import logging
from typing import Dict, Any
from fastapi import FastAPI
from ..consumers.asyncapi import get_patched_app_schema
from stufio.core.config import get_settings

logger = logging.getLogger(__name__)

class ConsumerRegistry:
    """Registry for Kafka consumers across modules."""

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConsumerRegistry, cls).__new__(cls)
            cls._instance._registered_routers = {}
            cls._instance._registered_brokers = {}
            cls._instance._registered_modules = set()
        return cls._instance

    def register_router(self, module_name: str, consumer_name: str, consumer_router: Any) -> None:
        """Register a consumer router from a module."""
        key = f"{module_name}.{consumer_name}"
        self._registered_routers[key] = consumer_router

        # Track that we've registered for this module
        if not module_name in self._registered_modules:
            self._registered_modules.add(module_name)
        logger.info(f"Registered consumer {key}")

    def register_broker(self, module_name: str, broker_name: str, broker: Any) -> None:
        """Register a Kafka broker from a module."""
        key = f"{module_name}.{broker_name}"
        self._registered_brokers[key] = broker

        # Track that we've registered for this module
        if not module_name in self._registered_modules:
            self._registered_modules.add(module_name)
        logger.info(f"Registered broker for {module_name}")

    def get_module_routers(self, module_name: str) -> Dict[str, Any]:
        """Get all consumers registered for a specific module."""
        result = {}
        prefix = f"{module_name}."
        for key, router in self._registered_routers.items():
            if key.startswith(prefix):
                consumer_name = key[len(prefix):]
                result[consumer_name] = router
        return result

    def get_module_brokers(self, module_name: str) -> Dict[str, Any]:
        """Get all brokers registered for a specific module."""
        result = {}
        prefix = f"{module_name}."
        for key, broker in self._registered_brokers.items():
            if key.startswith(prefix):
                consumer_name = key[len(prefix) :]
                result[consumer_name] = broker
        return result

    def register_with_app(self, app: FastAPI) -> None:
        """Register all consumers with the FastAPI app."""
        settings = get_settings()
        
        if not settings.events_ASYNCAPI_DOCS_ENABLED:
            logger.warning("AsyncAPI documentation is disabled in settings.")
            return
        
        if not self._initialized:
            logger.info("Initializing consumer registry")
            self._initialized = True
            
        else:
            logger.info("Consumer registry already initialized, skipping.")
            return
        
        from stufio.modules.events.consumers.asyncapi_init import initialize_asyncapi_docs

        for key, router in self._registered_routers.items():
            try:
                if router and hasattr(router, "include_in_schema") and router.include_in_schema:
                    app.include_router(router)
                    logger.info(f"Registered Kafka router {key} with FastAPI app")

                    # AsyncAPI documentation if available
                    if hasattr(router, 'docs_router') and router.docs_router:
                        # Initialize AsyncAPI docs
                        logger.info(f"Adding AsyncAPI documentation for {key}")
                        try:
                            schema = get_patched_app_schema(router)
                            initialize_asyncapi_docs(app, schema)
                        except Exception as e:
                            logger.error(f"Failed to initialize AsyncAPI docs: {e}", exc_info=True)
                            continue

            except Exception as e:
                logger.error(f"Failed to register Kafka router {key}: {e}", exc_info=True)


# Global singleton instance
consumer_registry = ConsumerRegistry()
