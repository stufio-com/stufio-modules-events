import logging
import importlib
import pkgutil
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Type
from fastapi import FastAPI

logger = logging.getLogger(__name__)

class EventConsumerRegistry:
    """Registry for Kafka event consumers across modules."""

    def __init__(self):
        self._registered_modules: Set[str] = set()
        self._router_cache: Dict[str, Any] = {}

    def discover_consumers(self, module_path: str) -> Dict[str, Any]:
        """
        Dynamically discover all consumer modules in a given module path.
        
        Args:
            module_path: Dotted path to the module containing consumers (e.g., 'stufio.modules.activity.consumers')
            
        Returns:
            Dictionary of discovered consumer names and their Kafka routers
        """
        if module_path in self._registered_modules:
            return self._router_cache.get(module_path, {})

        try:
            # Import the base module
            module = importlib.import_module(module_path)
            base_path = Path(module.__file__).parent

            # Track discovered routers
            discovered_routers = {}

            # Iterate through all submodules
            for _, name, is_pkg in pkgutil.iter_modules([str(base_path)]):
                if is_pkg:
                    continue  # Skip packages, only load modules

                try:
                    # Import the consumer module
                    consumer_module = importlib.import_module(f"{module_path}.{name}")

                    # Look for kafka_router attribute or getter
                    if hasattr(consumer_module, "kafka_router") and consumer_module.kafka_router:
                        router_name = f"{name}_router" if name != "kafka" else "kafka_router"
                        discovered_routers[router_name] = consumer_module.kafka_router
                        logger.info(f"Discovered Kafka router in {module_path}.{name}")
                    elif hasattr(consumer_module, "get_kafka_router") and callable(consumer_module.get_kafka_router):
                        # Call the getter function
                        router_name = f"{name}_router" if name != "kafka" else "kafka_router"
                        discovered_routers[router_name] = consumer_module.get_kafka_router()
                        logger.info(f"Discovered Kafka router via getter in {module_path}.{name}")
                except ImportError as e:
                    logger.warning(f"Could not import consumer module {name}: {str(e)}")
                except Exception as e:
                    logger.error(f"Error discovering consumers in {name}: {str(e)}", exc_info=True)

            # Cache the results
            self._router_cache[module_path] = discovered_routers
            self._registered_modules.add(module_path)

            return discovered_routers

        except ImportError as e:
            logger.warning(f"Could not import consumer module path {module_path}: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Error discovering consumers: {str(e)}", exc_info=True)
            return {}

    def register_consumers(self, app: FastAPI, module_name: str) -> None:
        """
        Register all discovered consumers for a module with the FastAPI app.
        
        Args:
            app: FastAPI application
            module_name: Module name (e.g., 'activity', 'events')
        """
        # Construct the consumer package path
        module_path = f"stufio.modules.{module_name}.consumers"

        # import debugpy
        # # Allow connections to the debugger from any host
        # debugpy.listen(("0.0.0.0", 5678))
        # logger.error("Waiting for debugger to attach...")
        # debugpy.wait_for_client()

        # Discover consumers
        routers = self.discover_consumers(module_path)

        # Register each router with the app
        for name, router in routers.items():
            try:
                if router and hasattr(router, "include_in_schema") and router.include_in_schema:
                    app.include_router(router)
                    logger.info(f"Registered Kafka router {name} for module {module_name}")

                    # AsyncAPI documentation if available
                    if hasattr(router, 'docs_router') and router.docs_router:
                        logger.info(f"Adding AsyncAPI documentation for {name}")
                        try:
                            # Use our patched schema generator
                            from stufio.modules.events.consumers.asyncapi import get_patched_app_schema
                            router.schema = get_patched_app_schema(router)
                        except ImportError as e:
                            logger.warning(f"AsyncAPI generation failed for {name}: {e}")
                        except Exception as e:
                            logger.error(f"Error generating AsyncAPI schema: {e}", exc_info=True)

                        # Add the docs router to the app
                        app.include_router(router.docs_router)
                        logger.info(f"Registered AsyncAPI documentation for {name}")

            except Exception as e:
                logger.error(f"Failed to register Kafka router {name}: {str(e)}", exc_info=True)

# Singleton instance
consumer_registry = EventConsumerRegistry()
