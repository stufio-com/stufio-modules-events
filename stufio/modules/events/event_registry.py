from typing import Dict, List, Optional, Callable, Any, Set
from .schemas.base import EventSubscription
from .schemas.event import EventMessage
from .schemas.event_definition import EventDefinition, EventDefinitionValid
from .models import EventDefinitionModel
from .crud import crud_event_definitions, crud_event_subscriptions
from stufio.core.module_registry import registry as module_registry
from fastapi import FastAPI
import logging
import asyncio
import inspect
from stufio.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# Type for event handlers
EventHandler = Callable[[EventMessage], Any]


class EventRegistry:
    """Registry for event definitions and subscriptions.
    
    Implemented as a singleton to ensure only one instance exists.
    """
    _instance: Optional['EventRegistry'] = None
    _initialized: bool = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(EventRegistry, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the event registry (only once)."""
        # Skip initialization if already initialized
        if self._initialized:
            return

        # Database caches
        self._definitions_cache: Dict[str, EventDefinition] = {}
        self._db_subscriptions_cache: Dict[str, List[EventSubscription]] = {}

        # In-memory handlers
        self._memory_handlers: Dict[str, List[EventHandler]] = {}

        # Set of registered modules
        self._registered_modules: Set[str] = set()

        # Mark as initialized
        self._initialized = True
        logger.debug("EventRegistry singleton initialized")

    def _get_event_key(self, entity_type: str, action: str) -> str:
        """Generate a key for the event handlers dictionary."""
        return f"{entity_type}:{action}"

    def subscribe_memory(self, entity_type: str, action: str, handler: EventHandler) -> None:
        """Subscribe to an event with an in-memory handler function."""
        key = self._get_event_key(entity_type, action)

        if key not in self._memory_handlers:
            self._memory_handlers[key] = []

        # Avoid duplicate handlers
        for existing_handler in self._memory_handlers[key]:
            if existing_handler.__name__ == handler.__name__ and existing_handler.__module__ == handler.__module__:
                logger.warning(f"Handler {handler.__name__} already registered for {key}")
                return

        self._memory_handlers[key].append(handler)
        logger.debug(f"Registered memory handler {handler.__name__} for event {key}")

    def get_memory_handlers(self, entity_type: str, action: str) -> List[EventHandler]:
        """Get all in-memory handlers for a specific event."""
        key = self._get_event_key(entity_type, action)

        # Get exact matches
        handlers = self._memory_handlers.get(key, []).copy()

        # Get wildcard entity matches
        wildcard_entity_key = self._get_event_key("*", action)
        handlers.extend(self._memory_handlers.get(wildcard_entity_key, []))

        # Get wildcard action matches
        wildcard_action_key = self._get_event_key(entity_type, "*")
        handlers.extend(self._memory_handlers.get(wildcard_action_key, []))

        # Get full wildcard matches
        full_wildcard_key = self._get_event_key("*", "*")
        handlers.extend(self._memory_handlers.get(full_wildcard_key, []))

        return handlers

    async def register_event(self, definition_dict: Dict[str, Any]) -> EventDefinitionModel:
        """Register a new event definition in the database."""
        # First validate with Pydantic

        validated_data = EventDefinitionValid(**definition_dict).model_dump()

        # Now use the validated data with your model
        event_def_model = EventDefinitionModel(**validated_data)

        # Check if event is already registered
        existing = await crud_event_definitions.get_by_name(validated_data["name"])
        if existing:
            # Update existing definition
            # FIX: Pass the full existing object, not just the ID
            update_data = event_def_model.model_dump(exclude={"id", "created_at"})
            updated = await crud_event_definitions.update(existing, update_data)
            self._definitions_cache[validated_data["name"]] = updated
            return updated

        # Create new definition
        created = await crud_event_definitions.create(event_def_model)
        self._definitions_cache[validated_data["name"]] = created
        return created

    async def subscribe(self, subscription: EventSubscription) -> EventSubscription:
        """Register a new event subscription in the database."""
        created = await crud_event_subscriptions.create(subscription)

        # Update cache
        cache_key = self._get_event_key(subscription.entity_type or '*', subscription.action or '*')
        if cache_key not in self._db_subscriptions_cache:
            self._db_subscriptions_cache[cache_key] = []
        self._db_subscriptions_cache[cache_key].append(created)

        return created

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Remove an event subscription from the database."""

        # Get the subscription first to update cache
        subscription = await crud_event_subscriptions.get(subscription_id)
        if not subscription:
            return False

        # Delete from database
        deleted = await crud_event_subscriptions.remove(subscription_id)

        # Update cache
        cache_key = self._get_event_key(subscription.entity_type or '*', subscription.action or '*')
        if cache_key in self._db_subscriptions_cache:
            self._db_subscriptions_cache[cache_key] = [
                s for s in self._db_subscriptions_cache[cache_key] 
                if str(s.id) != subscription_id
            ]

        return deleted

    async def get_matching_subscriptions(
        self, 
        entity_type: str, 
        action: str
    ) -> List[EventSubscription]:
        """Get all database subscriptions that match the given entity type and action."""

        # Try cache first
        cache_key = self._get_event_key(entity_type, action)
        if cache_key in self._db_subscriptions_cache:
            return self._db_subscriptions_cache[cache_key]

        # Query database
        subscriptions = await crud_event_subscriptions.get_by_entity_action(entity_type, action)

        # Update cache
        self._db_subscriptions_cache[cache_key] = subscriptions

        return subscriptions

    async def get_all_events(self) -> List[EventDefinition]:
        """Get all registered event definitions."""
        # If cache is empty, populate it
        if not self._definitions_cache:
            all_definitions = await crud_event_definitions.get_multi()
            self._definitions_cache = {d.name: d for d in all_definitions}
            return all_definitions

        return list(self._definitions_cache.values())

    async def refresh_cache(self) -> None:
        """Refresh the internal caches from the database."""
        # Clear and repopulate definitions cache
        all_definitions = await crud_event_definitions.get_multi()
        self._definitions_cache = {d.name: d for d in all_definitions}

        # Clear and repopulate subscriptions cache
        self._db_subscriptions_cache = {}
        all_subscriptions = await crud_event_subscriptions.get_multi()

        for sub in all_subscriptions:
            entity_type = sub.entity_type or '*'
            action = sub.action or '*'
            cache_key = self._get_event_key(entity_type, action)

            if cache_key not in self._db_subscriptions_cache:
                self._db_subscriptions_cache[cache_key] = []

            self._db_subscriptions_cache[cache_key].append(sub)

    # def discover_events(self, module_name: str) -> List[Dict[str, Any]]:
    #     """
    #     Discover all event definitions in a module.
        
    #     Args:
    #         module_name: Module name (e.g., 'users' or 'stufio.modules.users')
            
    #     Returns:
    #         List of event definitions as dictionaries
    #     """
    #     # Normalize the module name
    #     if not module_name.startswith("stufio.modules."):
    #         import_path = f"stufio.modules.{module_name}"
    #     else:
    #         import_path = module_name

    #     # Remove any path-like elements
    #     if "/" in import_path or "\\" in import_path:
    #         parts = import_path.split(".")
    #         filtered_parts = [p for p in parts if "/" not in p and "\\" not in p]
    #         import_path = ".".join(filtered_parts)

    #     logger.info(f"Attempting to discover events in module {import_path}")

    #     try:
    #         # Import the module's events.py file
    #         events_module = __import__(f"{import_path}.events", fromlist=[""])

    #         # Find all EventDefinition subclasses
    #         events = []

    #         # First check for ALL_EVENTS list which is the preferred approach
    #         if hasattr(events_module, "ALL_EVENTS") and isinstance(events_module.ALL_EVENTS, list):
    #             for event_class in events_module.ALL_EVENTS:
    #                 if (inspect.isclass(event_class) and 
    #                     issubclass(event_class, EventDefinition) and 
    #                     event_class != EventDefinition):
    #                     try:
    #                         events.append(event_class.to_dict())
    #                     except Exception as e:
    #                         logger.error(f"Error processing event {event_class.__name__}: {e}")
    #         else:
    #             # Fall back to inspecting module members
    #             for name, obj in inspect.getmembers(events_module):
    #                 if (inspect.isclass(obj) and 
    #                     issubclass(obj, EventDefinition) and 
    #                     obj != EventDefinition and
    #                     obj.__module__ == f"{import_path}.events"):
    #                     try:
    #                         events.append(obj.to_dict())
    #                     except Exception as e:
    #                         logger.error(f"Error processing event {name}: {e}")

    #         logger.info(f"Discovered {len(events)} event(s) in {import_path}")
    #         return events
    #     except ImportError as e:
    #         logger.warning(f"Module {import_path} does not have an events.py file or it cannot be imported: {e}")
    #         return []
    #     except Exception as e:
    #         logger.error(f"Error discovering events in {import_path}: {e}", exc_info=True)
    #         return []

    # def discover_consumers(self, module_name: str) -> bool:
    #     """
    #     Discover all consumer definitions in a module.
        
    #     Args:
    #         module_name: Module name (e.g., 'users' or 'stufio.modules.users')
            
    #     Returns:
    #         List of event consumers
    #     """
    #     # if not settings.events_APP_CONSUME_ROUTES:
    #     #     logger.debug("Skipping consumer discovery due to settings")
    #     #     return False

    #     # Normalize the module name
    #     if not module_name.startswith("stufio.modules."):
    #         import_path = f"stufio.modules.{module_name}"
    #     else:
    #         import_path = module_name

    #     # Remove any path-like elements
    #     if "/" in import_path or "\\" in import_path:
    #         parts = import_path.split(".")
    #         filtered_parts = [p for p in parts if "/" not in p and "\\" not in p]
    #         import_path = ".".join(filtered_parts)

    #     logger.info(f"Attempting to discover events in module {import_path}")

    #     try:
    #         # Import the module's events.py file
    #         __import__(f"{import_path}.consumers", fromlist=[""])
    #         return True
    #     except ImportError as e:
    #         logger.warning(f"Module {import_path} does not have an consumers subfolder or it cannot be imported: {e}")
    #         return False
    #     except Exception as e:
    #         logger.error(f"Error discovering consumers in {import_path}: {e}", exc_info=True)
    #         return False

    # def register_module(self, module_name: str, app: FastAPI) -> None:
    #     """Register a module's events with this registry."""
    #     # Normalize the module name
    #     if module_name.startswith("stufio.modules."):
    #         short_name = module_name.split(".")[-1]
    #     else:
    #         short_name = module_name

    #     if short_name in self._registered_modules:
    #         logger.debug(f"Module {short_name} already registered")
    #         return

    #     # Discover events from the module
    #     events = self.discover_events(module_name)

    #     if not events:
    #         logger.warning(f"No events found in module {module_name}")
    #         return

    #     # Register each event
    #     for event_dict in events:
    #         if not event_dict.get("name"):
    #             logger.warning(f"Skipping event with missing name in module {module_name}")
    #             continue

    #         # Debug: print the event definition structure
    #         logger.debug(f"Registering event: {event_dict.get('name')}")
    #         logger.debug(f"Event data types: {[(k, type(v).__name__) for k, v in event_dict.items()]}")

    #         # event_dict is already a dictionary, so pass it directly
    #         asyncio.create_task(self.register_event(event_dict))

    #     logger.info(f"Registered {len(events)} events from module {module_name}")

    #     # discover consumers
    #     has_consumers = self.discover_consumers(module_name)
    #     if not has_consumers:
    #         logger.warning(f"No consumers found in module {module_name}")

    #     self._registered_modules.add(short_name)

    # async def register_all_modules(self, app: FastAPI) -> None:
    #     """Register all modules from the ModuleRegistry."""
    #     # First register events from the events module itself
    #     try:
    #         self.register_module("events", app)
    #         logger.info("Registered events from events module")
    #     except Exception as e:
    #         logger.error(f"Error registering events from events module: {e}", exc_info=True)

    #     # Register events from other discovered modules
    #     try:
    #         # Get the list of registered modules from the module registry
    #         registered_modules = list(module_registry.modules.keys())
    #         logger.info(f"Found {len(registered_modules)} modules in registry")

    #         for module_name in registered_modules:
    #             # Skip events module as it's already registered
    #             if module_name == "events":
    #                 continue

    #             try:
    #                 self.register_module(module_name, app)
    #             except Exception as e:
    #                 logger.error(f"Error registering events from {module_name}: {e}", exc_info=True)
    #     except Exception as e:
    #         logger.error(f"Error discovering modules from registry: {e}", exc_info=True)

    # Decorator for subscribing to events
    def on(self, entity_type: str, action: str) -> Callable:
        """Decorator for subscribing to events."""
        def decorator(func: EventHandler) -> EventHandler:
            self.subscribe_memory(entity_type, action, func)
            return func
        return decorator


# Create a global instance of the registry
event_registry = EventRegistry()
