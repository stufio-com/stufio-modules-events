from typing import Dict, List, Optional, Callable, Any, Set, Tuple
from odmantic import AIOEngine
from .schemas.event import EventDefinition, EventSubscription, EventMessage
from .crud import crud_event_definitions, crud_event_subscriptions
from stufio.core.module_registry import registry as module_registry
import logging
import asyncio
import inspect

logger = logging.getLogger(__name__)

# Type for event handlers
EventHandler = Callable[[EventMessage], Any]


class EventRegistry:
    """Registry for event definitions and subscriptions."""

    def __init__(self, mongo_engine: Optional[AIOEngine] = None):
        self.mongo_engine = mongo_engine

        # Database caches
        self._definitions_cache: Dict[str, EventDefinition] = {}
        self._db_subscriptions_cache: Dict[str, List[EventSubscription]] = {}

        # In-memory handlers
        self._memory_handlers: Dict[str, List[EventHandler]] = {}

        # Set of registered modules
        self._registered_modules: Set[str] = set()

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

    async def register_event(self, definition: EventDefinition) -> EventDefinition:
        """Register a new event definition in the database."""
        if not self.mongo_engine:
            logger.warning("Mongo engine not initialized, skipping database registration")
            self._definitions_cache[definition.name] = definition
            return definition

        # Check if event is already registered
        existing = await crud_event_definitions.get_by_name(self.mongo_engine, definition.name)
        if existing:
            # Update existing definition
            updated = await crud_event_definitions.update(
                self.mongo_engine, 
                existing.id, 
                definition.model_dump(exclude={"id", "created_at"})
            )
            self._definitions_cache[definition.name] = updated
            return updated

        # Create new definition
        created = await crud_event_definitions.create(self.mongo_engine, definition)
        self._definitions_cache[definition.name] = created
        return created

    async def subscribe(self, subscription: EventSubscription) -> EventSubscription:
        """Register a new event subscription in the database."""
        if not self.mongo_engine:
            logger.warning("Mongo engine not initialized, skipping database subscription")
            return subscription

        created = await crud_event_subscriptions.create(self.mongo_engine, subscription)

        # Update cache
        cache_key = self._get_event_key(subscription.entity_type or '*', subscription.action or '*')
        if cache_key not in self._db_subscriptions_cache:
            self._db_subscriptions_cache[cache_key] = []
        self._db_subscriptions_cache[cache_key].append(created)

        return created

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Remove an event subscription from the database."""
        if not self.mongo_engine:
            logger.warning("Mongo engine not initialized, skipping database unsubscribe")
            return False

        # Get the subscription first to update cache
        subscription = await crud_event_subscriptions.get(self.mongo_engine, subscription_id)
        if not subscription:
            return False

        # Delete from database
        deleted = await crud_event_subscriptions.remove(self.mongo_engine, subscription_id)

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
        if not self.mongo_engine:
            logger.warning("Mongo engine not initialized, returning empty subscription list")
            return []

        # Try cache first
        cache_key = self._get_event_key(entity_type, action)
        if cache_key in self._db_subscriptions_cache:
            return self._db_subscriptions_cache[cache_key]

        # Query database
        subscriptions = await crud_event_subscriptions.get_by_entity_action(
            self.mongo_engine, entity_type, action
        )

        # Update cache
        self._db_subscriptions_cache[cache_key] = subscriptions

        return subscriptions

    async def get_all_events(self) -> List[EventDefinition]:
        """Get all registered event definitions."""
        if not self.mongo_engine:
            logger.warning("Mongo engine not initialized, returning cached definitions")
            return list(self._definitions_cache.values())

        # If cache is empty, populate it
        if not self._definitions_cache:
            all_definitions = await crud_event_definitions.get_multi(self.mongo_engine)
            self._definitions_cache = {d.name: d for d in all_definitions}
            return all_definitions

        return list(self._definitions_cache.values())

    async def refresh_cache(self) -> None:
        """Refresh the internal caches from the database."""
        if not self.mongo_engine:
            logger.warning("Mongo engine not initialized, skipping cache refresh")
            return

        # Clear and repopulate definitions cache
        all_definitions = await crud_event_definitions.get_multi(self.mongo_engine)
        self._definitions_cache = {d.name: d for d in all_definitions}

        # Clear and repopulate subscriptions cache
        self._db_subscriptions_cache = {}
        all_subscriptions = await crud_event_subscriptions.get_multi(self.mongo_engine)

        for sub in all_subscriptions:
            entity_type = sub.entity_type or '*'
            action = sub.action or '*'
            cache_key = self._get_event_key(entity_type, action)

            if cache_key not in self._db_subscriptions_cache:
                self._db_subscriptions_cache[cache_key] = []

            self._db_subscriptions_cache[cache_key].append(sub)

    def discover_events(self, module_import_path: str) -> List[Dict[str, Any]]:
        """
        Discover all event definitions in a module.
        
        Args:
            module_name: Full module name (e.g., 'stufio.modules.users')
            
        Returns:
            List of event definitions as dictionaries
        """
        try:
            # Import the module's events.py file
            events_module = __import__(f"{module_import_path}.events", fromlist=[""])

            # Find all EventDefinition subclasses
            events = []
            for name, obj in inspect.getmembers(events_module):
                if (inspect.isclass(obj) and 
                    issubclass(obj, EventDefinition) and 
                    obj != EventDefinition):
                    events.append(obj.to_dict())

            return events
        except ImportError:
            # Module doesn't have events.py
            logger.warning(
                f"Module {module_import_path} does not have an events.py file"
            )
            return []
        except Exception as e:
            # Log the error but don't crash
            logger.error(f"Error discovering events in {module_import_path}: {e}")
            return []

    def register_module(self, module_name: str, module_import_path: str = '') -> None:
        """Register a module's events with this registry."""
        if module_name in self._registered_modules:
            return

        if not module_import_path:
            module_import_path = f"stufio.modules.{module_name}"

        # Discover events from the module
        events = self.discover_events(module_import_path)

        # Register each event
        for event_dict in events:
            asyncio.create_task(self.register_event(EventDefinition(**event_dict)))

        self._registered_modules.add(module_name)
        logger.info(f"Registered {len(events)} events from module {module_name}")

    # Decorator for subscribing to events
    def on(self, entity_type: str, action: str) -> Callable:
        """Decorator for subscribing to events."""
        def decorator(func: EventHandler) -> EventHandler:
            self.subscribe_memory(entity_type, action, func)
            return func
        return decorator

    async def register_all_modules(self):
        """Register all default events from the events module."""
        try:
            self.register_module("events")
            logger.info("Registered events from stufio.modules.events")
        except Exception as e:
            logger.error(f"Error registering events from stufio.modules.events: {e}")

        # Register events from other discovered modules
        discovered_modules = module_registry.discovered_modules()
        logger.info(f"ModuleRegistry discovered {len(discovered_modules)} modules")

        for module_name, module_import_path in discovered_modules.items():
            try:
                self.register_module(module_import_path)
                self.logger.info(f"Registered events from {module_import_path}")
            except Exception as e:
                self.logger.error(f"Error registering events from {module_import_path}: {e}")

        # Allow registry to process async event registrations
        # await asyncio.sleep(0.1)  # Small delay to allow async tasks to run
