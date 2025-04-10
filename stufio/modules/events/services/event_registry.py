from typing import Dict, List, Optional, Callable, Any, Type, Set
from ..schemas.event import EventMessage
from ..schemas.event_definition import EventDefinition
import logging

logger = logging.getLogger(__name__)

# Type for event handlers
EventHandler = Callable[[EventMessage], Any]


class EventRegistry:
    """Registry for event definitions across modules."""

    _instance = None
    _initialized = False

    _registered_events: Dict[str, Type[EventDefinition]]
    _registered_modules: Set[str]

    _memory_handlers: Dict[str, List[EventHandler]]
    _http_subscribers: Dict[str, List[Any]]  # Added for HTTP subscribers

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EventRegistry, cls).__new__(cls)
            cls._instance._registered_events = {}
            cls._instance._registered_modules = set()
            # In-memory handlers
            cls._instance._memory_handlers = {}
            # HTTP subscribers
            cls._instance._http_subscribers = {}

        return cls._instance

    @property
    def registered_events(self) -> Dict[str, Type[EventDefinition]]:
        """Get all registered events."""
        return self._registered_events

    def get_module_events(self, module_name: str) -> List[Type[EventDefinition]]:
        """Get all events registered for a specific module."""
        return [e for e in self._registered_events.values() if getattr(e, "_module", None) == module_name]

    def register_event(self, event_class: Type[EventDefinition], module_name: str) -> Type[EventDefinition]:
        """Register an event definition class from a module."""
        # Get the event name for the registry
        entity_type = event_class.entity_type
        action = event_class.action

        # Handle tuple attributes if present (like in your EventsModule events)
        if isinstance(entity_type, tuple) and entity_type:
            entity_type = entity_type[0]
        if isinstance(action, tuple) and action:
            action = action[0]

        event_key = f"{entity_type}.{action}"

        # Store the event class and its module origin
        self._registered_events[event_key] = event_class
        setattr(event_class, "_module", module_name)

        # Track that we've registered events for this module
        self._registered_modules.add(module_name)

        return event_class

    def register_module_events(self, module_name: str, events: List[Type[EventDefinition]]) -> None:
        """Register all events from a module at once."""
        for event_class in events:
            self.register_event(event_class, module_name)

    def get_event(self, entity_type: str, action: str) -> Optional[Type[EventDefinition]]:
        """Get an event class by entity type and action."""
        event_key = f"{entity_type}.{action}"
        return self._registered_events.get(event_key)

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

    def subscribe(self, entity_type: str, action: str, handler: Callable) -> None:
        """Subscribe to an event with an in-memory handler function."""
        # Fixed method name to match what's used in EventBus
        self.subscribe_memory(entity_type, action, handler)

    async def get_matching_subscriptions(self, entity_type: str, action: str) -> List[Any]:
        """
        Get all HTTP subscribers for a specific event.
        
        Returns a list of subscription objects with callback_url attribute.
        """
        key = self._get_event_key(entity_type, action)
        
        # Get exact matches
        subscribers = self._http_subscribers.get(key, []).copy()
        
        # Get wildcard entity matches
        wildcard_entity_key = self._get_event_key("*", action)
        subscribers.extend(self._http_subscribers.get(wildcard_entity_key, []))
        
        # Get wildcard action matches
        wildcard_action_key = self._get_event_key(entity_type, "*")
        subscribers.extend(self._http_subscribers.get(wildcard_action_key, []))
        
        # Get full wildcard matches
        full_wildcard_key = self._get_event_key("*", "*")
        subscribers.extend(self._http_subscribers.get(full_wildcard_key, []))
        
        return subscribers
    
    def register_http_subscription(self, entity_type: str, action: str, callback_url: str, 
                                  subscription_id: str = None) -> str:
        """
        Register an HTTP subscription for an event.
        
        Args:
            entity_type: Entity type to subscribe to
            action: Action to subscribe to
            callback_url: URL to call when event occurs
            subscription_id: Optional ID for the subscription
            
        Returns:
            Subscription ID
        """
        from uuid import uuid4
        
        key = self._get_event_key(entity_type, action)
        
        if key not in self._http_subscribers:
            self._http_subscribers[key] = []
            
        # Generate subscription ID if not provided
        sub_id = subscription_id or str(uuid4())
        
        # Create subscription object
        subscription = {
            "id": sub_id,
            "entity_type": entity_type,
            "action": action,
            "callback_url": callback_url
        }
        
        # Add to subscribers list
        self._http_subscribers[key].append(subscription)
        logger.debug(f"Registered HTTP subscription {sub_id} for event {key} to {callback_url}")
        
        return sub_id

# Global singleton instance
event_registry = EventRegistry()
