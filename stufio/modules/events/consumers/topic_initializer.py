import asyncio
import inspect
import logging
import importlib
from typing import Dict, List, Set, Type, Optional, Any
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from pydantic import BaseModel

from stufio.core.config import get_settings
from stufio.modules.events.schemas.event_definition import EventDefinition

logger = logging.getLogger(__name__)
settings = get_settings()

class TopicConfig(BaseModel):
    """Configuration for a Kafka topic."""
    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    config: Dict[str, str] = {}
    description: Optional[str] = None
    source_event: Optional[str] = None

class KafkaTopicInitializer:
    """Initializes Kafka topics for the application based on event definitions."""

    def __init__(self):
        self.admin_client = None
        self.topics: Dict[str, TopicConfig] = {}
        self.discovered_modules: Set[str] = set()

    async def initialize_client(self) -> None:
        """Initialize the Kafka admin client."""
        if self.admin_client is None:
            try:
                bootstrap_servers = getattr(settings, "events_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
                self.admin_client = AIOKafkaAdminClient(
                    bootstrap_servers=bootstrap_servers,
                    client_id=f"stufio-topic-initializer"
                )
                await self.admin_client.start()
                logger.info(f"Connected to Kafka at {bootstrap_servers}")
            except Exception as e:
                logger.error(f"Failed to connect to Kafka: {e}", exc_info=True)
                raise

    async def close_client(self) -> None:
        """Close the Kafka admin client."""
        if self.admin_client:
            await self.admin_client.close()
            self.admin_client = None
            logger.info("Closed Kafka admin client")

    def register_topic(self, config: TopicConfig) -> None:
        """Register a topic configuration."""
        self.topics[config.name] = config
        logger.debug(f"Registered topic configuration: {config.name}")

    def extract_from_event_definition(self, event_class: Type[EventDefinition]) -> Optional[TopicConfig]:
        """Extract topic configuration from an event definition class."""
        try:
            # Get event attributes
            event_attrs = getattr(event_class, "_event_attrs", {})

            # Normalize attributes that might be tuples
            entity_type = event_attrs.get('entity_type')
            action = event_attrs.get('action')

            if isinstance(entity_type, tuple) and entity_type:
                entity_type = entity_type[0]

            if isinstance(action, tuple) and action:
                action = action[0]

            # Check for custom topic configuration
            custom_topic = event_attrs.get('topic')
            is_high_volume = event_attrs.get('high_volume', False)

            # Determine topic name
            if custom_topic:
                topic_name = custom_topic
            else:
                topic_prefix = getattr(settings, "events_KAFKA_TOPIC_PREFIX", "stufio.events")
                topic_name = f"{topic_prefix}"
                if is_high_volume:
                    topic_name = f"{topic_prefix}.{entity_type}.{action}"

            if self.topics.get(topic_name):
                logger.warning(f"Topic {topic_name} already registered, skipping")
                return None

            # Determine partitions
            partitions = event_attrs.get(
                "partitions", getattr(settings, "events_KAFKA_DEFAULT_PARTITIONS", 9)
            )
            if is_high_volume and not event_attrs.get('partitions'):
                partitions = getattr(
                    settings, "events_KAFKA_DEFAULT_HL_PARTITIONS", 9
                )  # Default higher partition count for high-volume events

            # Create topic config
            return TopicConfig(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=getattr(settings, "events_KAFKA_REPLICATION_FACTOR", 1),
                config={
                    "cleanup.policy": "delete",
                    "retention.ms": str(getattr(settings, "events_KAFKA_RETENTION_MS", 604800000)),  # 7 days
                    "segment.bytes": str(getattr(settings, "events_KAFKA_SEGMENT_BYTES", 1073741824)),  # 1GB
                },
                description=event_attrs.get('description'),
                source_event=event_class.__name__
            )
        except Exception as e:
            logger.error(f"Error extracting topic from {event_class.__name__}: {e}", exc_info=True)
            return None

    def discover_events_in_module(self, module_path: str) -> List[Type[EventDefinition]]:
        """Discover all event definition classes in a module."""
        try:
            module = importlib.import_module(module_path)
            events = []

            # First check for ALL_EVENTS list
            if hasattr(module, "ALL_EVENTS") and isinstance(module.ALL_EVENTS, list):
                for event_class in module.ALL_EVENTS:
                    if (inspect.isclass(event_class) and 
                        issubclass(event_class, EventDefinition) and 
                        event_class != EventDefinition):
                        events.append(event_class)
            else:
                # Fall back to inspecting module members
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and 
                        issubclass(obj, EventDefinition) and 
                        obj != EventDefinition and
                        obj.__module__ == module_path):
                        events.append(obj)

            logger.info(f"Discovered {len(events)} event(s) in {module_path}")
            return events
        except ImportError as e:
            logger.warning(f"Could not import module {module_path}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error discovering events in {module_path}: {e}", exc_info=True)
            return []

    def discover_events_from_registry(self) -> List[Type[EventDefinition]]:
        """Discover all event definition classes from the module registry."""
        from stufio.core.module_registry import registry
        all_events = []

        # Process core events module
        events_module_path = "stufio.modules.events.events"
        events = self.discover_events_in_module(events_module_path)
        all_events.extend(events)

        # Process events from other modules
        for module_name in registry.discovered_modules():
            if module_name in self.discovered_modules:
                continue

            try:
                module_path = f"stufio.modules.{module_name}.events"
                module_events = self.discover_events_in_module(module_path)
                all_events.extend(module_events)
                self.discovered_modules.add(module_name)
            except Exception as e:
                logger.error(f"Error discovering events in module {module_name}: {e}", exc_info=True)

        return all_events

    async def create_topic(self, config: TopicConfig) -> bool:
        """Create a Kafka topic if it doesn't exist."""
        try:
            # Check if the topic already exists
            existing_topics = await self.admin_client.list_topics()

            if config.name in existing_topics:
                logger.info(f"Topic already exists: {config.name}")
                return True

            # Create the topic
            topic = NewTopic(
                name=config.name,
                num_partitions=config.num_partitions,
                replication_factor=config.replication_factor,
                topic_configs=config.config
            )

            await self.admin_client.create_topics([topic])
            logger.info(f"Created topic: {config.name} with {config.num_partitions} partitions")
            return True
        except Exception as e:
            logger.error(f"Failed to create topic {config.name}: {e}", exc_info=True)
            return False

    async def initialize_topics(self) -> int:
        """Initialize all registered topics."""
        if not getattr(settings, "events_KAFKA_ENABLED", True):
            logger.info("Kafka is disabled, skipping topic initialization")
            return 0

        # Clear topics registry to allow fresh discovery
        self.topics = {}

        # Initialize the Kafka admin client
        await self.initialize_client()

        try:
            # Discover events and register topics
            events = self.discover_events_from_registry()
            logger.info(f"Discovered {len(events)} event classes")

            # Register default shared topic
            default_topic = TopicConfig(
                name=getattr(settings, "events_KAFKA_TOPIC_PREFIX", "stufio.events"),
                num_partitions=getattr(
                    settings, "events_KAFKA_DEFAULT_HL_PARTITIONS", 9
                ),
                replication_factor=getattr(
                    settings, "events_KAFKA_REPLICATION_FACTOR", 1
                ),
                config={
                    "cleanup.policy": "delete",
                    "retention.ms": str(
                        getattr(settings, "events_KAFKA_RETENTION_MS", 604800000)
                    ),
                },
                description="Default shared topic for most events",
            )
            self.register_topic(default_topic)

            # Extract and register topics from event definitions
            for event_class in events:
                topic_config = self.extract_from_event_definition(event_class)
                if topic_config:
                    self.register_topic(topic_config)

            # Create all registered topics
            created_count = 0
            for topic_name, config in self.topics.items():
                success = await self.create_topic(config)
                if success:
                    created_count += 1

            return created_count
        except Exception as e:
            logger.error(f"Failed to initialize topics: {e}", exc_info=True)
            return 0
        finally:
            # Close the Kafka admin client
            await self.close_client()

# Create a global instance
topic_initializer = KafkaTopicInitializer()

async def initialize_kafka_topics() -> int:
    """Initialize Kafka topics for the application."""
    return await topic_initializer.initialize_topics()
