from typing import Dict, Any, Optional, List, Callable, Awaitable
from faststream.kafka import KafkaBroker, Stream, KafkaProducer
from faststream.exceptions import FastStreamError
import json
import logging
from .schemas.event import EventMessage

logger = logging.getLogger(__name__)

class KafkaClient:
    """Kafka client for publishing and consuming events."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.broker: Optional[KafkaBroker] = None
        self.producers: Dict[str, KafkaProducer] = {}
        self.streams: Dict[str, Stream] = {}
        self.event_handlers: Dict[str, List[Callable[[EventMessage], Awaitable[None]]]] = {}
        
        # Default config
        self.bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")
        self.default_topic = config.get("default_topic", "stufio_events")
        
    async def startup(self) -> None:
        """Initialize Kafka broker on startup."""
        try:
            self.broker = KafkaBroker(self.bootstrap_servers)
            await self.broker.start()
            
            # Create default producer for main events topic
            self.producers[self.default_topic] = self.broker.get_producer(self.default_topic)
            
            logger.info(f"Kafka client initialized with broker: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka client: {str(e)}")
            raise
    
    async def shutdown(self) -> None:
        """Clean up Kafka resources on shutdown."""
        if self.broker:
            await self.broker.close()
            logger.info("Kafka client shut down")
    
    async def publish_event(
        self, 
        event: EventMessage, 
        topic: Optional[str] = None
    ) -> bool:
        """Publish an event to Kafka."""
        if not self.broker:
            logger.error("Kafka broker not initialized")
            return False
        
        target_topic = topic or self.default_topic
        
        try:
            # Get or create producer for this topic
            if target_topic not in self.producers:
                self.producers[target_topic] = self.broker.get_producer(target_topic)
            
            # Publish event
            event_dict = event.model_dump()
            await self.producers[target_topic].publish(event_dict)
            logger.debug(f"Published event {event.event_id} to topic {target_topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish event to Kafka: {str(e)}")
            return False
    
    async def subscribe(
        self,
        topic: str,
        handler: Callable[[EventMessage], Awaitable[None]]
    ) -> None:
        """Subscribe to events on a Kafka topic."""
        if not self.broker:
            logger.error("Kafka broker not initialized")
            return
        
        try:
            # Register handler
            if topic not in self.event_handlers:
                self.event_handlers[topic] = []
            self.event_handlers[topic].append(handler)
            
            # Create stream if not exists
            if topic not in self.streams:
                # Define message handler
                async def message_handler(msg: Dict[str, Any]) -> None:
                    try:
                        event = EventMessage(**msg)
                        for h in self.event_handlers[topic]:
                            await h(event)
                    except Exception as e:
                        logger.error(f"Error handling Kafka message: {str(e)}")
                
                # Create stream
                self.streams[topic] = await self.broker.subscribe(
                    topic,
                    message_handler,
                    group_id=f"stufio-events-{topic}"
                )
                
            logger.info(f"Subscribed to Kafka topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to subscribe to Kafka topic {topic}: {str(e)}")