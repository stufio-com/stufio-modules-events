import trace
import traceback
from faststream.kafka.fastapi import KafkaRouter
from stufio.core.config import get_settings
import logging
import weakref
import atexit
from typing import (
    TYPE_CHECKING,
    Any,
)

if TYPE_CHECKING:
    from faststream.broker.message import StreamMessage

settings = get_settings()
logger = logging.getLogger(__name__)

# Track all Kafka producers for proper cleanup
_producers = weakref.WeakSet()

# Use None as default instead of initializing at import time
kafka_router = None
kafka_broker = None
_initialized = False

# Create a proper mock object if Kafka is disabled
class KafkaBrokerMock:
    """Mock class for Kafka broker when Kafka is disabled."""

    def __init__(self, logger):
        self.logger = logger

    def subscriber(self, *args, **kwargs):
        """Mock subscriber decorator that just returns the function unchanged."""

        def decorator(func):
            return func

        return decorator

    async def publish(self, *args, **kwargs):
        """Mock publish method."""
        self.logger.debug(
            "Kafka publishing disabled. Set events_KAFKA_ENABLED=True to enable."
        )
        return None

    async def start(self):
        """Mock start method."""
        pass

    async def shutdown(self):
        """Mock shutdown method."""
        pass

    async def close(self):
        """Mock close method."""
        pass


def initialize_kafka():
    """Initialize Kafka broker and router lazily to avoid import-time resources."""
    global kafka_router, kafka_broker, _initialized, _producers

    if _initialized:
        return

    kafka_enabled = getattr(settings, "events_KAFKA_ENABLED", False)

    if not kafka_enabled:
        kafka_router = KafkaBrokerMock(logger)
        kafka_broker = KafkaBrokerMock(logger)
        logger.debug("Kafka is not enabled in settings. Set events_KAFKA_ENABLED=True to enable.")
        _initialized = True
        return

    try:
        # Cleanup any existing producers before creating new ones
        await_shutdown_pending_producers()

        logger.info(f"Initializing Kafka router with bootstrap servers: {settings.events_KAFKA_BOOTSTRAP_SERVERS}")

        # Create the router with only supported parameters
        kafka_router = KafkaRouter(
            settings.events_KAFKA_BOOTSTRAP_SERVERS,
            schema_url="/asyncapi",
            include_in_schema=True,
            log_level=logging.DEBUG,
            setup_state=False,
        )

        # Get the broker reference
        kafka_broker = kafka_router.broker

        # Track the producer if it exists
        if hasattr(kafka_broker, "_producer") and kafka_broker._producer:
            _producers.add(kafka_broker._producer)

        logger.info("Kafka router initialized successfully")
        _initialized = True
    except Exception as e:
        logger.error(f"❌ Failed to initialize Kafka router: {e}")
        # Set to None in case of error
        kafka_router = None
        kafka_broker = None


def await_shutdown_pending_producers():
    """Find and close any pending producers from previous runs."""
    try:
        import aiokafka
        import gc
        import asyncio

        # Find all AIOKafkaProducer instances
        producers = [obj for obj in gc.get_objects()
                     if isinstance(obj, aiokafka.AIOKafkaProducer)
                     and not getattr(obj, "_closed", False)]

        if producers:
            logger.debug(f"Found {len(producers)} unclosed AIOKafkaProducer instances from previous run")

            # Create an event loop if needed
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # If no event loop exists, create a new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Run the cleanup in the event loop
            for producer in producers:
                try:
                    if hasattr(producer, "stop") and not getattr(producer, "_closed", False):
                        loop.run_until_complete(producer.stop())
                        producer._closed = True
                        logger.info(f"Closed leftover producer {producer}")
                except Exception as e:
                    logger.debug(f"Error closing leftover producer: {e}")
    except Exception as e:
        logger.error(f"Error cleaning up pending producers: {e}")


# Register atexit handler for cleanup
atexit.register(await_shutdown_pending_producers)


async def force_shutdown_kafka():
    """Force shutdown all Kafka resources."""
    global kafka_broker, kafka_router, _producers

    if not kafka_broker or isinstance(kafka_broker, KafkaBrokerMock):
        return

    logger.info("Forcing complete Kafka shutdown")

    # 1. Shutdown producers first
    try:
        import aiokafka
        import asyncio
        import gc

        # Find all AIOKafkaProducer instances
        producers = [obj for obj in gc.get_objects()
                     if isinstance(obj, aiokafka.AIOKafkaProducer)
                     and not getattr(obj, "_closed", False)]

        if producers:
            logger.info(f"Found {len(producers)} unclosed AIOKafkaProducer instances")
            for producer in producers:
                try:
                    logger.info(f"Force closing producer {producer}")
                    if not producer._closed:
                        await producer.stop()
                        producer._closed = True
                except Exception as e:
                    logger.debug(f"Error force-closing producer: {e}")

        # Also close tracked producers
        for producer in _producers:
            try:
                if producer and not getattr(producer, "_closed", False):
                    logger.info(f"Closing tracked producer {producer}")
                    await producer.stop()
                    producer._closed = True
            except Exception as e:
                logger.debug(f"Error closing tracked producer: {e}")

        # 2. Force close all aiokafka consumers
        consumers = [obj for obj in gc.get_objects()
                     if isinstance(obj, aiokafka.AIOKafkaConsumer)
                     and not getattr(obj, "_closed", False)]

        if consumers:
            logger.info(f"Found {len(consumers)} unclosed AIOKafkaConsumer instances")
            for consumer in consumers:
                try:
                    logger.info(f"Force closing consumer {consumer}")
                    if not consumer._closed:
                        await consumer.stop()
                        consumer._closed = True
                except Exception as e:
                    logger.debug(f"Error force-closing consumer: {e}")

        # 3. Close the broker explicitly
        if hasattr(kafka_broker, "shutdown"):
            logger.info("Calling broker shutdown()")
            await kafka_broker.shutdown()
        elif hasattr(kafka_broker, "close"):
            logger.info("Calling broker close()")
            await kafka_broker.close()

        # Wait a moment for resources to be released
        await asyncio.sleep(0.5)

    except Exception as e:
        logger.error(f"Error during force shutdown: {e}", exc_info=True)

    # Reset global references
    kafka_broker = None
    kafka_router = None
    _producers.clear()
    _initialized = False


# Only create proxy getters now to support lazy initialization
def get_kafka_router():
    """Get or initialize the Kafka router."""
    if kafka_router is None:
        initialize_kafka()
    return kafka_router


def get_kafka_broker():
    """Get or initialize the Kafka broker."""
    if kafka_broker is None:
        initialize_kafka()
    return kafka_broker


async def header_filter(msg: "StreamMessage[Any]", **headers) -> bool:
    """Filter messages based on their Kafka headers.
    
    Args:
        msg: The message to filter
        **headers: Key-value pairs to match against message headers
                  (e.g., entity_type=b"user", action=b"login")
    
    Returns:
        bool: True if the message headers match all provided criteria, False otherwise
    """
    # Check if message has headers
    if not hasattr(msg, "raw_message") or not hasattr(msg.raw_message, "headers"):
        return False

    # Convert message headers to dict for easier matching
    msg_headers = {}
    for k, v in msg.raw_message.headers or []:
        msg_headers[k] = v

    # Check if all specified headers match
    for key, value in headers.items():
        if msg_headers.get(key) != value:
            return False
    return True


# Factory function to create header filters with specific criteria
def create_header_filter(**headers):
    """Create a filter function for message headers.
    
    Args:
        **headers: Key-value pairs to match against message headers
                 (e.g., entity_type=b"user", action=b"login")
    
    Returns:
        An async filter function compatible with FastStream
    """
    async def filter_func(msg):
        return await header_filter(msg, **headers)

    # Copy docstring and add filter criteria for better debugging
    filter_criteria = ", ".join(f"{k}={v!r}" for k, v in headers.items())
    filter_func.__doc__ = f"Filter messages with headers: {filter_criteria}"
    return filter_func


# Make initialization happen when the module is imported
await_shutdown_pending_producers()


async def correlation_id_middleware(msg):
    """Middleware to extract and set correlation_id from message headers."""
    try:
        from ..utils.context import TaskContext

        # Extract headers if available
        headers = {}
        correlation_id = None

        logger.debug(f"correlation_id_middleware processing message type: {type(msg).__name__}")

        # DIRECT ACCESS: Handle ConsumerRecord objects directly (aiokafka)
        if hasattr(msg, "headers") and msg.headers is not None:
            # Direct access for ConsumerRecord objects
            raw_headers = msg.headers
            logger.debug(f"Direct raw headers: {raw_headers}")

            headers = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                      v.decode('utf-8') if isinstance(v, bytes) else v 
                      for k, v in raw_headers}

            logger.debug(f"Direct processed headers: {headers}")

        # FASTSTREAM ACCESS: Handle FastStream message objects
        elif hasattr(msg, "raw_message") and hasattr(msg.raw_message, "headers"):
            raw_headers = msg.raw_message.headers or []
            logger.debug(f"FastStream raw headers: {raw_headers}")

            headers = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                      v.decode('utf-8') if isinstance(v, bytes) else v 
                      for k, v in raw_headers}

            logger.debug(f"FastStream processed headers: {headers}")

        # RAW ATTEMPT: Try an even more direct approach
        if not headers and hasattr(msg, "_message") and hasattr(msg._message, "headers"):
            raw_headers = msg._message.headers or []
            logger.debug(f"Raw _message headers: {raw_headers}")

            headers = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                      v.decode('utf-8') if isinstance(v, bytes) else v 
                      for k, v in raw_headers}

            logger.debug(f"Processed _message headers: {headers}")

        # Try to get correlation_id from headers first
        correlation_id = headers.get('correlation_id')
        if correlation_id:
            before_id = TaskContext.get_correlation_id()
            TaskContext.set_correlation_id(correlation_id)
            after_id = TaskContext.get_correlation_id()

            logger.debug(
                f"✅ Set correlation_id from headers: '{correlation_id}' (changed from '{before_id}' to '{after_id}')"
            )
            return True
        else:
            logger.debug("No correlation_id found in headers")

        # If no correlation_id in headers, try message body as JSON
        if not correlation_id and hasattr(msg, 'value') and isinstance(msg.value, bytes):
            try:
                import json
                body_data = json.loads(msg.value.decode('utf-8'))
                if isinstance(body_data, dict) and 'correlation_id' in body_data:
                    correlation_id = body_data['correlation_id']
                    before_id = TaskContext.get_correlation_id()
                    TaskContext.set_correlation_id(correlation_id)
                    after_id = TaskContext.get_correlation_id()

                    logger.debug(
                        f"✅ Set correlation_id from JSON body: '{correlation_id}' (changed from '{before_id}' to '{after_id}')"
                    )
                    return True
                else:
                    logger.debug(f"JSON body parsed but no correlation_id found. Keys: {list(body_data.keys()) if isinstance(body_data, dict) else 'not a dict'}")
            except Exception as e:
                logger.debug(f"❌ Failed to parse message value as JSON: {e}")

        # Try other message body methods
        for attr_name in ['_decoded_body', 'body']:
            if hasattr(msg, attr_name) and isinstance(getattr(msg, attr_name), dict):
                body_data = getattr(msg, attr_name)
                correlation_id = body_data.get('correlation_id')
                if correlation_id:
                    before_id = TaskContext.get_correlation_id()
                    TaskContext.set_correlation_id(correlation_id)
                    after_id = TaskContext.get_correlation_id()

                    logger.debug(
                        f"✅ Set correlation_id from {attr_name}: '{correlation_id}' (changed from '{before_id}' to '{after_id}')"
                    )
                    return True
                else:
                    logger.debug(f"No correlation_id found in {attr_name}")

        logger.debug("⚠️ No correlation_id found in any message location")
        # logger.warning(f"Message headers: {headers}")
        # logger.warning(f"Message body: {msg}")
        return False
    except Exception as e:
        logger.error(f"❌ Error in correlation_id_middleware: {e}", exc_info=True)
        return False


# Updated exports that promote lazy initialization
__all__ = ["get_kafka_router", "get_kafka_broker", "force_shutdown_kafka", "header_filter", "create_header_filter", "initialize_kafka", "correlation_id_middleware"]
