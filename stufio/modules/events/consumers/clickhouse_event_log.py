from faststream.kafka.fastapi import KafkaMessage, Logger

from stufio.modules.events.consumers.asyncapi import stufio_event_subscriber
from stufio.modules.events.consumers import get_kafka_broker, get_kafka_router
from stufio.modules.events.events import UserCreatedEvent, UserLoginEvent, UserLoginPayload
from stufio.modules.events import ActorType
from stufio.modules.events.schemas import EventLogCreate
import json

from stufio.modules.events.schemas.payloads import UserCreatedPayload, UserPayload
from stufio.core.module_registry import registry

from ..crud import crud_event_log
from ..schemas.messages import BaseEventMessage
from stufio.core.config import get_settings

settings = get_settings()
# Keep the pattern for routing
event_logs_topics = [settings.events_KAFKA_TOPIC_PREFIX]
pattern = settings.events_KAFKA_TOPIC_PREFIX.replace('.', '\.') + "\.{entity_type}\.{action}"

import logging
logger = logging.getLogger(__name__)

kafka_router = get_kafka_router()
kafka_broker = get_kafka_broker()


# Generic event handler with automatic casting to the correct event message type
@kafka_broker.subscriber(
    *event_logs_topics,
    group_id=f"{settings.events_KAFKA_GROUP_ID}.*.*:events_log",
    include_in_schema=False,
    # Replace consumer_config with individual parameters
    max_poll_interval_ms=300000,
    session_timeout_ms=60000,
    heartbeat_interval_ms=20000,
    auto_offset_reset="earliest"
)
async def log_event_to_clickhouse(msg: KafkaMessage) -> None:
    """
    Consume events from Kafka and log them to Clickhouse.
    
    The event type will be automatically determined based on message headers or topic path.
    """
    # Extract topic name from the raw_message
    topic = msg.raw_message.topic if hasattr(msg, 'raw_message') and hasattr(msg.raw_message, 'topic') else "unknown"
    correlation_id = msg.correlation_id if hasattr(msg, 'correlation_id') else None

    # Initialize default values
    entity_type = "unknown"
    action = "unknown"

    # FIRST: Try to extract from headers (this is the preferred method)
    if hasattr(msg, "raw_message") and hasattr(msg.raw_message, "headers"):
        # Convert message headers to dict for easier access
        msg_headers = {}
        for k, v in msg.raw_message.headers or []:
            # Convert bytes to string for easier use
            if isinstance(v, bytes):
                v = v.decode('utf-8')
            msg_headers[k] = v

        # Check if we have entity_type and action in headers
        if "entity_type" in msg_headers:
            entity_type = msg_headers["entity_type"]
            kafka_broker.logger.debug(f"Extracted entity_type from headers: {entity_type}")

        if "action" in msg_headers:
            action = msg_headers["action"]
            kafka_broker.logger.debug(f"Extracted action from headers: {action}")

    # SECOND: If headers didn't provide the info, try to extract from topic
    if entity_type == "unknown" or action == "unknown":
        # Extract entity_type and action from topic
        # Example: "stufio.events.user.login" -> entity_type="user", action="login"
        topic_parts = topic.split('.')
        if len(topic_parts) >= 3:
            # Only override if not already found in headers
            if entity_type == "unknown":
                entity_type = topic_parts[-2]
                kafka_broker.logger.debug(f"Extracted entity_type from topic: {entity_type}")

            if action == "unknown":
                action = topic_parts[-1]
                kafka_broker.logger.debug(f"Extracted action from topic: {action}")

    # Construct an event name for logging
    event_name = f"{entity_type}.{action}" if entity_type != "unknown" else topic

    # Get the body - this is the actual message content
    body = msg._decoded_body if hasattr(msg, '_decoded_body') else msg.body

    # Log with proper event name
    kafka_broker.logger.info(f"Received message on topic {topic} - event: {event_name}")

    # For debugging - log a portion of the body
    if body:
        body_str = json.dumps(body, default=str)[:200] + "..." if len(json.dumps(body, default=str)) > 200 else json.dumps(body, default=str)
        kafka_broker.logger.debug(f"Message body: {body_str}")

    try:
        # Process the message if it's an event
        if isinstance(body, dict):
            # Convert to BaseEventMessage
            event = BaseEventMessage.model_validate(body)
            kafka_broker.logger.info(f"Successfully parsed event {event_name} with ID {event.event_id}")

            # THIRD: Try to extract info from event data as last resort
            if entity_type == "unknown" and event.entity and hasattr(event.entity, 'type'):
                entity_type = event.entity.type
                kafka_broker.logger.debug(f"Extracted entity_type from event data: {entity_type}")

            if action == "unknown" and hasattr(event, 'action'):
                action = event.action
                kafka_broker.logger.debug(f"Extracted action from event data: {action}")

            # Extract actor information correctly
            actor_type = event.actor.type if event.actor else body.get("actor_type", "")
            actor_id = event.actor.id if event.actor else body.get("actor_id", "")

            # Get payload and metrics as JSON strings
            payload_json = None
            metrics_json = None

            if body.get('payload', False):
                try:
                    payload_json = json.dumps(body.get("payload", {}))
                except Exception as e:
                    kafka_broker.logger.warning(f"Failed to serialize payload: {e}")

            if body.get("metrics", False):
                try:
                    metrics_json = json.dumps(body.get("metrics", {}))
                except Exception as e:
                    kafka_broker.logger.warning(f"Failed to serialize metrics: {e}")

            # Create the EventLogCreate object with proper JSON strings
            try:
                event_log = EventLogCreate(
                    event_id=event.event_id,
                    entity_type=entity_type,
                    action=action,
                    entity_id=event.entity.id if event.entity else "",
                    payload=payload_json,  # JSON string
                    actor_type=actor_type,
                    actor_id=actor_id,
                    correlation_id=correlation_id,
                    timestamp=event.timestamp,
                    metrics=metrics_json,  # JSON string
                )

                # Save to ClickHouse
                await crud_event_log.create(event_log)

                kafka_broker.logger.info(
                    f"Event {event_name} logged to Clickhouse with ID {event.event_id}"
                )

                return "ok"
            except Exception as e:
                kafka_broker.logger.error(f"Failed to create event log: {e}", exc_info=True)
    except Exception as e:
        kafka_broker.logger.error(f"Error processing event from {topic}: {str(e)}", exc_info=True)

    # If we reach here, something went wrong
    kafka_broker.logger.error(f"Failed to process event {event_name} from {topic}")
    if msg:
        body_str = f"{msg.body[:200]}..." if len(msg.body) > 200 else msg.body
        kafka_broker.logger.error(f"Message body: {body_str}")

    return None


# For shared topic events, use header filtering with explicit naming
@stufio_event_subscriber(UserCreatedEvent)
async def handle_user_created(
    event: BaseEventMessage[UserCreatedPayload], logger: Logger
) -> None:
    """Handle user created events with strong typing."""
    # This handler gets a properly typed UserCreatedMessage
    user = event.payload.after if event.payload else None
    if user:
        logger.info(f"User created: {user.user_id}, {user.email}")


# Make sure this has a different channel name
@stufio_event_subscriber(UserLoginEvent)
async def handle_user_login(
    event: BaseEventMessage[UserLoginPayload], logger: Logger
) -> None:
    """Handle user login events with strong typing."""

    # registry.get_module_instance("events").track_handler()

    # This handler gets a properly typed UserLoginMessage
    logger.info(f"HANDLE USER LOGIN. User logged in from {event}")


# Make sure this has a different channel name
@stufio_event_subscriber(UserLoginEvent)
async def another_handle_user_login(
    event: BaseEventMessage[UserLoginPayload], logger: Logger
) -> None:
    """Handle user login events with strong typing."""
    # This handler gets a properly typed UserLoginMessage
    logger.info(f"ANOTHER HANDLE USER LOGIN !!! User logged in from {event}")
    


@kafka_router.subscriber(
    f"{settings.events_KAFKA_TOPIC_PREFIX}.test",
    # group_id=settings.events_KAFKA_GROUP_ID,
    include_in_schema=False,
)
async def handle_test_event(event: str, logger: Logger) -> None:
    """Handle test events."""
    logger.error(f"0. TestTestTest: {event}")

    # Add entity_id which is required
    try:
        await UserLoginEvent.publish(
            entity_id="test-user-id",
            actor_type=ActorType.USER,
            actor_id="test_actor_id",
            payload=UserLoginPayload(
                user_id="123.test_user_id",
                ip_address="1.1.1.1",
                user_agent="test.user_agent",
                success=True  # Add required fields
            ),
            metrics={"test_metric": 123},
        )
        await UserCreatedEvent.publish(
            entity_id="test-user-id",
            actor_type=ActorType.USER,
            actor_id="test_actor_id",
            payload=UserCreatedPayload(
                after=UserPayload(
                    user_id="123.test_user_id",
                    ip_address="1.1.1.1",
                    user_agent="test.user_agent",
                    success=True  # Add required fields
                )
            ),
            metrics={"test_metric": 123},
        )
        logger.info("Successfully published test login event")
    except Exception as e:
        logger.error(f"Failed to publish test login event: {e}", exc_info=True)
