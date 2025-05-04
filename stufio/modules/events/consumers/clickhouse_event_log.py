import asyncio
import traceback
from typing import Any, Optional
from faststream.kafka.fastapi import KafkaMessage, Logger

from ..crud import crud_error_log
from ..utils.context import TaskContext
from ..consumers.asyncapi import stufio_event_subscriber, stufio_subscriber
from ..consumers import get_kafka_broker, get_kafka_router
from ..events import UserCreatedEvent, UserLoginEvent, UserLoginPayload
from ..schemas import ActorType, EventLogCreate, ErrorLogCreate
import json

from ..schemas.payloads import UserCreatedPayload, UserPayload
from ..crud import crud_event_log
from ..schemas.messages import BaseEventMessage
from stufio.core.config import get_settings

from ..events import SystemErrorEvent
from ..schemas.payloads import SystemErrorPayload

settings = get_settings()

event_logs_topics = [settings.events_KAFKA_TOPIC_PREFIX]


kafka_router = get_kafka_router()
kafka_broker = get_kafka_broker()


# Generic event handler with automatic casting to the correct event message type
@stufio_subscriber(
    # *event_logs_topics,
    pattern=settings.events_KAFKA_TOPIC_PREFIX.replace(".", "\.") + "(\.(.*)\.(.*))?$",
    group_id=f"{settings.events_KAFKA_GROUP_ID}.*.*:events_log",
    include_in_schema=False,
    # Replace consumer_config with individual parameters
    max_poll_interval_ms=300000,
    session_timeout_ms=60000,
    heartbeat_interval_ms=20000,
    auto_offset_reset="earliest",
)
async def log_event_to_clickhouse(msg: KafkaMessage, logger: Logger) -> None:
    """
    Consume events from Kafka and log them to Clickhouse with improved reliability.
    """
    # Initialize default values
    topic = "unknown"
    event_name = "unknown"
    entity_type = "unknown"
    action = "unknown"

    try:
        # Extract topic
        if hasattr(msg, 'raw_message') and hasattr(msg.raw_message, 'topic'):
            topic = msg.raw_message.topic

        from ..helpers import extract_headers_safely
        headers = extract_headers_safely(msg)
        
        logger.warning(f"Headers: {headers}")

        # Try to get correlation_id from headers first
        correlation_id = headers.get('correlation_id')
        if not correlation_id:
            correlation_id = TaskContext.get_correlation_id()
        else:
            # If we found a correlation_id in headers, set it in TaskContext
            TaskContext.set_correlation_id(correlation_id)

        # 2. Extract entity_type and action - with header priority
        entity_type, action = extract_event_metadata(msg)
        event_name = f"{entity_type}.{action}" if entity_type != "unknown" else topic

        # if entity_type == "api" and action == "request":
        #     return None  # Skip API request events

        # 3. Get message body
        body = msg._decoded_body if hasattr(msg, '_decoded_body') else msg.body
        kafka_broker.logger.info(f"Processing event: {event_name} from topic: {topic}")

        # 4. Handle case where body is not a dict
        if not isinstance(body, dict):
            if body:
                kafka_broker.logger.warning(f"Received non-dict message: {body[:100]}...")
                # Try to parse if it's a string that could be JSON
                if isinstance(body, str):
                    try:
                        body = json.loads(body)
                    except json.JSONDecodeError:
                        kafka_broker.logger.error("Could not parse string as JSON")
                        return None
            else:
                kafka_broker.logger.warning("Received empty message body")
                return None

        # Log body type and structure for debugging
        kafka_broker.logger.info(f"Body type: {type(body)}, Body keys: {list(body.keys()) if isinstance(body, dict) else 'Not a dict'}")
        if isinstance(body, dict) and 'event_id' in body:
            kafka_broker.logger.info(f"Event ID in body: {body['event_id']}")
        
        # 5. Convert to BaseEventMessage and extract further metadata
        try:
            event = BaseEventMessage.model_validate(body)
            kafka_broker.logger.info(f"Parsed event {event_name} with ID {event.event_id}")
        except Exception as e:
            kafka_broker.logger.error(f"Failed to validate message as BaseEventMessage: {e}")
            # Log the body structure for debugging
            kafka_broker.logger.error(f"Message body structure: {json.dumps(body, default=str)[:500]}...")
            raise

        # 6. Extract entity_type/action from event if still unknown
        if entity_type == "unknown" and event.entity and hasattr(event.entity, 'type'):
            entity_type = event.entity.type
        if action == "unknown" and hasattr(event, 'action'):
            action = event.action

        # 7. Prepare event log data
        actor_type = event.actor.type if event.actor else body.get("actor_type", "")
        actor_id = event.actor.id if event.actor else body.get("actor_id", "")

        # 8. Convert payload and metrics to JSON
        payload_json = serialize_field(body.get("payload", {}))
        metrics_json = serialize_field(body.get("metrics", {}))

        # 9. Create the EventLogCreate object with correlation_id
        event_log = EventLogCreate(
            event_id=event.event_id,
            entity_type=entity_type,
            action=action,
            entity_id=event.entity.id if event.entity else "",
            payload=payload_json,
            actor_type=actor_type,
            actor_id=actor_id,
            correlation_id=correlation_id,  # Use the extracted/generated correlation_id
            timestamp=event.timestamp,
            metrics=metrics_json,
        )

        # 10. Save to ClickHouse with retry capability
        await save_event_with_retry(event_log, event_name)

    except Exception as e:
        kafka_broker.logger.error(f"Error processing event from {topic}: {str(e)}", exc_info=True)
        await save_error_event(msg, topic, e)

    return None

# Helper functions for cleaner code
def extract_event_metadata(msg: KafkaMessage) -> tuple[str, str]:
    """Extract entity_type and action from message headers or topic."""
    entity_type = "unknown"
    action = "unknown"
    
    # Try headers first
    if hasattr(msg, "raw_message") and hasattr(msg.raw_message, "headers"):
        headers = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                  v.decode('utf-8') if isinstance(v, bytes) else v 
                  for k, v in msg.raw_message.headers or []}
        
        if "entity_type" in headers:
            entity_type = headers["entity_type"]
        if "action" in headers:
            action = headers["action"]
    
    # Fall back to topic parsing
    if entity_type == "unknown" or action == "unknown":
        topic = msg.raw_message.topic if hasattr(msg, 'raw_message') and hasattr(msg.raw_message, 'topic') else ""
        topic_parts = topic.split('.')
        if len(topic_parts) >= 3:
            if entity_type == "unknown":
                entity_type = topic_parts[-2]
            if action == "unknown":
                action = topic_parts[-1]
    
    return entity_type, action

def serialize_field(field_data: Any) -> Optional[str]:
    """Safely serialize data to JSON string."""
    if not field_data:
        return None
        
    try:
        return json.dumps(field_data)
    except Exception as e:
        kafka_broker.logger.warning(f"Failed to serialize data: {e}")
        # Try using a default serializer that handles more types
        try:
            return json.dumps(field_data, default=str)
        except Exception:
            return json.dumps({"error": "Could not serialize data"})

async def save_event_with_retry(event_log: EventLogCreate, event_name: str, max_retries: int = 3) -> bool:
    """Save event to ClickHouse with retry logic."""
    retries = 0
    last_error = None
    
    while retries < max_retries:
        try:
            await crud_event_log.create(event_log)
            kafka_broker.logger.info(f"Event {event_name} logged to Clickhouse with ID {event_log.event_id}")
            return True
        except Exception as e:
            last_error = e
            retries += 1
            kafka_broker.logger.warning(f"Retry {retries}/{max_retries} - Failed to save event: {e}")
            await asyncio.sleep(0.5 * retries)  # Exponential backoff
    
    kafka_broker.logger.error(f"Failed to save event after {max_retries} attempts: {last_error}")
    return False

async def save_error_event(msg: KafkaMessage, topic: str, exception: Exception) -> None:
    """Save detailed error information about a failed event processing."""
    correlation_id = TaskContext.get_correlation_id()
    try:
        # Extract any available information from the failed message
        body = msg._decoded_body if hasattr(msg, '_decoded_body') else msg.body
        body_str = str(body)[:1000] if body else "Empty message"

        # Extract entity_type and action
        entity_type, action = extract_event_metadata(msg)

        # Get error stack trace
        error_stack = traceback.format_exc()

        # Create error log
        error_log = ErrorLogCreate(
            correlation_id=correlation_id,
            error_type="event_processing_error",
            severity="error",
            source="kafka_consumer",
            entity_type=entity_type if entity_type != "unknown" else None,
            entity_id=None,  # We may not have this
            action=action if action != "unknown" else None,
            error_message=str(exception),
            error_stack=error_stack,
            request_data=json.dumps({"topic": topic, "message_body": body_str})
        )

        # Save to ClickHouse
        await crud_error_log.create(error_log)
        kafka_broker.logger.info(f"Error event logged to Clickhouse with ID {error_log.error_id}")

        # Publish system error event
        await SystemErrorEvent.publish(
            entity_id="kafka_consumer_error",
            actor_type=ActorType.SYSTEM,
            actor_id="clickhouse_event_log",
            payload=SystemErrorPayload(
                error_type="event_processing_error",
                error_message=str(exception),
                severity="error",
                stacktrace=error_stack,
            ),
            correlation_id=str(correlation_id),
        )

    except Exception as e:
        kafka_broker.logger.error(f"Failed to save error event: {e}", exc_info=True)
        # Try to publish system error event even if saving to ClickHouse failed
        try:
            await SystemErrorEvent.publish(
                entity_id="error_logging_failure",
                actor_type=ActorType.SYSTEM,
                actor_id="clickhouse_event_log",
                payload=SystemErrorPayload(
                    error_type="error_logging_failure",
                    error_message=str(e),
                    severity="critical",
                    stacktrace=traceback.format_exc(),
                ),
                correlation_id=str(correlation_id),
            )
        except Exception as e2:
            kafka_broker.logger.error(f"Failed to publish system error event: {e2}", exc_info=True)

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
