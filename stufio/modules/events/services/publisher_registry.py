"""Registry for publisher channels in AsyncAPI documentation."""
import logging
from typing import Dict, Any, Type, Optional
from faststream.asyncapi.schema import Channel, ChannelBinding, Message, Operation
from faststream.asyncapi.schema.bindings import kafka
from faststream.asyncapi.schema.message import CorrelationId
from faststream.asyncapi.utils import resolve_payloads
from ..schemas.base import BaseEventPayload
from ..schemas.event_definition import EventDefinition
from ..schemas.messages import BaseEventMessage  # Import BaseEventMessage
from stufio.core.config import get_settings

settings = get_settings()

logger = logging.getLogger(__name__)

# Store publisher channel registry
publisher_registry: Dict[str, Dict[str, Any]] = {}

def register_publisher_channel(
    event_def: Type[EventDefinition],
    payload_class: Optional[Type] = None,  # Add explicit payload_class parameter
    description: Optional[str] = None,
    include_in_schema: bool = True,
) -> None:
    """Register a publisher channel for AsyncAPI documentation."""
    # Skip if not meant to be included
    if not include_in_schema:
        return

    # Extract event attributes
    event_attrs = getattr(event_def, "_event_attrs", {})
    entity_type = event_attrs.get('entity_type')
    action = event_attrs.get('action')
    topic = event_attrs.get('topic')

    # Get payload class using multiple methods for robustness
    if payload_class is None:
        try:
            payload_class = event_def.get_payload_class()
        except Exception as e:
            logger.debug(f"Error extracting payload class for {event_def.__name__}: {e}")

    # Skip if we still can't determine the payload class
    if not payload_class:
        logger.warning(f"Cannot determine payload class for {event_def.__name__}")
        payload_class = BaseEventPayload

    # Create channel name - use custom topic or generate from entity_type/action
    # if topic:
    #     # channel_name = topic.replace('.', '/')
    #     channel_name = topic
    # else:
    channel_name = f"{settings.events_ASYNCAPI_PREFIX}.{entity_type}.{action}"

    # Store in registry
    publisher_registry[channel_name] = {
        "event_def": event_def,
        "payload_class": payload_class,
        "description": description or event_attrs.get('description') or f"Publisher channel for {event_def.__name__}",
        "topic": topic or f"{entity_type}.{action}"
    }

    logger.info(f"Registered publisher channel {channel_name} for {event_def.__name__}")

def get_publisher_channels() -> Dict[str, Channel]:
    """Get all registered publisher channels for AsyncAPI schema."""
    channels = {}
    message_components = {}  # NEW: Store message components
    top_level_schemas = {}

    def transform_refs(obj):
        """Recursively transform references from #/$defs/ to #/components/schemas/"""
        if isinstance(obj, dict):
            if "$ref" in obj and isinstance(obj["$ref"], str) and obj["$ref"].startswith("#/$defs/"):
                # Replace the reference path
                obj["$ref"] = obj["$ref"].replace("#/$defs/", "#/components/schemas/")

            # Process all dictionary values recursively
            return {k: transform_refs(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            # Process all list items recursively
            return [transform_refs(item) for item in obj]
        else:
            # Return primitive values as is
            return obj

    for channel_name, info in publisher_registry.items():
        try:
            payload_class = info["payload_class"]
            topic = info["topic"]
            description = info["description"]

            # Create a proper payload dict with class name as key
            payloads = {}

            try:
                # Generate schema safely for the basic payload class
                if hasattr(payload_class, "model_json_schema"):
                    # Pydantic v2
                    schema = payload_class.model_json_schema()
                    # Transform $defs to components.schemas for AsyncAPI compatibility
                    if isinstance(schema, dict) and "$defs" in schema:
                        # Save all definitions to be merged at the top level
                        defs = schema.pop("$defs")
                        for def_name, def_schema in defs.items():
                            top_level_schemas[def_name] = def_schema

                    # Transform all references
                    schema = transform_refs(schema)

                    # Extract any nested components.schemas into top_level_schemas
                    if isinstance(schema, dict) and "components" in schema:
                        if "schemas" in schema["components"]:
                            for name, s in schema["components"]["schemas"].items():
                                top_level_schemas[name] = s
                            schema.pop("components")

                elif hasattr(payload_class, "schema"):
                    # Pydantic v1
                    schema = payload_class.schema()
                    # Transform references for Pydantic v1 if needed
                    schema = transform_refs(schema)

                    # Extract any nested definitions into top_level_schemas
                    if isinstance(schema, dict) and "definitions" in schema:
                        for name, s in schema["definitions"].items():
                            top_level_schemas[name] = s
                        schema.pop("definitions")
                else:
                    schema = {"type": "object", "properties": {}}

                # Store the raw payload schema
                payloads[payload_class.__name__] = schema
                top_level_schemas[payload_class.__name__] = schema

                # Add the wrapper BaseEventMessage class name
                wrapper_name = f"BaseEventMessage[{payload_class.__name__}]"

                # Create wrapper by using the schema objects
                # The key change is to create a reference to the actual schema
                # in the components/schemas section with the correct naming
                wrapper_schema = {
                    "title": wrapper_name,
                    "type": "object", 
                    "properties": {
                        "event_id": {
                            "description": "Unique event identifier",
                            "title": "Event Id",
                            "type": "string",
                            "format": "uuid4"
                        },
                        "correlation_id": {
                            "anyOf": [
                                {"type": "string", "format": "uuid4"},
                                {"type": "null"}
                            ],
                            "default": None,
                            "description": "Correlation ID for tracking related events",
                            "title": "Correlation Id"
                        },
                        "timestamp": {
                            "description": "Event timestamp",
                            "title": "Timestamp",
                            "type": "string",
                            "format": "date-time"
                        },
                        "entity": {
                            "allOf": [
                                {"$ref": "#/components/schemas/Entity"}
                            ],
                            "description": "Entity involved in the event"
                        },
                        "action": {
                            "description": "Action performed on the entity",
                            "title": "Action",
                            "type": "string"
                        },
                        "actor": {
                            "allOf": [
                                {"$ref": "#/components/schemas/Actor"}
                            ],
                            "description": "Actor who performed the action"
                        },
                        "payload": {
                            "allOf": [
                                {"$ref": f"#/components/schemas/{payload_class.__name__}"}
                            ],
                            "description": "Event payload data"
                        },
                        "metrics": {
                            "anyOf": [
                                {"$ref": "#/components/schemas/EventMetrics"},
                                {"type": "null"}
                            ],
                            "default": None,
                            "description": "Event performance metrics"
                        }
                    },
                    "required": ["timestamp", "entity", "action", "actor", "payload"]
                }

                # Add the wrapper schema to the collection
                payloads[wrapper_name] = wrapper_schema
                top_level_schemas[wrapper_name] = wrapper_schema

                # Register standard schemas if they don't exist
                register_standard_schemas(top_level_schemas)

                # Create a unique message component name
                message_component_name = f"{payload_class.__name__}Event" if not "event_def" in info else f"{info['event_def'].__name__}"

                # Create the message component - similar to how subscribers define them
                message_components[message_component_name] = {
                    "title": message_component_name,
                    "correlationId": {"location": "$message.header#/correlation_id"},
                    "payload": {
                        "$ref": f"#/components/schemas/{wrapper_name}"
                    },
                    "headers": {
                        "entity_type": {
                            "description": "Entity type of the message",
                            "location": "$message.header#/entity_type"
                        },
                        "action": {
                            "description": "Action performed on the entity",
                            "location": "$message.header#/action"
                        }
                    }
                }

                # Create AsyncAPI channel that references the message component
                channels[channel_name] = Channel(
                    description=description,
                    publish=Operation(
                        message={"$ref": f"#/components/messages/{message_component_name}"}
                    ),
                    bindings=ChannelBinding(
                        kafka=kafka.ChannelBinding(topic=topic),
                    ),
                )
                logger.debug(f"Added publisher channel: {channel_name} with message component: {message_component_name}")

            except Exception as e:
                logger.warning(
                    f"❌ Failed to generate schema for {payload_class.__name__}: {e}"
                )

        except Exception as e:
            logger.error(
                f"❌ Failed to create channel for {channel_name}: {e}", exc_info=True
            )

    # Add message components to the AsyncAPI doc
    from ..consumers.asyncapi_patches import (
        top_level_schemas_registry,
        get_patched_app_schema,
    )

    # Add schemas to components.schemas
    for name, schema in top_level_schemas.items():
        top_level_schemas_registry[name] = schema

    setattr(get_patched_app_schema, "message_components", message_components)

    return channels

def register_standard_schemas(top_level_schemas: Dict[str, Any]) -> None:
    """Register standard schemas used by BaseEventMessage."""
    # Register Entity schema if needed
    if "Entity" not in top_level_schemas:
        top_level_schemas["Entity"] = {
            "description": "Entity involved in an event.",
            "properties": {
                "type": {
                    "description": "Type of entity (e.g., user, product, order)",
                    "title": "Type",
                    "type": "string"
                },
                "id": {
                    "description": "Entity ID",
                    "title": "Id",
                    "type": "string"
                }
            },
            "required": ["type", "id"],
            "title": "Entity",
            "type": "object"
        }

    # Register Actor schema if needed
    if "Actor" not in top_level_schemas:
        top_level_schemas["Actor"] = {
            "description": "Actor who performed an action.",
            "properties": {
                "type": {
                    "allOf": [{"$ref": "#/components/schemas/ActorType"}],
                    "description": "Type of actor performing the action"
                },
                "id": {
                    "description": "Actor ID",
                    "title": "Id",
                    "type": "string"
                }
            },
            "required": ["type", "id"],
            "title": "Actor",
            "type": "object"
        }

    # Register ActorType schema if needed
    if "ActorType" not in top_level_schemas:
        top_level_schemas["ActorType"] = {
            "description": "Type of actor performing an action.",
            "enum": ["user", "admin", "system", "service"],
            "title": "ActorType",
            "type": "string"
        }

    # Register EventMetrics schema if needed
    if "EventMetrics" not in top_level_schemas:
        top_level_schemas["EventMetrics"] = {
            "description": "Performance metrics for event processing.",
            "properties": {
                "processing_time_ms": {
                    "anyOf": [{"type": "integer"}, {"type": "null"}],
                    "default": None,
                    "description": "Processing time in milliseconds",
                    "title": "Processing Time Ms"
                },
                "db_time_ms": {
                    "anyOf": [{"type": "integer"}, {"type": "null"}],
                    "default": None,
                    "description": "Database operation time in milliseconds",
                    "title": "Db Time Ms"
                },
                "api_time_ms": {
                    "anyOf": [{"type": "integer"}, {"type": "null"}],
                    "default": None,
                    "description": "External API time in milliseconds",
                    "title": "Api Time Ms"
                },
                "queue_time_ms": {
                    "anyOf": [{"type": "integer"}, {"type": "null"}],
                    "default": None,
                    "description": "Time spent in queue",
                    "title": "Queue Time Ms"
                },
                "custom_metrics": {
                    "anyOf": [{"type": "object"}, {"type": "null"}],
                    "default": None,
                    "description": "Custom service-specific metrics",
                    "title": "Custom Metrics"
                }
            },
            "title": "EventMetrics",
            "type": "object"
        }
