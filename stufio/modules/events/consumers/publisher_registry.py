"""Registry for publisher channels in AsyncAPI documentation."""
import logging
from typing import Dict, Any, Type, Optional
from faststream.asyncapi.schema import Channel, ChannelBinding, Message, Operation
from faststream.asyncapi.schema.bindings import kafka
from faststream.asyncapi.schema.message import CorrelationId
from faststream.asyncapi.utils import resolve_payloads
from ..schemas.event_definition import EventDefinition

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
            # Method 1: Check if defined in event attributes
            if event_attrs.get('payload_class'):
                payload_class = event_attrs.get('payload_class')
                logger.debug(f"Found payload class from event_attrs: {payload_class.__name__}")
            
            # Method 2: Try generic type parameters - check __orig_bases__
            elif hasattr(event_def, "__orig_bases__") and event_def.__orig_bases__:
                for base in event_def.__orig_bases__:
                    if hasattr(base, "__args__") and base.__args__:
                        payload_class = base.__args__[0]
                        logger.debug(f"Found payload class from __orig_bases__: {payload_class.__name__}")
                        break
            
            # Method 3: Access through __class_getitem__ parameter storage
            elif hasattr(event_def, "__parameters__") and event_def.__parameters__:
                payload_class = event_def.__parameters__[0] 
                logger.debug(f"Found payload class from __parameters__: {payload_class.__name__}")
                
        except Exception as e:
            logger.debug(f"Error extracting payload class for {event_def.__name__}: {e}")
    
    # Skip if we still can't determine the payload class
    if not payload_class:
        logger.warning(f"Cannot determine payload class for {event_def.__name__}")
        # Create a fallback empty payload class to avoid errors
        from pydantic import BaseModel
        class EmptyPayload(BaseModel):
            pass
        payload_class = EmptyPayload
    
    # Create channel name - use custom topic or generate from entity_type/action
    if topic:
        # channel_name = topic.replace('.', '/')
        channel_name = topic
    else:
        channel_name = f"{entity_type}.{action}"
    
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
    # Dictionary to store top-level component schemas
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
                # Generate schema safely
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

                # Store in payload dictionary
                payloads[payload_class.__name__] = schema

            except Exception as e:
                logger.warning(f"Failed to generate schema for {payload_class.__name__}: {e}")
                # Use fallback schema
                payloads[payload_class.__name__] = {"type": "object", "properties": {}}

            # Convert payloads dict to list of tuples format expected by resolve_payloads
            payloads_list = [(schema, class_name) for class_name, schema in payloads.items()]

            # Create AsyncAPI channel with proper schema resolution
            channels[channel_name] = Channel(
                description=description,
                publish=Operation(
                    message=Message(
                        title=f"{channel_name}:Message",
                        payload=resolve_payloads(payloads_list),
                        correlationId=CorrelationId(
                            location="$message.header#/correlation_id"
                        ),
                    ),
                ),
                bindings=ChannelBinding(
                    kafka=kafka.ChannelBinding(topic=topic),
                ),
            )
            logger.debug(f"Added publisher channel: {channel_name}")
        except Exception as e:
            logger.error(f"Failed to create channel for {channel_name}: {e}", exc_info=True)

    # Add our collected top-level schemas to the AsyncAPI doc
    from stufio.modules.events.consumers.asyncapi import top_level_schemas_registry
    for name, schema in top_level_schemas.items():
        top_level_schemas_registry[name] = schema

    return channels
