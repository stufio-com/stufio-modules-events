"""
Handler response schemas.

This module defines the standard response types for event handlers.
"""

from typing import Any, Dict, List, Optional, Type, Union
from uuid import UUID
import asyncio
import logging
import time
from faststream.broker.message import StreamMessage
from pydantic import BaseModel, Field, model_serializer

from faststream.broker.response import Response as FastStreamResponse

from .base import ActorType, EventMetrics
from .event_definition import EventDefinition
from ..utils.context import TaskContext

logger = logging.getLogger(__name__)


class PublishEventInfo(BaseModel):
    """
    Information needed to publish a follow-up event from an event handler.
    
    This class enforces the use of predefined event types (EventDefinition)
    to ensure all events follow a consistent structure and are properly documented.
    """
    # Event Definition-based publishing
    event_type: Type[EventDefinition] = Field(
        ..., description="Event definition class to use for publishing"
    )
    entity_id: Optional[str] = Field(
        None, description="Entity ID for the event"
    )
    actor_type: Optional[Union[str, ActorType]] = Field(
        None, description="Actor type for the event"
    )
    actor_id: Optional[str] = Field(
        None, description="Actor ID for the event"
    )
    
    # Event data
    payload: Optional[Any] = Field(
        None, description="Event payload data"
    )
    correlation_id: Optional[Union[str, UUID]] = Field(
        None, description="Correlation ID to use for the event"
    )
    
    class Config:
        arbitrary_types_allowed = True


class HandlerResponse(FastStreamResponse):
    """
    Standardized response schema for event handlers that extends FastStream Response.
    
    This structure allows handlers to:
    1. Return a custom result
    2. Include metrics data
    3. Request follow-up events to be published
    
    By extending FastStreamResponse, HandlerResponse objects are already valid responses
    and won't be overwritten by serialization.
    """
    def __init__(
        self,
        result: Optional[Any] = None,
        metrics: Optional[Union[Dict[str, Any], EventMetrics]] = None,
        publish: Optional[List[PublishEventInfo]] = None,
        headers: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        event_id: Optional[str] = None,
        **kwargs: Any
    ):
        """
        Initialize a HandlerResponse with result, metrics, and publish events.
        
        Args:
            result: Handler's primary result/return value
            metrics: Metrics collected during handling
            publish: Events to publish as follow-up actions
            headers: Additional headers to include in the response
            correlation_id: Correlation ID for the response
        """
        # Initialize FastStreamResponse with the result as body
        super().__init__(body=result, headers=headers, correlation_id=correlation_id, **kwargs=kwargs)

        # Store additional properties specific to HandlerResponse
        self.handler_result = result
        self.handler_metrics = metrics or {}
        self.publish = publish or []

        # Store the time when this response was created (for metrics)
        self.response_time = time.time()

    # Add a serializer method to make this work with FastAPI's serialize_response
    @model_serializer
    def serialize_model(self):
        """Serialize the HandlerResponse to ensure FastAPI doesn't clear the body."""
        return self.handler_result

    def __bool__(self):
        """Make HandlerResponse truthy even with None result."""
        return True

    # Method to make the object compatible with FastAPI serialization
    def __getattr__(self, name):
        """Handle special attributes for FastAPI compatibility."""
        # For model_dump compatibility with Pydantic
        if name == "model_dump":
            return lambda: self.handler_result
        # For dict compatibility with FastAPI
        if name == "dict":
            return lambda: self.handler_result
        raise AttributeError(f"{self.__class__.__name__} has no attribute {name}")

    def _process_metrics(self, handler_name: Optional[str] = None) -> None:
        """Process metrics"""
        if not self.handler_metrics or not self.correlation_id:
            return

        try:
            from ..decorators.metrics import save_event_metrics

            # Extract metrics from handler_metrics
            metrics_data = {}
            if hasattr(self.handler_metrics, "dict"):
                metrics_data = self.handler_metrics.dict()
            elif hasattr(self.handler_metrics, "model_dump"):
                metrics_data = self.handler_metrics.model_dump()
            elif isinstance(self.handler_metrics, dict):
                metrics_data = self.handler_metrics

            # Get response handler name if available
            module_name = "events"

            # Get the current time for metrics
            current_time = time.time()
            started_at = getattr(self, "response_time", current_time - 0.001)
            
            save_event_metrics(
                event_id=self.event_id or 'unknown_event_id',  # Generate UUID if not available
                correlation_id=self.correlation_id,
                source_type="handler",
                consumer_name=handler_name or "unknown_handler",
                module_name=module_name,
                event_timestamp=self.response_time,
                started_at=started_at,
                completed_at=current_time,
                success=True,  # Assume success if we reached this point
                error_message=None,
                metrics=metrics_data
            )

        except Exception as e:
            logger.error(f"Error processing publish events: {e}", exc_info=True)
            
    def after_consume(self, msg: StreamMessage, handler_name: Optional[str] = None) -> None:
        """Override after_consume to process metrics and publish events."""
        # Call the parent method to ensure proper handling
        super().after_consume(msg)
        
        if not self.correlation_id:
            decoded_body = msg.decoded_body()
            if decoded_body and hasattr(decoded_body, "correlation_id"):
                # Use the correlation ID from the message if available
                self.correlation_id = str(msg.decoded_message.correlation_id)
            else:
                # Generate a new correlation ID if not provided
                self.correlation_id = str(TaskContext.get_correlation_id())
                
        if not self.event_id:
            # Use the event ID from the message if available
            self.event_id = str(msg.decoded_message.event_id)
        else:
            # Generate a new event ID if not provided
            self.event_id = 'unknown_event_id'
                
        # Process metrics
        self._process_metrics(handler_name)

        # Process publish events
        self._process_publish_events()

    def _process_publish_events(self):
        """Process events in the publish array."""
        if not self.publish:
            return

        try:
            # Import here to avoid circular imports
            from stufio.modules.events.helpers import publish_event

            # Create an async task to publish all events
            for event_info in self.publish:
                event_info.correlation_id = self.correlation_id
                asyncio.create_task(
                    self._publish_single_event(event_info, publish_event)
                )
        except Exception as e:
            logger.error(f"Error processing publish events: {e}")

    @staticmethod
    async def _publish_single_event(event_info, publish_func):
        """Publish a single event from the publish array."""
        try:
            # Convert correlation_id to string if it's a UUID
            correlation_id = event_info.correlation_id
            if isinstance(correlation_id, UUID):
                correlation_id = str(correlation_id)

            # Use the publish_event helper to publish the event
            # Metrics are always excluded from published events (only stored in ClickHouse)
            await publish_func(
                event_def=event_info.event_type,
                entity_id=event_info.entity_id,
                actor_type=event_info.actor_type,
                actor_id=event_info.actor_id,
                payload=event_info.payload,
                correlation_id=correlation_id
            )
        except Exception as e:
            logger.error(f"Error publishing event from HandlerResponse: {e}", exc_info=True)

    def add_metrics(self, new_metrics: Dict[str, Any]) -> 'HandlerResponse':
        """Add metrics to the HandlerResponse and update headers."""
        # Store metrics
        self.handler_metrics.update(new_metrics)

        return self


# Convenience aliases
Metrics = Dict[str, Any]
PublishEvent = PublishEventInfo
