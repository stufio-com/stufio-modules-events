"""
Handler response schemas.

This module defines the standard response types for event handlers.
"""

from typing import Any, Dict, List, Optional, Type, Union
from uuid import UUID
import asyncio
import logging
from pydantic import BaseModel, Field, model_serializer

from faststream.broker.response import Response as FastStreamResponse

from stufio.modules.events.schemas.base import ActorType, EventMetrics
from stufio.modules.events.schemas.event_definition import EventDefinition

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
    metrics: Optional[Dict[str, Any]] = Field(
        None, description="Metrics to include with the event"
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
        # Process metrics for headers
        all_headers = headers or {}
        if metrics:
            # Convert metrics to a dict if it's not already
            if hasattr(metrics, "dict"):
                metrics_dict = metrics.dict()
            elif hasattr(metrics, "model_dump"):
                metrics_dict = metrics.model_dump()
            elif isinstance(metrics, dict):
                metrics_dict = metrics
            else:
                metrics_dict = {}
            
            # Merge metrics with headers
            all_headers.update(metrics_dict)
        
        # Initialize FastStreamResponse with the result as body
        super().__init__(body=result, headers=all_headers, correlation_id=correlation_id)
        
        # Store additional properties specific to HandlerResponse
        self.handler_result = result
        self.handler_metrics = metrics
        self.publish = publish or []
        
        # Process publish events if provided
        if self.publish:
            self._process_publish_events()

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
    
    def _process_publish_events(self):
        """Process events in the publish array."""
        if not self.publish:
            return
            
        try:
            # Import here to avoid circular imports
            from stufio.modules.events.helpers import publish_event
            
            # Create an async task to publish all events
            for event_info in self.publish:
                asyncio.create_task(self._publish_single_event(event_info, publish_event))
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
            await publish_func(
                event_def=event_info.event_type,
                entity_id=event_info.entity_id,
                actor_type=event_info.actor_type,
                actor_id=event_info.actor_id,
                payload=event_info.payload,
                correlation_id=correlation_id,
                metrics=event_info.metrics,
            )
        except Exception as e:
            logger.error(f"Error publishing event from HandlerResponse: {e}", exc_info=True)
    
    def add_metrics(self, new_metrics: Dict[str, Any]) -> 'HandlerResponse':
        """Add metrics to the HandlerResponse and update headers."""
        # Store metrics
        self.handler_metrics = new_metrics
        
        # Update headers with metrics
        self.headers.update(new_metrics)
        
        return self


# Convenience aliases
Metrics = Dict[str, Any]
PublishEvent = PublishEventInfo