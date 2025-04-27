"""
Base classes for metrics providers.

This module contains the base classes that all metrics providers must implement,
allowing for a standardized way of collecting metrics from different systems.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, ClassVar


class BaseMetricsProvider(ABC):
    """Base class for all metrics providers.
    
    A metrics provider is responsible for collecting metrics from a specific system
    like a database, external API, or other service. Providers are registered with
    the MetricsProviderRegistry and automatically included in event metrics.
    
    Example:
        ```python
        class OpenSearchMetricsProvider(BaseMetricsProvider):
            provider_name = "opensearch"
            
            async def get_metrics(self) -> Dict[str, Any]:
                # Implementation to collect OpenSearch metrics
                return {
                    "queries": 10,
                    "time_ms": 45,
                    "slow_queries": 1
                }
        ```
    """
    
    # Class variable that must be overridden by subclasses
    provider_name: ClassVar[str] = None
    
    # Optional configuration parameters
    config: Dict[str, Any]
    
    def __init__(self, **config):
        """Initialize the metrics provider with optional configuration."""
        self.config = config
        
    @abstractmethod
    async def get_metrics(self) -> Dict[str, Any]:
        """Collect and return metrics for this provider.
        
        Returns:
            Dict[str, Any]: A dictionary containing the metrics collected.
                The structure is provider-specific, but should generally include:
                - Basic counters (queries, operations, calls, etc.)
                - Timing information (time_ms, avg_time_ms, etc.)
                - Error information if relevant
        
        Example return value:
            {
                "queries": 5,
                "time_ms": 120,
                "slow_queries": 0,
                "query_types": {"search": 3, "index": 2}
            }
        """
        pass
    
    async def reset_metrics(self) -> None:
        """Reset any metrics counters for this provider.
        
        This method is called at the start of a new request or event processing
        to ensure metrics are specific to the current operation.
        
        The default implementation does nothing; override if your provider
        maintains stateful counters.
        """
        pass