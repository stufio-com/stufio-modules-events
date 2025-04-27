"""
Metrics collection and registration system for event metrics.

This module provides a registry for metric providers that can be extended
by other modules to collect and report metrics from various systems.
"""

from .registry import MetricsProviderRegistry, register_metrics_provider, get_all_metrics
from .providers import BaseMetricsProvider

__all__ = [
    "MetricsProviderRegistry",
    "register_metrics_provider",
    "get_all_metrics",
    "BaseMetricsProvider",
]