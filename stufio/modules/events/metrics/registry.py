"""
Registry for metrics providers.

This module manages the registration and retrieval of metrics providers,
which collect performance metrics from various systems.
"""

import importlib
import logging
import pkgutil
import sys
from typing import Any, Dict, List, Optional, Set, Type

from stufio.core.config import get_settings
from .providers import BaseMetricsProvider

# Import unified metrics system from framework
from stufio.db.metrics import reset_all_metrics as framework_reset_all_metrics
from stufio.db.metrics import get_all_metrics as framework_get_all_metrics

settings = get_settings()
logger = logging.getLogger(__name__)

# Registry for additional metrics providers beyond the core DB providers
class MetricsProviderRegistry:
    """Registry for metrics providers.
    
    This registry handles the discovery, registration, and management of custom 
    metrics providers beyond the core database providers.
    """
    
    _instance = None
    _provider_classes = {}
    _providers = {}
    _discovery_complete = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MetricsProviderRegistry, cls).__new__(cls)
            # Initialize instance attributes
            cls._instance._provider_classes = {}
            cls._instance._providers = {}
            cls._instance._discovery_complete = False
        return cls._instance

    def register_provider(self, provider_class: Type[BaseMetricsProvider]) -> None:
        """Register a metrics provider class.
        
        Args:
            provider_class: The provider class to register. Must have a provider_name attribute.
        
        Raises:
            ValueError: If the provider doesn't have a name.
        """
        provider_name = getattr(provider_class, "provider_name", None)
        if not provider_name:
            raise ValueError(f"Metrics provider {provider_class.__name__} has no provider_name attribute")
            
        self._provider_classes[provider_name] = provider_class
        logger.info(f"Registered metrics provider: {provider_name}")
        
        # If we already have an instance, replace it
        if provider_name in self._providers:
            del self._providers[provider_name]
        
    def get_provider(self, name: str) -> Optional[BaseMetricsProvider]:
        """Get a metrics provider by name.
        
        Args:
            name: Provider name to retrieve
            
        Returns:
            The provider instance, or None if not found
        """
        # Ensure all providers are discovered
        if not self._discovery_complete:
            self.discover_providers()
        
        # If we already have an instance, return it
        if name in self._providers:
            return self._providers[name]
            
        # If the class is registered, create an instance
        if name in self._provider_classes:
            try:
                provider = self._provider_classes[name]()
                self._providers[name] = provider
                return provider
            except Exception as e:
                logger.error(f"Error instantiating metrics provider {name}: {e}")
        
        return None
        
    async def get_metrics_from_provider(self, name: str) -> Dict[str, Any]:
        """Get metrics from a specific provider.
        
        Args:
            name: Provider name to get metrics from
            
        Returns:
            Dict with metrics, or empty dict if provider not found
        """
        provider = self.get_provider(name)
        if provider is None:
            return {}
        
        try:
            return await provider.get_metrics()
        except Exception as e:
            logger.error(f"Error getting metrics from provider {name}: {e}")
            return {"error": str(e)}
        
    async def reset_provider_metrics(self, name: str) -> None:
        """Reset metrics for a specific provider.
        
        Args:
            name: Provider name to reset metrics for
        """
        provider = self.get_provider(name)
        if provider is None:
            return
        
        try:
            await provider.reset_metrics()
        except Exception as e:
            logger.error(f"Error resetting metrics for provider {name}: {e}")
        
    async def reset_custom_providers_metrics(self) -> None:
        """Reset metrics for all custom providers."""
        # Ensure all providers are discovered
        if not self._discovery_complete:
            self.discover_providers()
            
        # Reset metrics for all registered providers
        for name in self._provider_classes:
            await self.reset_provider_metrics(name)
            
    def discover_providers(self) -> None:
        """Discover metrics providers in relevant modules.
        
        Searches for providers in:
        1. stufio.modules.events.metrics
        2. stufio.modules.* (other module metrics)
        3. Application modules
        """
        discovered = 0
        
        # Look in current module first
        discovered += self._scan_module_for_providers("stufio.modules.events.metrics")
        
        # Check modules namespace
        try:
            import stufio.modules
            for _, module_name, is_pkg in pkgutil.iter_modules(stufio.modules.__path__, "stufio.modules."):
                if is_pkg and module_name != "stufio.modules.events":
                    # Look for metrics module
                    metrics_module = f"{module_name}.metrics"
                    try:
                        discovered += self._scan_module_for_providers(metrics_module)
                    except ImportError:
                        # No metrics module in this package
                        pass
        except (ImportError, AttributeError):
            pass
            
        # TODO: Add support for application module discovery
                
        logger.debug(f"Discovered {discovered} metrics providers")
        self._discovery_complete = True
            
    def _scan_module_for_providers(self, module_name: str) -> int:
        """Scan a module for metrics providers.
        
        Args:
            module_name: Full module name to scan
            
        Returns:
            Number of providers found
        """
        discovered = 0
        try:
            module = importlib.import_module(module_name)
            for name in dir(module):
                obj = getattr(module, name)
                if (isinstance(obj, type) and 
                    issubclass(obj, BaseMetricsProvider) and 
                    obj is not BaseMetricsProvider and
                    hasattr(obj, "provider_name")):
                    
                    provider_name = getattr(obj, "provider_name", None)
                    if provider_name and provider_name not in self._provider_classes:
                        self._provider_classes[provider_name] = obj
                        discovered += 1
                    
        except ImportError:
            pass
            
        return discovered
        

# Global registry instance
_registry = MetricsProviderRegistry()


def register_metrics_provider(provider_class: Type[BaseMetricsProvider]) -> Type[BaseMetricsProvider]:
    """Decorator to register a metrics provider.
    
    Example:
        ```python
        @register_metrics_provider
        class OpenSearchMetricsProvider(BaseMetricsProvider):
            provider_name = "opensearch"
            
            async def get_metrics(self) -> Dict[str, Any]:
                # Implementation
                return {"queries": 10}
        ```
    """
    _registry.register_provider(provider_class)
    return provider_class


async def get_all_metrics() -> Dict[str, Dict[str, Any]]:
    """Get metrics from all registered providers.
    
    Returns:
        Dict mapping provider names to their metrics
    """
    # Get framework metrics (DB providers)
    metrics = await framework_get_all_metrics()
    
    # Add custom metrics from registry
    # Ensure providers are discovered
    if not _registry._discovery_complete:
        _registry.discover_providers()
        
    # Get metrics from all custom providers
    for name, provider_class in _registry._provider_classes.items():
        if name not in metrics:  # Don't override core providers
            try:
                provider = _registry.get_provider(name)
                if provider:
                    metrics[name] = await provider.get_metrics()
            except Exception as e:
                logger.error(f"Error getting metrics from provider {name}: {e}")
                metrics[name] = {"error": str(e)}
                
    return metrics


async def reset_all_metrics() -> None:
    """Reset all metrics providers (DB and custom)."""
    # Reset framework metrics (DB providers)
    await framework_reset_all_metrics()
    
    # Reset custom metrics
    await _registry.reset_custom_providers_metrics()


__all__ = ["register_metrics_provider", "get_all_metrics", "reset_all_metrics", "MetricsProviderRegistry"]