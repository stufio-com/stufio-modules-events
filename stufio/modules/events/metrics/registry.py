"""
Registry for metrics providers.

This module provides the core registry for metrics providers that allows
modules to register their own metrics providers without modifying the
events module code.
"""

import logging
import inspect
import sys
from typing import Dict, Any, Type, Optional, List

from .providers import BaseMetricsProvider

logger = logging.getLogger(__name__)

class MetricsProviderRegistry:
    """Registry for metrics providers.
    
    This singleton class manages all registered metrics providers and allows
    collecting metrics from them in a standardized way.
    """
    
    # Singleton instance
    _instance = None
    
    # Dictionary of registered provider classes
    _provider_classes: Dict[str, Type[BaseMetricsProvider]] = {}
    
    # Dictionary of instantiated provider objects
    _providers: Dict[str, BaseMetricsProvider] = {}
    
    # Flag to track if discovery has run
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
            provider_class: The provider class to register
        
        Raises:
            ValueError: If the provider doesn't have a valid provider_name or is already registered
        """
        if not provider_class.provider_name:
            raise ValueError(
                f"Provider class {provider_class.__name__} must have a provider_name class attribute"
            )
        
        provider_name = provider_class.provider_name
        
        if provider_name in self._provider_classes:
            # Skip if already registered with the same class
            if self._provider_classes[provider_name] == provider_class:
                return
            
            logger.warning(
                f"Provider '{provider_name}' is already registered with {self._provider_classes[provider_name].__name__}. "
                f"Overriding with {provider_class.__name__}"
            )
        
        self._provider_classes[provider_name] = provider_class
        logger.info(f"Registered metrics provider: {provider_name}")
        
        # If we already have an instance, replace it
        if provider_name in self._providers:
            del self._providers[provider_name]
    
    def get_provider(self, provider_name: str, **config) -> Optional[BaseMetricsProvider]:
        """Get or create a provider instance by name.
        
        Args:
            provider_name: Name of the provider to get
            config: Optional configuration for the provider if it needs to be created
            
        Returns:
            The provider instance, or None if no such provider exists
        """
        # Return existing instance if we have it
        if provider_name in self._providers:
            return self._providers[provider_name]
        
        # Create new instance if we have the class
        if provider_name in self._provider_classes:
            provider_class = self._provider_classes[provider_name]
            try:
                provider = provider_class(**config)
                self._providers[provider_name] = provider
                return provider
            except Exception as e:
                logger.error(f"Error instantiating provider {provider_name}: {e}")
                return None
        
        return None
    
    def get_all_providers(self) -> Dict[str, BaseMetricsProvider]:
        """Get all registered provider instances.
        
        Returns:
            Dictionary mapping provider names to provider instances
        """
        # Ensure discovery has run if it hasn't already
        if not self._discovery_complete:
            self.discover_providers()
            
        # Create instances for any registered classes that don't have instances yet
        for name, cls in self._provider_classes.items():
            if name not in self._providers:
                try:
                    self._providers[name] = cls()
                except Exception as e:
                    logger.error(f"Error instantiating provider {name}: {e}")
        
        return self._providers
    
    async def collect_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Collect metrics from all registered providers.
        
        Returns:
            Dictionary mapping provider names to their metrics
        """
        providers = self.get_all_providers()
        results = {}
        
        for name, provider in providers.items():
            try:
                results[name] = await provider.get_metrics()
            except Exception as e:
                logger.error(f"Error collecting metrics from provider {name}: {e}")
                results[name] = {"error": str(e)}
        
        return results
    
    async def reset_all_metrics(self) -> None:
        """Reset metrics for all providers."""
        providers = self.get_all_providers()
        
        for name, provider in providers.items():
            try:
                await provider.reset_metrics()
            except Exception as e:
                logger.error(f"Error resetting metrics for provider {name}: {e}")
    
    def discover_providers(self) -> None:
        """
        Discover metrics providers from all installed modules.
        
        This method searches for BaseMetricsProvider subclasses in:
        1. All installed stufio modules that match metrics/db patterns
        2. All application modules registered in the stufio module registry
        """
        if self._discovery_complete:
            return
            
        logger.info("Discovering metrics providers...")
        
        # Track number of providers found
        providers_found = 0
        
        # 1. First method: Check specific stufio packages that likely contain metrics providers
        relevant_packages = set()
        
        # Add stufio modules focused on db and metrics
        for name, module in list(sys.modules.items()):
            if name.startswith('stufio.') and ('metrics' in name or 'db' in name):
                relevant_packages.add(name)
        
        # Check these specific modules for metrics providers
        for name in relevant_packages:
            providers_found += self._scan_module_for_providers(name)
            
        # 2. Second method: Check all modules in the stufio module registry
        try:
            # Use core module registry to discover providers in application modules
            from stufio.core.module_registry import registry
            
            # Go through all registered modules
            for module_name, module in registry.modules.items():
                try:
                    # Look for metrics submodule
                    metrics_module = module.get_submodule('metrics')
                    if metrics_module:
                        module_path = f"{module.__module__}.metrics"
                        providers_found += self._scan_module_for_providers(module_path)
                    
                    # Also check if the module has metrics providers in its main package
                    module_path = module.__module__
                    if module_path in sys.modules:
                        providers_found += self._scan_module_for_providers(module_path)
                        
                    # Check if module.py contains metrics providers (common pattern)
                    module_impl = module.get_submodule('module')
                    if module_impl:
                        module_path = f"{module.__module__}.module"
                        providers_found += self._scan_module_for_providers(module_path)
                    
                except Exception as e:
                    logger.debug(f"Error scanning module {module_name} for metrics providers: {e}")
        except ImportError:
            logger.debug("Core module registry not available, skipping module discovery")
        except Exception as e:
            logger.error(f"Error during module registry scan: {e}")

        # 3. Import built-in providers to ensure they're registered
        try:
            # Import our built-in providers
            from ..metrics import db_providers
            providers_found += 1
        except ImportError:
            pass
            
        logger.info(f"Discovered {providers_found} metrics providers")
        self._discovery_complete = True
    
    def _scan_module_for_providers(self, module_name: str) -> int:
        """
        Scan a module for metrics providers.
        
        Args:
            module_name: Name of the module to scan
            
        Returns:
            Number of providers found
        """
        providers_found = 0
        try:
            module = sys.modules.get(module_name)
            if not module:
                return 0
                
            # Look for BaseMetricsProvider subclasses
            for attr_name in dir(module):
                try:
                    attr = getattr(module, attr_name)
                    if (inspect.isclass(attr) and 
                        issubclass(attr, BaseMetricsProvider) and 
                        attr is not BaseMetricsProvider and
                        attr.provider_name):
                        
                        # Register the provider
                        self.register_provider(attr)
                        providers_found += 1
                except (AttributeError, TypeError):
                    pass
        except Exception as e:
            logger.debug(f"Error scanning module {module_name}: {e}")
        
        return providers_found


# Singleton instance for global access
_registry = MetricsProviderRegistry()

def register_metrics_provider(provider_class: Type[BaseMetricsProvider]) -> Type[BaseMetricsProvider]:
    """Decorator to register a metrics provider class.
    
    Example:
        ```python
        @register_metrics_provider
        class OpenSearchMetricsProvider(BaseMetricsProvider):
            provider_name = "opensearch"
            
            async def get_metrics(self) -> Dict[str, Any]:
                # Implementation
                return {"queries": 10}
        ```
    
    Args:
        provider_class: The provider class to register
        
    Returns:
        The provider class (unchanged)
    """
    _registry.register_provider(provider_class)
    return provider_class


async def get_all_metrics() -> Dict[str, Dict[str, Any]]:
    """Collect all metrics from registered providers.
    
    Returns:
        Dictionary mapping provider names to their metrics
    """
    return await _registry.collect_all_metrics()


async def reset_all_metrics() -> None:
    """Reset metrics for all registered providers."""
    await _registry.reset_all_metrics()


def discover_metrics_providers() -> List[Type[BaseMetricsProvider]]:
    """
    Discover and register all metrics providers.
    
    Returns:
        List of discovered provider classes
    """
    _registry.discover_providers()
    return list(_registry._provider_classes.values())


# Get a specific provider instance by name
def get_metrics_provider(name: str, **config) -> Optional[BaseMetricsProvider]:
    """Get a metrics provider instance by name.
    
    Args:
        name: The name of the provider to get
        config: Optional configuration for the provider
        
    Returns:
        The provider instance, or None if no such provider exists
    """
    return _registry.get_provider(name, **config)