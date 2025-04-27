"""
Built-in metrics providers for common database systems.

This module contains default providers for MongoDB, ClickHouse, and Redis
that integrate with the existing metrics system.
"""

from typing import Dict, Any, ClassVar
import logging
import contextvars
from datetime import datetime

from .providers import BaseMetricsProvider
from .registry import register_metrics_provider

logger = logging.getLogger(__name__)

# Thread-local storage for request-specific metrics
class DBMetricsStorage:
    """Thread/task-local storage for metrics isolation between requests."""
    
    def __init__(self):
        # Create contextvars for each DB provider
        self.mongodb_context = contextvars.ContextVar('mongodb_metrics', default={})
        self.clickhouse_context = contextvars.ContextVar('clickhouse_metrics', default={})
        self.redis_context = contextvars.ContextVar('redis_metrics', default={})
    
    def get_mongodb_metrics(self) -> Dict:
        """Get thread-local MongoDB metrics."""
        return self.mongodb_context.get()
    
    def set_mongodb_metrics(self, metrics: Dict) -> None:
        """Set thread-local MongoDB metrics."""
        self.mongodb_context.set(metrics)
    
    def get_clickhouse_metrics(self) -> Dict:
        """Get thread-local ClickHouse metrics."""
        return self.clickhouse_context.get()
    
    def set_clickhouse_metrics(self, metrics: Dict) -> None:
        """Set thread-local ClickHouse metrics."""
        self.clickhouse_context.set(metrics)
    
    def get_redis_metrics(self) -> Dict:
        """Get thread-local Redis metrics."""
        return self.redis_context.get()
    
    def set_redis_metrics(self, metrics: Dict) -> None:
        """Set thread-local Redis metrics."""
        self.redis_context.set(metrics)


# Global metrics storage instance
db_metrics_storage = DBMetricsStorage()


# Try to import existing metrics utilities if available
try:
    from stufio.db.metrics import get_request_metrics as get_legacy_request_metrics
    from stufio.db.metrics import reset_request_metrics as reset_legacy_metrics
    from stufio.db.metrics import (
        get_mongodb_metrics,
        get_clickhouse_metrics,
        get_redis_metrics,
    )

    LEGACY_METRICS_AVAILABLE = True
except ImportError:
    LEGACY_METRICS_AVAILABLE = False


@register_metrics_provider
class MongoDBMetricsProvider(BaseMetricsProvider):
    """Provider for MongoDB metrics."""

    provider_name: ClassVar[str] = "mongodb"

    def __init__(self, **config):
        super().__init__(**config)
        self.reset_state()

    def reset_state(self):
        """Reset metrics for current request context."""
        db_metrics_storage.set_mongodb_metrics({
            "queries": 0,
            "time_ms": 0,
            "slow_queries": 0,
            "operation_types": {},
            "collection_stats": {},
            "timestamp": datetime.utcnow().timestamp()
        })

    async def get_metrics(self) -> Dict[str, Any]:
        """Collect MongoDB metrics."""
        # First check legacy metrics system
        if LEGACY_METRICS_AVAILABLE:
            try:
                # Try to get metrics from legacy system
                if hasattr(get_legacy_request_metrics, "__call__"):
                    legacy_metrics = await get_legacy_request_metrics()
                    if "mongo" in legacy_metrics and legacy_metrics["mongo"]:
                        # Convert legacy metrics to new format
                        mongo_metrics = legacy_metrics.get("mongo", {})
                        return {
                            "queries": mongo_metrics.get("queries", 0),
                            "time_ms": mongo_metrics.get("time_ms", 0),
                            "slow_queries": mongo_metrics.get("slow_queries", 0),
                            "operation_types": mongo_metrics.get("operation_types", {}),
                            "collection_stats": mongo_metrics.get("collection_stats", {})
                        }

                # Alternatively try direct MongoDB metrics function
                if hasattr(get_mongodb_metrics, "__call__"):
                    return await get_mongodb_metrics()

            except Exception as e:
                logger.debug(f"Error getting MongoDB metrics from legacy system: {e}")

        # Fallback to context-var based metrics
        return db_metrics_storage.get_mongodb_metrics()

    async def reset_metrics(self) -> None:
        """Reset MongoDB metrics."""
        # Reset local state
        self.reset_state()

        # Also try to reset legacy metrics if available
        if LEGACY_METRICS_AVAILABLE:
            try:
                await reset_legacy_metrics()
            except Exception as e:
                logger.debug(f"Error resetting legacy MongoDB metrics: {e}")


@register_metrics_provider
class ClickHouseMetricsProvider(BaseMetricsProvider):
    """Provider for ClickHouse metrics."""
    
    provider_name: ClassVar[str] = "clickhouse"
    
    def __init__(self, **config):
        super().__init__(**config)
        self.reset_state()
    
    def reset_state(self):
        """Reset metrics for current request context."""
        db_metrics_storage.set_clickhouse_metrics({
            "queries": 0,
            "time_ms": 0,
            "slow_queries": 0,
            "query_types": {},
            "timestamp": datetime.utcnow().timestamp()
        })
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Collect ClickHouse metrics."""
        # First check legacy metrics system
        if LEGACY_METRICS_AVAILABLE:
            try:
                # Try to get metrics from legacy system
                if hasattr(get_legacy_request_metrics, "__call__"):
                    legacy_metrics = await get_legacy_request_metrics()
                    if "clickhouse" in legacy_metrics and legacy_metrics["clickhouse"]:
                        # Convert legacy metrics to new format
                        clickhouse_metrics = legacy_metrics.get("clickhouse", {})
                        return {
                            "queries": clickhouse_metrics.get("queries", 0),
                            "time_ms": clickhouse_metrics.get("time_ms", 0),
                            "slow_queries": clickhouse_metrics.get("slow_queries", 0),
                            "query_types": clickhouse_metrics.get("query_types", {})
                        }
                
                # Alternatively try direct ClickHouse metrics function
                if hasattr(get_clickhouse_metrics, "__call__"):
                    return await get_clickhouse_metrics()
                    
            except Exception as e:
                logger.debug(f"Error getting ClickHouse metrics from legacy system: {e}")
        
        # Fallback to context-var based metrics
        return db_metrics_storage.get_clickhouse_metrics()
    
    async def reset_metrics(self) -> None:
        """Reset ClickHouse metrics."""
        # Reset local state
        self.reset_state()
        
        # Also try to reset legacy metrics if available
        if LEGACY_METRICS_AVAILABLE:
            try:
                await reset_legacy_metrics()
            except Exception as e:
                logger.debug(f"Error resetting legacy ClickHouse metrics: {e}")


@register_metrics_provider
class RedisMetricsProvider(BaseMetricsProvider):
    """Provider for Redis metrics."""
    
    provider_name: ClassVar[str] = "redis"
    
    def __init__(self, **config):
        super().__init__(**config)
        self.reset_state()
    
    def reset_state(self):
        """Reset metrics for current request context."""
        db_metrics_storage.set_redis_metrics({
            "operations": 0,
            "time_ms": 0,
            "slow_operations": 0,
            "command_types": {},
            "timestamp": datetime.utcnow().timestamp()
        })
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Collect Redis metrics."""
        # First check legacy metrics system
        if LEGACY_METRICS_AVAILABLE:
            try:
                # Try to get metrics from legacy system
                if hasattr(get_legacy_request_metrics, "__call__"):
                    legacy_metrics = await get_legacy_request_metrics()
                    if "redis" in legacy_metrics and legacy_metrics["redis"]:
                        # Convert legacy metrics to new format
                        redis_metrics = legacy_metrics.get("redis", {})
                        return {
                            "operations": redis_metrics.get("operations", 0),
                            "time_ms": redis_metrics.get("time_ms", 0),
                            "slow_operations": redis_metrics.get("slow_operations", 0),
                            "command_types": redis_metrics.get("command_types", {})
                        }
                
                # Alternatively try direct Redis metrics function
                if hasattr(get_redis_metrics, "__call__"):
                    return await get_redis_metrics()
                    
            except Exception as e:
                logger.debug(f"Error getting Redis metrics from legacy system: {e}")
        
        # Fallback to context-var based metrics
        return db_metrics_storage.get_redis_metrics()
    
    async def reset_metrics(self) -> None:
        """Reset Redis metrics."""
        # Reset local state
        self.reset_state()
        
        # Also try to reset legacy metrics if available
        if LEGACY_METRICS_AVAILABLE:
            try:
                await reset_legacy_metrics()
            except Exception as e:
                logger.debug(f"Error resetting legacy Redis metrics: {e}")


# Create module-level convenience functions for direct access from other modules
async def get_mongodb_metrics() -> Dict[str, Any]:
    """Get MongoDB metrics from the provider."""
    provider = MongoDBMetricsProvider()
    return await provider.get_metrics()

async def get_clickhouse_metrics() -> Dict[str, Any]:
    """Get ClickHouse metrics from the provider."""
    provider = ClickHouseMetricsProvider()
    return await provider.get_metrics()

async def get_redis_metrics() -> Dict[str, Any]:
    """Get Redis metrics from the provider."""
    provider = RedisMetricsProvider()
    return await provider.get_metrics()

async def reset_db_metrics() -> None:
    """Reset all DB metrics providers."""
    mongodb = MongoDBMetricsProvider()
    clickhouse = ClickHouseMetricsProvider()
    redis = RedisMetricsProvider()
    
    await mongodb.reset_metrics()
    await clickhouse.reset_metrics()
    await redis.reset_metrics()
