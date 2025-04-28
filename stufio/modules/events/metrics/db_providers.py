"""

Database metrics integration with core framework.

This module provides compatibility with stufio.db.metrics and re-exports
the necessary functions for metrics integration.
"""

# Re-export the unified metrics system from the core framework
from stufio.db.metrics import (
    get_all_metrics,
    reset_all_metrics,
    get_mongodb_metrics,
    get_clickhouse_metrics,
    get_redis_metrics,
)

# For backward compatibility
async def reset_db_metrics():
    """Reset all DB metrics providers."""
    await reset_all_metrics()

__all__ = [
    "get_all_metrics",
    "reset_all_metrics",
    "reset_db_metrics",
    "get_mongodb_metrics",
    "get_clickhouse_metrics",
    "get_redis_metrics",
]
