import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from stufio.crud.clickhouse_base import CRUDClickhouse
from ..models.event_metrics import EventMetricsModel
from ..schemas.event_metrics import EventMetricsCreate, EventMetricsResponse, EventMetricsUpdate

logger = logging.getLogger(__name__)


class CRUDEventMetrics(
    CRUDClickhouse[EventMetricsModel, EventMetricsCreate, EventMetricsUpdate]
):
    """CRUD operations for event metrics in ClickHouse."""

    async def create_metrics(self, obj_in: EventMetricsCreate) -> Dict[str, Any]:
        """Create event metrics record."""
        try:
            result = await super().create(obj_in)
            return result.model_dump()
        except Exception as e:
            logger.error(f"❌ Failed to create event metrics: {e}", exc_info=True)
            # If we can't log the metrics, at least we still have it in the app logs
            return {"error": str(e)}

    async def get_metrics_by_event_id(self, event_id: str) -> Optional[EventMetricsResponse]:
        """Get metrics for specific event."""
        try:
            return await self.get_by_field("event_id", event_id)
        except Exception as e:
            logger.error(f"Error retrieving metrics for event {event_id}: {e}", exc_info=True)
            return None

    async def get_metrics_summary(self, 
                                days: int = 7, 
                                module_name: Optional[str] = None,
                                source_type: Optional[str] = None) -> Dict[str, Any]:
        """Get metrics summary for recent events."""
        try:
            client = await self.client
            since_date = datetime.utcnow() - timedelta(days=days)

            # Build the query with optional filters
            query = f"""
            SELECT 
                source_type,
                AVG(duration_ms) as avg_duration_ms,
                AVG(latency_ms) as avg_latency_ms,
                COUNT(*) as total_events,
                COUNT(CASE WHEN success = 1 THEN 1 END) as successful_events,
                COUNT(CASE WHEN success = 0 THEN 1 END) as failed_events,
                AVG(mongodb_queries) as avg_mongodb_queries,
                AVG(mongodb_time_ms) as avg_mongodb_time_ms,
                AVG(clickhouse_queries) as avg_clickhouse_queries,
                AVG(clickhouse_time_ms) as avg_clickhouse_time_ms,
                AVG(redis_operations) as avg_redis_operations,
                AVG(redis_time_ms) as avg_redis_time_ms
            FROM {self.model.get_table_name()}
            WHERE timestamp >= :since_date
            """

            parameters = {"since_date": since_date}

            if module_name:
                query += " AND module_name = :module_name"
                parameters["module_name"] = module_name

            if source_type:
                query += " AND source_type = :source_type"
                parameters["source_type"] = source_type

            query += " GROUP BY source_type"

            result = await client.query(query, parameters=parameters)

            summary = {}
            for row in result.named_results():
                summary[row["source_type"]] = {k: v for k, v in row.items() if k != "source_type"}

            return summary
        except Exception as e:
            logger.error(f"❌ Failed to get metrics summary: {e}", exc_info=True)
            return {}


# Create a global instance of the CRUD class
crud_event_metrics = CRUDEventMetrics(EventMetricsModel)
