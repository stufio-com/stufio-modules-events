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
        """Create event metrics record with memory optimization."""
        try:
            # Add logging to understand data size and content
            data_dict = obj_in.model_dump()
            data_size = len(str(data_dict))
            
            # SAFETY CHECK: If data is too large, truncate or skip
            if data_size > 500000:  # 500KB limit
                logger.warning(f"⚠️ Event metrics data too large ({data_size} bytes), skipping to prevent memory issues")
                return {"status": "skipped", "reason": "data_too_large", "size": data_size}
            
            logger.info(f"🔍 Creating event metrics - Data size: {data_size} bytes")
            logger.debug(f"📊 Event metrics data keys: {list(data_dict.keys())}")
            
            # Log potentially large fields
            large_fields = []
            for key, value in data_dict.items():
                if isinstance(value, str) and len(value) > 1000:
                    large_fields.append(f"{key}: {len(value)} chars")
                elif isinstance(value, (list, dict)) and len(str(value)) > 1000:
                    large_fields.append(f"{key}: {len(str(value))} chars")
            
            if large_fields:
                logger.warning(f"⚠️ Large fields detected: {large_fields}")
            
            # For large data, use batch insert with smaller chunks
            result = await super().create(obj_in)
            logger.info(f"✅ Event metrics created successfully - Result type: {type(result)}")
            return result.model_dump()
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Failed to create event metrics: {e}", exc_info=True)
            logger.error(f"🔍 Error type: {type(e).__name__}")
            logger.error(f"📊 Input data size: {len(str(obj_in.model_dump()))} bytes")
            
            # Check if it's a memory limit error
            if "MEMORY_LIMIT_EXCEEDED" in error_msg or "Memory limit" in error_msg:
                logger.warning("🔄 Memory limit exceeded, attempting retry with optimized settings")
                try:
                    # Retry with smaller batch or alternative approach
                    return await self._create_with_retry(obj_in)
                except Exception as retry_error:
                    logger.error(f"❌ Retry also failed: {retry_error}")
                    return {"error": f"Memory limit exceeded: {retry_error}"}
            
            # If we can't log the metrics, at least we still have it in the app logs
            return {"error": str(e)}

    async def _create_with_retry(self, obj_in: EventMetricsCreate) -> Dict[str, Any]:
        """Retry creation with memory optimization techniques."""
        try:
            logger.info("🔄 Starting retry with memory optimization")
            
            # Convert to dict and ensure we're not sending huge data
            data_dict = obj_in.model_dump()
            original_size = len(str(data_dict))
            logger.info(f"📊 Original data size: {original_size} bytes")
            
            # Truncate large text fields if they exist
            truncated_fields = []
            for field_name in ['custom_metrics', 'error_message']:
                if field_name in data_dict and isinstance(data_dict[field_name], str):
                    original_length = len(data_dict[field_name])
                    if original_length > 10000:  # 10KB limit
                        data_dict[field_name] = data_dict[field_name][:10000] + "... [truncated]"
                        truncated_fields.append(f"{field_name}: {original_length} -> 10000")
                        logger.warning(f"📦 Truncated {field_name} field: {original_length} -> 10000 chars")
            
            if truncated_fields:
                logger.warning(f"🔧 Truncated fields: {truncated_fields}")
            
            # Also truncate any other unexpectedly large string fields
            for key, value in data_dict.items():
                if isinstance(value, str) and len(value) > 5000:  # 5KB limit for other fields
                    original_length = len(value)
                    data_dict[key] = value[:5000] + "... [truncated]"
                    logger.warning(f"🔧 Truncated large field {key}: {original_length} -> 5000 chars")
            
            new_size = len(str(data_dict))
            logger.info(f"📊 Optimized data size: {new_size} bytes (reduced by {original_size - new_size})")
            
            # Create a new object with truncated data and use the base class method
            truncated_obj = EventMetricsCreate(**data_dict)
            logger.info("🔄 Attempting create with optimized data")
            result = await super().create(truncated_obj)
            
            logger.info("✅ Successfully created event metrics with retry")
            return result.model_dump()
            
        except Exception as e:
            logger.error(f"❌ Retry creation failed: {e}")
            logger.error(f"🔍 Retry error type: {type(e).__name__}")
            raise

    async def get_metrics_by_event_id(self, event_id: str) -> Optional[EventMetricsResponse]:
        """Get metrics for specific event."""
        try:
            result = await self.get_by_field("event_id", event_id)
            if result:
                return EventMetricsResponse(**result.model_dump())
            return None
        except Exception as e:
            logger.error(f"Error retrieving metrics for event {event_id}: {e}", exc_info=True)
            return None

    async def get_metrics_summary(self, 
                                days: int = 7, 
                                module_name: Optional[str] = None,
                                source_type: Optional[str] = None) -> Dict[str, Any]:
        """Get metrics summary for recent events."""
        try:
            logger.info(f"🔍 Getting metrics summary for {days} days, module: {module_name}, source: {source_type}")
            client = await self.client
            since_date = datetime.utcnow() - timedelta(days=days)
            logger.info(f"📅 Query date range: since {since_date}")

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

            parameters: Dict[str, Any] = {"since_date": since_date}

            if module_name:
                query += " AND module_name = :module_name"
                parameters["module_name"] = module_name

            if source_type:
                query += " AND source_type = :source_type"
                parameters["source_type"] = source_type

            query += " GROUP BY source_type"
            
            logger.info(f"📊 Executing query with {len(parameters)} parameters")
            logger.debug(f"🔍 Query: {query}")
            logger.debug(f"🔍 Parameters: {parameters}")

            result = await client.query(query, parameters=parameters)
            logger.info(f"✅ Query executed successfully")

            summary = {}
            row_count = 0
            for row in result.named_results():
                row_count += 1
                summary[row["source_type"]] = {k: v for k, v in row.items() if k != "source_type"}
            
            logger.info(f"📊 Processed {row_count} result rows, summary size: {len(str(summary))} bytes")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Failed to get metrics summary: {e}", exc_info=True)
            logger.error(f"🔍 Summary error type: {type(e).__name__}")
            return {}


# Create a global instance of the CRUD class
crud_event_metrics = CRUDEventMetrics(EventMetricsModel)
