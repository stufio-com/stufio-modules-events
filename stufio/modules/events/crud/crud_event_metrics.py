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
        """Create event metrics record with aggressive memory optimization."""
        try:
            # Add logging to understand data size and content
            data_dict = obj_in.model_dump()
            data_size = len(str(data_dict))
            
            # ULTRA AGGRESSIVE SAFETY CHECK: Even lower threshold due to ClickHouse memory amplification
            if data_size > 10000:  # 10KB limit (reduced from 50KB)
                logger.warning(f"⚠️ Event metrics data too large ({data_size} bytes), skipping to prevent memory issues")
                return {"status": "skipped", "reason": "data_too_large", "size": data_size}
            
            logger.info(f"🔍 Creating event metrics - Data size: {data_size} bytes")
            logger.debug(f"📊 Event metrics data keys: {list(data_dict.keys())}")
            
            # Immediately optimize data before any processing
            optimized_data = await self._optimize_data_for_insertion(data_dict)
            optimized_size = len(str(optimized_data))
            
            if optimized_size != data_size:
                logger.info(f"🔧 Data optimized: {data_size} -> {optimized_size} bytes")
                optimized_obj = EventMetricsCreate(**optimized_data)
            else:
                optimized_obj = obj_in
            
            # Use direct ClickHouse insertion with minimal memory footprint
            result = await self._direct_insert_minimal(optimized_obj)
            logger.info(f"✅ Event metrics created successfully via direct insert")
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Failed to create event metrics: {e}", exc_info=True)
            logger.error(f"🔍 Error type: {type(e).__name__}")
            logger.error(f"📊 Input data size: {len(str(obj_in.model_dump()))} bytes")
            
            # Check if it's a memory limit error
            if "MEMORY_LIMIT_EXCEEDED" in error_msg or "Memory limit" in error_msg:
                logger.warning("🔄 Memory limit exceeded, attempting emergency fallback")
                try:
                    # Use emergency fallback that stores minimal essential data only
                    return await self._emergency_minimal_insert(obj_in)
                except Exception as retry_error:
                    logger.error(f"❌ Emergency fallback also failed: {retry_error}")
                    # Return success with minimal data to prevent application crashes
                    return {
                        "status": "degraded", 
                        "reason": "memory_limit_exceeded",
                        "event_id": getattr(obj_in, 'event_id', 'unknown'),
                        "timestamp": getattr(obj_in, 'timestamp', datetime.utcnow()).isoformat()
                    }
            
            # Return degraded success for other errors to prevent cascading failures
            return {
                "status": "error", 
                "reason": str(e)[:200],  # Limit error message length
                "event_id": getattr(obj_in, 'event_id', 'unknown')
            }

    async def _optimize_data_for_insertion(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Aggressively optimize data to minimize ClickHouse memory usage."""
        optimized = data_dict.copy()
        
        # Define ultra-aggressive size limits for different field types
        field_limits = {
            'error_message': 200,       # 200B max for error messages (reduced from 1KB)
            'custom_metrics': 300,      # 300B max for custom metrics (reduced from 2KB)
            'headers': 100,             # 100B max for headers (reduced from 500B)
            'query_params': 100,        # 100B max for query params (reduced from 500B)
            'response_body': 200,       # 200B max for response body (reduced from 1KB)
            'request_body': 200,        # 200B max for request body (reduced from 1KB)
        }
        
        # Truncate large string fields
        for field_name, limit in field_limits.items():
            if field_name in optimized and isinstance(optimized[field_name], str):
                if len(optimized[field_name]) > limit:
                    original_length = len(optimized[field_name])
                    optimized[field_name] = optimized[field_name][:limit-6] + "...[T]"
                    logger.debug(f"🔧 Truncated {field_name}: {original_length} -> {limit} chars")
        
        # Convert complex objects to simple strings with ultra-tight size limits
        for key, value in optimized.items():
            if isinstance(value, (dict, list)) and key not in ['timestamp', 'started_at', 'completed_at']:
                str_value = str(value)
                if len(str_value) > 300:  # 300B limit for complex objects (reduced from 1KB)
                    # Try to JSON serialize with truncation
                    try:
                        import json
                        json_str = json.dumps(value)
                        if len(json_str) > 300:
                            optimized[key] = json_str[:297] + "..."
                        else:
                            optimized[key] = json_str
                    except:
                        optimized[key] = str_value[:297] + "..."
                        
        # Remove None values to reduce payload
        optimized = {k: v for k, v in optimized.items() if v is not None}
        
        return optimized

    async def _direct_insert_minimal(self, obj_in: EventMetricsCreate) -> Dict[str, Any]:
        """Direct minimal insertion to ClickHouse with memory-conscious approach."""
        try:
            # Get client and prepare minimal data
            client = await self.client
            
            # Convert to model and get only essential fields for insert
            model_instance = EventMetricsModel(**obj_in.model_dump())
            
            # Use dict_for_insert method if available, otherwise model_dump
            if hasattr(model_instance, 'dict_for_insert'):
                data = model_instance.dict_for_insert()
            else:
                data = model_instance.model_dump(exclude_unset=True)
            
            # Remove any potentially large fields that aren't essential
            # Only include fields that exist in the actual table
            essential_fields = {
                'event_id', 'tenant', 'module_name', 'source_type', 'timestamp', 
                'started_at', 'completed_at', 'duration_ms', 'success', 'latency_ms',
                'mongodb_queries', 'mongodb_time_ms', 'clickhouse_queries', 'clickhouse_time_ms',
                'redis_operations', 'redis_time_ms'
            }
            
            # Keep only essential fields to minimize memory usage
            minimal_data = {k: v for k, v in data.items() if k in essential_fields and v is not None}
            
            # Ensure required fields are present
            if 'tenant' not in minimal_data:
                minimal_data['tenant'] = getattr(obj_in, 'tenant', 'unknown')
            if 'started_at' not in minimal_data:
                minimal_data['started_at'] = getattr(obj_in, 'started_at', datetime.utcnow())
            if 'completed_at' not in minimal_data:
                minimal_data['completed_at'] = getattr(obj_in, 'completed_at', datetime.utcnow())
            
            # Single row insert with minimal data
            await client.insert(
                self.model.get_table_name(),
                [list(minimal_data.values())],
                column_names=list(minimal_data.keys()),
            )
            
            return {
                "status": "success",
                "event_id": minimal_data.get('event_id', 'unknown'),
                "insertion_type": "minimal"
            }
            
        except Exception as e:
            logger.error(f"❌ Direct minimal insert failed: {e}")
            raise

    async def _emergency_minimal_insert(self, obj_in: EventMetricsCreate) -> Dict[str, Any]:
        """Emergency fallback with absolute minimal data insertion."""
        try:
            logger.warning("🚨 Using emergency minimal insertion")
            client = await self.client
            
            # Only store the most critical fields that definitely exist in the table
            emergency_data = {
                'event_id': getattr(obj_in, 'event_id', f"emergency_{int(datetime.utcnow().timestamp())}"),
                'tenant': getattr(obj_in, 'tenant', 'unknown')[:50],  # Limit to 50 chars
                'source_type': getattr(obj_in, 'source_type', 'unknown')[:50],
                'timestamp': getattr(obj_in, 'timestamp', datetime.utcnow()),
                'started_at': getattr(obj_in, 'started_at', datetime.utcnow()),
                'completed_at': getattr(obj_in, 'completed_at', datetime.utcnow()),
                'success': getattr(obj_in, 'success', False),
                'duration_ms': min(getattr(obj_in, 'duration_ms', 0), 999999),  # Cap large numbers
            }
            
            # Add module_name only if it exists
            module_name = getattr(obj_in, 'module_name', None)
            if module_name:
                emergency_data['module_name'] = str(module_name)[:50]
            
            # Single minimal insert
            await client.insert(
                self.model.get_table_name(),
                [list(emergency_data.values())],
                column_names=list(emergency_data.keys()),
            )
            
            logger.info("✅ Emergency minimal data saved successfully")
            return {
                "status": "emergency_success",
                "event_id": emergency_data['event_id'],
                "insertion_type": "emergency_minimal"
            }
            
        except Exception as e:
            logger.error(f"❌ Emergency minimal insert failed: {e}")
            # Even if this fails, return success to prevent cascading failures
            return {
                "status": "failed_gracefully", 
                "reason": "emergency_insert_failed",
                "event_id": getattr(obj_in, 'event_id', 'unknown')
            }

    async def _create_with_retry(self, obj_in: EventMetricsCreate) -> Dict[str, Any]:
        """Legacy retry method - now redirects to emergency minimal insert."""
        logger.info("� Legacy retry method called, redirecting to emergency minimal insert")
        return await self._emergency_minimal_insert(obj_in)
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
