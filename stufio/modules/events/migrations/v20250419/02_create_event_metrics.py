from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn

class CreateEventMetricsTable(ClickhouseMigrationScript):
    name = "create_event_metrics_table"
    description = "Create unified event metrics table"
    migration_type = "schema"
    order = 10

    async def run(self, db) -> None:

        db_name = get_database_from_dsn()

        await db.command(
            f"""
        CREATE TABLE IF NOT EXISTS `{db_name}`.`event_metrics` (
            id UUID DEFAULT generateUUIDv4(),
            event_id UUID,
            correlation_id UUID,
            tenant String,
            source_type String,
            consumer_name String,
            module_name String,
            timestamp DateTime64(3),
            
            started_at DateTime64(3),
            completed_at DateTime64(3),
            duration_ms Int32,
            latency_ms Int32,
            
            success UInt8 DEFAULT 1,
            error_message Nullable(String),
            
            mongodb_queries UInt16 DEFAULT 0,
            mongodb_time_ms Int32 DEFAULT 0,
            mongodb_slow_queries UInt16 DEFAULT 0,
            
            clickhouse_queries UInt16 DEFAULT 0,
            clickhouse_time_ms Int32 DEFAULT 0,
            clickhouse_slow_queries UInt16 DEFAULT 0,
            
            redis_operations UInt16 DEFAULT 0,
            redis_time_ms Int32 DEFAULT 0,
            redis_slow_operations UInt16 DEFAULT 0,
            
            custom_metrics Nullable(String) CODEC(ZSTD(3)),
            
            created_at DateTime DEFAULT now(),
            
            -- Add a converted DateTime field for TTL
            ttl_date DateTime DEFAULT toDateTime(started_at)
        ) ENGINE = MergeTree()
        PRIMARY KEY (event_id)
        ORDER BY (event_id, source_type, consumer_name, correlation_id, started_at)
        PARTITION BY toYYYYMM(started_at)
        TTL ttl_date + INTERVAL 3 MONTH;
        """
        )

        # First create the target table for the materialized view
        await db.command(
            f"""
        CREATE TABLE IF NOT EXISTS `{db_name}`.`event_metrics_daily` (
            date Date,
            tenant String,
            source_type String,
            consumer_name String,
            module_name String,
            event_count UInt64,
            avg_duration_ms Float64,
            max_duration_ms Int32,
            avg_latency_ms Float64,
            max_latency_ms Int32,
            error_count UInt64,
            total_mongodb_queries UInt64,
            total_mongodb_time_ms Int64,
            total_clickhouse_queries UInt64,
            total_clickhouse_time_ms Int64,
            total_redis_operations UInt64,
            total_redis_time_ms Int64
        ) ENGINE = SummingMergeTree()
        PRIMARY KEY (date, tenant, source_type, consumer_name, module_name)
        ORDER BY (date, tenant, source_type, consumer_name, module_name)
        PARTITION BY toYYYYMM(date)
        TTL date + INTERVAL 12 MONTH;
        """
        )

        # Then create materialized view for aggregation
        await db.command(
            f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS `{db_name}`.`event_metrics_daily_mv`
        TO event_metrics_daily
        AS SELECT 
            toDate(started_at) AS date,
            tenant,
            source_type,
            consumer_name,
            module_name,
            count() AS event_count,
            avg(duration_ms) AS avg_duration_ms, 
            max(duration_ms) AS max_duration_ms,
            avg(latency_ms) AS avg_latency_ms,
            max(latency_ms) AS max_latency_ms,
            sum(if(success = 0, 1, 0)) AS error_count,
            sum(mongodb_queries) AS total_mongodb_queries,
            sum(mongodb_time_ms) AS total_mongodb_time_ms,
            sum(clickhouse_queries) AS total_clickhouse_queries,
            sum(clickhouse_time_ms) AS total_clickhouse_time_ms,
            sum(redis_operations) AS total_redis_operations,
            sum(redis_time_ms) AS total_redis_time_ms
        FROM event_metrics
        GROUP BY date, tenant, source_type, consumer_name, module_name;
        """
        )
