from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn

class CreateEventApiLogPathDailyTable(ClickhouseMigrationScript):
    name = "create_event_api_log_path_daily_table"
    description = "Create the destination table for the event_api_log_path_mv materialized view"
    migration_type = "schema"
    order = 25  # Higher than the previous migrations

    async def run(self, db) -> None:
        db_name = get_database_from_dsn()

        # Create the destination table that the materialized view writes to
        await db.command(
            f"""
        CREATE TABLE IF NOT EXISTS `{db_name}`.`event_api_log_path_daily` (
            date Date,
            path String,
            method String,
            request_count UInt64,
            avg_duration_ms Float64,
            max_duration_ms Int32,
            success_count UInt64,
            client_error_count UInt64,
            server_error_count UInt64
        ) ENGINE = SummingMergeTree()
        PRIMARY KEY (date, path, method)
        ORDER BY (date, path, method)
        PARTITION BY toYYYYMM(date)
        TTL date + INTERVAL 12 MONTH;
        """
        )

        # Recreate the materialized view to ensure it points to the destination table
        await db.command(
            f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS `{db_name}`.`event_api_log_path_mv`
        TO event_api_log_path_daily
        AS SELECT 
            toDate(timestamp) AS date,
            path,
            method,
            count() AS request_count,
            avg(duration_ms) AS avg_duration_ms,
            max(duration_ms) AS max_duration_ms,
            countIf(status_code >= 200 AND status_code < 300) AS success_count,
            countIf(status_code >= 400 AND status_code < 500) AS client_error_count,
            countIf(status_code >= 500) AS server_error_count
        FROM event_api_log
        GROUP BY date, path, method;
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
