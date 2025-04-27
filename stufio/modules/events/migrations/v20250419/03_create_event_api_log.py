from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn

class CreateEventApiLogTable(ClickhouseMigrationScript):
    name = "create_event_api_log_table"
    description = "Create a dedicated table for API request logs"
    migration_type = "schema"
    order = 20

    async def run(self, db) -> None:

        db_name = get_database_from_dsn()

        await db.command(f"""
        CREATE TABLE IF NOT EXISTS `{db_name}`.`event_api_log` (
            id UUID DEFAULT generateUUIDv4(),
            event_id UUID,
            correlation_id UUID,
            timestamp DateTime64(3),
            
            path String,
            method String,
            status_code UInt16,
            duration_ms Int32,
            
            user_id String,
            ip_address String,
            user_agent String,
            is_authenticated UInt8,
            
            payload_size_bytes UInt32 DEFAULT 0,
            response_size_bytes UInt32 DEFAULT 0,
            
            headers Nullable(String) CODEC(ZSTD(3)),
            query_params Nullable(String) CODEC(ZSTD(3)),
            
            created_at DateTime DEFAULT now(),
            
            -- Add a converted DateTime field for TTL
            ttl_date DateTime DEFAULT toDateTime(timestamp)
        ) ENGINE = MergeTree()
        PRIMARY KEY (event_id)
        ORDER BY (event_id, timestamp, path) 
        PARTITION BY toYYYYMM(timestamp)
        TTL ttl_date + INTERVAL 1 MONTH;
        """)

        # Create a materialized view for path-based aggregation if needed
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
