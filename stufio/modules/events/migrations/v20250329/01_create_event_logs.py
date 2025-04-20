"""
Migration to create event_logs table in Clickhouse
"""
from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn

class CreateEventLogsTable(ClickhouseMigrationScript):
    name = "create_event_logs_table"
    description = "Create Clickhouse tables for event logs and daily aggregations"
    migration_type = "schema"
    order = 10

    async def run(self, db):

        db_name = get_database_from_dsn()

        # Create event_logs table
        await db.command(
            f"""
        CREATE TABLE IF NOT EXISTS `{db_name}`.`event_logs` (
            id UUID DEFAULT generateUUIDv4(),
            event_id UUID NOT NULL,
            correlation_id UUID NULL,
            timestamp DateTime NOT NULL,
            entity_type String NOT NULL,
            entity_id String NOT NULL,
            action String NOT NULL,
            actor_type String NOT NULL,
            actor_id String NOT NULL,
            payload String NULL CODEC(ZSTD(3)),
            metrics String NULL CODEC(ZSTD(3)),
            processed UInt8 DEFAULT 0,
            processing_attempts UInt16 DEFAULT 0,
            error_message String NULL,
            created_at DateTime DEFAULT now(),
            updated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, entity_type, action, event_id)
        TTL timestamp + INTERVAL 90 DAY
        SETTINGS index_granularity = 8192
        """
        )

        # Create time-based views/aggregations
        await db.command(
            f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS `{db_name}`.`event_logs_daily`
        ENGINE = SummingMergeTree
        ORDER BY (event_date, entity_type, action)
        POPULATE AS
        SELECT
            toDate(timestamp) as event_date,
            entity_type,
            action,
            count() as event_count,
            countIf(processed = 1) as processed_count,
            avg(JSONExtractInt(metrics, 'processing_time_ms')) as avg_processing_time_ms,
            max(JSONExtractInt(metrics, 'processing_time_ms')) as max_processing_time_ms
        FROM event_logs
        GROUP BY event_date, entity_type, action
        """
        )
