"""
Migration to create error_logs table in Clickhouse
"""
from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn

class CreateErrorLogsTable(ClickhouseMigrationScript):
    name = "create_error_logs_table"
    description = "Create Clickhouse table for detailed error logs"
    migration_type = "schema"
    order = 10

    async def run(self, db):

        db_name = get_database_from_dsn()

        # Create error_logs table
        await db.command(
            f"""
        CREATE TABLE IF NOT EXISTS `{db_name}`.`error_logs` (
            id UUID DEFAULT generateUUIDv4(),
            error_id UUID NOT NULL,
            correlation_id UUID NULL,
            timestamp DateTime DEFAULT now(),
            
            -- Error classification
            error_type String NOT NULL,
            severity String NOT NULL,
            source String NOT NULL,
            
            -- Error context
            entity_type String NULL,
            entity_id String NULL,
            action String NULL,
            actor_type String NULL,
            actor_id String NULL,
            
            -- Error details
            error_message String NOT NULL,
            error_stack String NULL CODEC(ZSTD(3)),
            request_path String NULL,
            request_method String NULL,
            status_code Int32 NULL,
            
            -- Serialized data
            request_data String NULL CODEC(ZSTD(3)),
            response_data String NULL CODEC(ZSTD(3)),
            
            -- Metadata
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, error_type, source)
        TTL timestamp + INTERVAL 90 DAY
        SETTINGS index_granularity = 8192
        """
        )

        # Create materialized view for error statistics
        await db.command(
            f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS `{db_name}`.`error_logs_daily`
        ENGINE = SummingMergeTree
        ORDER BY (error_date, error_type, source, severity)
        POPULATE AS
        SELECT
            toDate(timestamp) as error_date,
            error_type,
            source,
            severity,
            status_code,
            count() as error_count
        FROM error_logs
        GROUP BY error_date, error_type, source, severity, status_code
        """
        )
