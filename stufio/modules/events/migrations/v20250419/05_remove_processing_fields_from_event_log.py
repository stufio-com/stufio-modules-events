from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn
import logging

logger = logging.getLogger(__name__)


class RemoveProcessingFieldsFromEventLog(ClickhouseMigrationScript):
    name = "remove_processing_fields_from_event_log"
    description = "Remove processing fields and updated_at from event_logs table"
    migration_type = "schema"
    order = 40  # Higher than the previous migrations

    async def run(self, db) -> None:
        """
        Drop the processed, processing_attempts, error_message, and updated_at columns from the event_logs table,
        since we're moving to a different processing tracking approach and event_logs shouldn't be updated.
        """
        db_name = get_database_from_dsn()

        # First check if the table exists
        result = await db.command(
            f"""
            SELECT count() FROM system.tables 
            WHERE database = '{db_name}' AND name = 'event_logs';
            """
        )
        
        # Convert result to something we can check
        table_exists = False
        if isinstance(result, int) and result > 0:
            table_exists = True
        
        if not table_exists:
            # Log that the table doesn't exist, but don't fail the migration
            logger.info(f"Table {db_name}.event_logs does not exist, skipping column removal")
            return
            
        # Check for dependent views
        dependent_views = []
        try:
            # Get all materialized views
            result = await db.command(
                f"""
                SELECT name FROM system.tables 
                WHERE database = '{db_name}' AND engine = 'MaterializedView';
                """
            )
            
            if result:
                for row in result:
                    view_name = row[0]
                    # Check if view depends on event_logs table
                    dep_result = await db.command(
                        f"""
                        SELECT count() FROM system.tables 
                        WHERE database = '{db_name}' 
                          AND name = '{view_name}' 
                          AND has(CAST(dependencies AS Array(String)), 'event_logs');
                        """
                    )
                    
                    if isinstance(dep_result, int) and dep_result > 0:
                        dependent_views.append(view_name)
        except Exception as e:
            logger.warning(f"Error checking for dependent views: {e}")
            # Assume event_logs_daily might be dependent
            dependent_views = ['event_logs_daily']
            
        # If we couldn't determine dependencies, check for specific views
        if not dependent_views and await self._view_exists(db, db_name, 'event_logs_daily'):
            dependent_views = ['event_logs_daily']
        
        # Drop the dependent views
        if dependent_views:
            logger.info(f"Found the following dependent materialized views: {', '.join(dependent_views)}")
            
            # Drop each dependent view first
            for view_name in dependent_views:
                logger.info(f"Dropping materialized view: {view_name}")
                await db.command(
                    f"""
                    DROP VIEW IF EXISTS `{db_name}`.`{view_name}`;
                    """
                )
        
        # Now drop the columns one by one to handle potential errors with individual columns
        columns_to_drop = ['processed', 'processing_attempts', 'error_message', 'updated_at']
        
        for column in columns_to_drop:
            # Check if the column exists
            column_exists = await self._column_exists(db, db_name, 'event_logs', column)
            
            if column_exists:
                logger.info(f"Dropping column {column} from {db_name}.event_logs")
                try:
                    await db.command(
                        f"""
                        ALTER TABLE `{db_name}`.`event_logs`
                        DROP COLUMN IF EXISTS {column};
                        """
                    )
                    logger.info(f"Successfully removed {column} column from {db_name}.event_logs table")
                except Exception as e:
                    logger.error(f"Error dropping column {column}: {e}")
            else:
                logger.info(f"Column {column} does not exist in {db_name}.event_logs table, skipping")
        
        # Recreate the materialized views if needed
        if 'event_logs_daily' in dependent_views:
            logger.info("Recreating event_logs_daily materialized view without processing fields")
            await db.command(
                f"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS `{db_name}`.`event_logs_daily`
                ENGINE = SummingMergeTree()
                PARTITION BY toDate(event_date)
                ORDER BY (event_date, entity_type, action)
                AS SELECT
                    toDate(timestamp) as event_date,
                    entity_type,
                    action,
                    count() as event_count,
                    avg(JSONExtractInt(ifNull(metrics, '{{}}'), 'processing_time_ms')) as avg_processing_time_ms,
                    max(JSONExtractInt(ifNull(metrics, '{{}}'), 'processing_time_ms')) as max_processing_time_ms
                FROM `{db_name}`.`event_logs`
                GROUP BY event_date, entity_type, action;
                """
            )
    
    async def _view_exists(self, db, db_name: str, view_name: str) -> bool:
        """Check if a materialized view exists in the database"""
        try:
            result = await db.command(
                f"""
                SELECT count() FROM system.tables 
                WHERE database = '{db_name}' AND name = '{view_name}';
                """
            )
            return isinstance(result, int) and result > 0
        except Exception:
            return False
            
    async def _column_exists(self, db, db_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table"""
        try:
            result = await db.command(
                f"""
                SELECT count() FROM system.columns
                WHERE database = '{db_name}' AND table = '{table_name}' AND name = '{column_name}';
                """
            )
            return isinstance(result, int) and result > 0
        except Exception:
            return False