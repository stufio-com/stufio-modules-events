from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn
import logging

logger = logging.getLogger(__name__)


class RemoveMetricsFromEventLog(ClickhouseMigrationScript):
    name = "remove_metrics_from_event_log"
    description = "Remove metrics from event_logs table"
    migration_type = "schema"
    order = 30

    async def run(self, db) -> None:
        """
        Drop the metrics column from the event_logs table,
        since metrics are now stored in the event_metrics table.
        """
        db_name = get_database_from_dsn()

        # First check if the table exists using command instead of execute
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
            logger.info(f"Table {db_name}.event_logs does not exist, skipping metrics column removal")
            return
            
        # Check if metrics column exists
        result = await db.command(
            f"""
            SELECT count() FROM system.columns
            WHERE database = '{db_name}' AND table = 'event_logs' AND name = 'metrics';
            """
        )
        
        column_exists = False
        if isinstance(result, int) and result > 0:
            column_exists = True
        
        if not column_exists:
            logger.info(f"Column metrics does not exist in {db_name}.event_logs table, skipping")
            return
        
        # Find materialized views that depend on event_logs table
        # First check if the 'query' column exists in system.tables
        query_col_exists = True
        try:
            result = await db.command(
                f"""
                SELECT count() FROM system.columns
                WHERE database = 'system' AND table = 'tables' AND name = 'query';
                """
            )
            if isinstance(result, int) and result == 0:
                query_col_exists = False
        except Exception as e:
            logger.warning(f"Could not check if query column exists in system.tables: {e}")
            query_col_exists = False
        
        dependent_views = []
        
        if query_col_exists:
            # If query column exists, use it to find views that reference the metrics column
            try:
                result = await db.command(
                    f"""
                    SELECT name FROM system.tables 
                    WHERE database = '{db_name}' 
                      AND engine = 'MaterializedView'
                      AND query LIKE '%event_logs%metrics%';
                    """
                )
                
                if result:
                    for row in result:
                        view_name = row[0]
                        dependent_views.append(view_name)
            except Exception as e:
                logger.warning(f"Error finding views by query: {e}")
        
        # If we couldn't use the query column or it failed, try a more basic approach:
        # Just get all materialized views and drop them before modifying the table
        if not dependent_views:
            try:
                result = await db.command(
                    f"""
                    SELECT name FROM system.tables 
                    WHERE database = '{db_name}' AND engine = 'MaterializedView';
                    """
                )
                
                if result:
                    for row in result:
                        view_name = row[0]
                        # Check if this view uses the event_logs table by examining its dependencies
                        dep_result = await db.command(
                            f"""
                            SELECT count() FROM system.tables
                            WHERE database = '{db_name}' 
                              AND name = '{view_name}'
                              AND dependencies LIKE '%event_logs%';
                            """
                        )
                        
                        if isinstance(dep_result, int) and dep_result > 0:
                            dependent_views.append(view_name)
            except Exception as e:
                logger.warning(f"Error finding views by dependencies: {e}")
        
        # If still no views found, check for event_logs_daily specifically which is likely to exist
        if not dependent_views and await self._view_exists(db, db_name, 'event_logs_daily'):
            dependent_views = ['event_logs_daily']
        
        # Drop the views we found
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
        
        # Now we can safely drop the metrics column
        logger.info(f"Dropping metrics column from {db_name}.event_logs")
        await db.command(
            f"""
            ALTER TABLE `{db_name}`.`event_logs`
            DROP COLUMN IF EXISTS metrics;
            """
        )
        logger.info(f"Successfully removed metrics column from {db_name}.event_logs table")
        
        # Recreate the materialized views if needed
        # In this case, we assume event_logs_daily needs to be recreated without the metrics column
        if 'event_logs_daily' in dependent_views:
            logger.info("Recreating event_logs_daily materialized view without metrics column")
            await db.command(
                f"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS `{db_name}`.`event_logs_daily`
                ENGINE = SummingMergeTree()
                PARTITION BY toDate(timestamp)
                ORDER BY (date, event_type, module, correlation_id)
                AS SELECT
                    toDate(timestamp) AS date,
                    event_type,
                    module,
                    correlation_id,
                    count() AS count,
                    min(timestamp) AS first_seen,
                    max(timestamp) AS last_seen
                FROM `{db_name}`.`event_logs`
                GROUP BY date, event_type, module, correlation_id;
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
