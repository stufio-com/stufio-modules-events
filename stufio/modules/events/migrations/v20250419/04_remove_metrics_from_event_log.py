from stufio.core.migrations.base import ClickhouseMigrationScript
from stufio.db.clickhouse import get_database_from_dsn


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
        
        if table_exists:
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
            
            if column_exists:
                # Alter to remove the metrics column
                await db.command(
                    f"""
                    ALTER TABLE `{db_name}`.`event_logs`
                    DROP COLUMN IF EXISTS metrics;
                    """
                )
                print(f"Successfully removed metrics column from {db_name}.event_logs table")
        else:
            # Log that the table doesn't exist, but don't fail the migration
            print(f"Table {db_name}.event_logs does not exist, skipping metrics column removal")
