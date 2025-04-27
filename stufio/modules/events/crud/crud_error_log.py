import logging
from typing import List, Optional, Dict, Any, Union, Tuple
from datetime import datetime, timedelta
import uuid

from stufio.crud.clickhouse_base import CRUDClickhouse
from ..models.error_log import ErrorLogModel
from ..schemas.error import ErrorLogCreate, ErrorLogUpdate, ErrorLog, ErrorSummary

logger = logging.getLogger(__name__)


class CRUDErrorLog(CRUDClickhouse[ErrorLogModel, ErrorLogCreate, ErrorLogUpdate]):
    """CRUD operations for error logs in ClickHouse."""

    async def create(self, obj_in: ErrorLogCreate) -> Dict[str, Any]:
        """Create an error log entry."""
        try:
            result = await super().create(obj_in)
            return result
        except Exception as e:
            logger.error(f"❌ Failed to create error log: {e}", exc_info=True)
            # If we can't log the error, at least we still have it in the app logs
            return {"error": str(e)}

    async def get_recent_errors(
        self, 
        skip: int = 0, 
        limit: int = 20, 
        days: int = 7
    ) -> List[ErrorLog]:
        """Get recent errors from the last X days."""
        try:
            client = await self.client
            since_date = datetime.utcnow() - timedelta(days=days)

            result = await client.query(
                f"""
                SELECT *
                FROM {self.get_table_name()}
                WHERE timestamp >= :since_date
                ORDER BY timestamp DESC
                LIMIT :limit OFFSET :skip
                """,
                parameters={
                    "since_date": since_date,
                    "limit": limit,
                    "skip": skip
                }
            )

            errors = []
            for row in result.named_results():
                error = ErrorLog(**dict(row))
                errors.append(error)

            return errors
        except Exception as e:
            logger.error(f"❌ Failed to get recent errors: {e}", exc_info=True)
            return []

    async def get_error_summary(self, days: int = 30) -> ErrorSummary:
        """Get error summary statistics."""
        try:
            client = await self.client
            since_date = datetime.utcnow() - timedelta(days=days)

            # Get total count
            count_result = await client.query(
                f"""
                SELECT count(*) as total
                FROM {self.get_table_name()}
                WHERE timestamp >= :since_date
                """,
                parameters={"since_date": since_date}
            )

            total = list(count_result.named_results())[0]["total"]

            # Get counts by error type
            type_result = await client.query(
                f"""
                SELECT error_type, count() as count
                FROM {self.get_table_name()}
                WHERE timestamp >= :since_date
                GROUP BY error_type
                ORDER BY count DESC
                """,
                parameters={"since_date": since_date}
            )

            errors_by_type = {row["error_type"]: row["count"] for row in type_result.named_results()}

            # Get counts by path
            path_result = await client.query(
                f"""
                SELECT request_path, count() as count
                FROM {self.get_table_name()}
                WHERE timestamp >= :since_date
                GROUP BY request_path
                ORDER BY count DESC
                LIMIT 10
                """,
                parameters={"since_date": since_date}
            )

            errors_by_path = {row["request_path"]: row["count"] for row in path_result.named_results()}

            # Get counts by severity
            severity_result = await client.query(
                f"""
                SELECT severity, count() as count
                FROM {self.get_table_name()}
                WHERE timestamp >= :since_date
                GROUP BY severity
                ORDER BY count DESC
                """,
                parameters={"since_date": since_date}
            )

            errors_by_severity = {row["severity"]: row["count"] for row in severity_result.named_results()}

            # Get most recent errors
            recent_errors = await self.get_recent_errors(limit=5)

            return ErrorSummary(
                total_errors=total,
                errors_by_type=errors_by_type,
                errors_by_path=errors_by_path,
                errors_by_severity=errors_by_severity,
                recent_errors=recent_errors
            )
        except Exception as e:
            logger.error(f"❌ Failed to get error summary: {e}", exc_info=True)
            return ErrorSummary(
                total_errors=0,
                errors_by_type={},
                errors_by_path={},
                errors_by_severity={},
                recent_errors=[]
            )

    async def get_by_correlation_id(self, correlation_id: Union[str, uuid.UUID]) -> Optional[ErrorLog]:
        """Get error log by correlation ID."""
        try:
            client = await self.client

            if isinstance(correlation_id, str):
                correlation_id = uuid.UUID(correlation_id)

            result = await client.query(
                f"""
                SELECT *
                FROM {self.get_table_name()}
                WHERE correlation_id = :correlation_id
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                parameters={"correlation_id": str(correlation_id)}
            )

            rows = list(result.named_results())
            if not rows:
                return None

            return ErrorLog(**dict(rows[0]))
        except Exception as e:
            logger.error(
                f"❌ Failed to get error by correlation ID: {e}", exc_info=True
            )
            return None

# Create a global instance of the CRUD class
crud_error_log = CRUDErrorLog(ErrorLogModel)
