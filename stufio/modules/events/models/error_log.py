from pydantic import ConfigDict
from datetime import datetime
from uuid import UUID
from typing import Optional

from stufio.db.clickhouse_base import ClickhouseBase, datetime_now_sec


class ErrorLogModel(ClickhouseBase):
    """ClickHouse model for storing error logs."""
    correlation_id: UUID
    timestamp: datetime = datetime_now_sec()
    error_type: str
    severity: str
    source: str
    error_message: str
    error_stack: Optional[str] = None
    request_path: str
    request_method: str
    status_code: int
    request_data: Optional[str] = None
    actor_id: Optional[str] = None
    resolution_status: Optional[str] = None
    resolution_notes: Optional[str] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None

    model_config = ConfigDict(table_name="event_error_logs")