import logging
from ..models import EventLogModel
from ..schemas import EventLogCreate, EventLogUpdate
from stufio.crud.clickhouse_base import CRUDClickhouseBase


logger = logging.getLogger(__name__)


# Create CRUD operations for event logs
class CRUDEventLog(CRUDClickhouseBase[EventLogModel, EventLogCreate, EventLogUpdate]):
    """CRUD operations for event logs in Clickhouse."""
    pass


crud_event_log = CRUDEventLog(EventLogModel)
