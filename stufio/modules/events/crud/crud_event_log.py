import logging
from ..models import EventLogModel
from ..schemas.event import EventLogCreate, EventLogUpdate
from stufio.crud.clickhouse_base import CRUDClickhouse


logger = logging.getLogger(__name__)


# Create CRUD operations for event logs
class CRUDEventLog(CRUDClickhouse[EventLogModel, EventLogCreate, EventLogUpdate]):
    """CRUD operations for event logs in Clickhouse."""
    pass


crud_event_log = CRUDEventLog(EventLogModel)
