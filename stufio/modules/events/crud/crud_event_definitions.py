from typing import List, Optional, Dict, Any
from odmantic import AIOEngine
from stufio.crud.mongo_base import CRUDMongoBase
from ..models import EventDefinitionModel
from ..schemas.event import EventDefinition


class CRUDEventDefinition(CRUDMongoBase[EventDefinition, EventDefinition]):
    """CRUD operations for event definitions in MongoDB."""

    async def get_by_name(
        self, engine: AIOEngine, name: str
    ) -> Optional[EventDefinition]:
        """Get event definition by name."""
        return await engine.find_one(self.model, self.model.name == name)

    async def get_by_entity_action(
        self, engine: AIOEngine, entity_type: str, action: str
    ) -> List[EventDefinition]:
        """Get event definitions by entity type and action."""
        return await engine.find(
            self.model, 
            (self.model.entity_type == entity_type) & (self.model.action == action)
        )

    async def get_by_module(
        self, engine: AIOEngine, module_name: str
    ) -> List[EventDefinition]:
        """Get all event definitions for a specific module."""
        return await engine.find(self.model, self.model.module_name == module_name)


crud_event_definitions = CRUDEventDefinition(EventDefinitionModel)
