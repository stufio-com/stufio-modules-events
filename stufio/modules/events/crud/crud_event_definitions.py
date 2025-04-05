from typing import List, Optional
from stufio.crud.mongo_base import CRUDMongo
from ..models import EventDefinitionModel
from ..schemas.event import EventDefinitionCreate, EventDefinitionUpdate, EventDefinition


class CRUDEventDefinition(
    CRUDMongo[EventDefinitionModel, EventDefinitionCreate, EventDefinitionUpdate]
):
    """CRUD operations for event definitions in MongoDB."""

    async def get_by_name(
        self, name: str
    ) -> Optional[EventDefinition]:
        """Get event definition by name."""
        return await self.engine.find_one(self.model, self.model.name == name)

    async def get_by_entity_action(
        self, entity_type: str, action: str
    ) -> List[EventDefinition]:
        """Get event definitions by entity type and action."""
        return await self.engine.find(
            self.model, 
            (self.model.entity_type == entity_type) & (self.model.action == action)
        )

    async def get_by_module(
        self, module_name: str
    ) -> List[EventDefinition]:
        """Get all event definitions for a specific module."""
        return await self.engine.find(self.model, self.model.module_name == module_name)


crud_event_definitions = CRUDEventDefinition(EventDefinitionModel)
