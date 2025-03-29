from typing import List, Optional
from odmantic import AIOEngine
from stufio.crud.mongo_base import CRUDMongoBase
from ..models import EventSubscriptionModel
from ..schemas import EventSubscription, EventSubscriptionCreate, EventSubscriptionUpdate


class CRUDEventSubscription(
    CRUDMongoBase[EventSubscriptionCreate, EventSubscriptionUpdate]
):
    """CRUD operations for event subscriptions in MongoDB."""

    async def get_by_entity_action(
        self,
        engine: AIOEngine,
        entity_type: Optional[str] = None,
        action: Optional[str] = None,
    ) -> List[EventSubscription]:
        """Get event subscriptions for a specific entity type and action, or all if None."""
        query = self.model.enabled == True

        if entity_type:
            # Match subscriptions where entity_type is None or matches the provided entity_type
            query = query & ((self.model.entity_type == None) | (self.model.entity_type == entity_type))

        if action:
            # Match subscriptions where action is None or matches the provided action
            query = query & ((self.model.action == None) | (self.model.action == action))

        return await engine.find(self.model, query)

    async def get_by_module(self, engine: AIOEngine, module_name: str) -> List[EventSubscriptionModel]:
        """Get all event subscriptions for a specific module."""
        return await engine.find(self.model, self.model.module_name == module_name)


crud_event_subscriptions = CRUDEventSubscription(EventSubscriptionModel)
