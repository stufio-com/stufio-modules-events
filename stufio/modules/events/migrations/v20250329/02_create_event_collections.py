from motor.core import AgnosticDatabase
from stufio.core.migrations.base import MongoMigrationScript

class InitCollections(MongoMigrationScript):
    name = "init_collections"
    description = "Initialize collections for locale module"
    migration_type = "init"
    order = 10  # Low number ensures it runs first

    async def run(self, db: AgnosticDatabase) -> None:
        # Create collections with appropriate settings
        collections = ["event_definitions", "event_subscriptions"]

        for collection in collections:
            # Check if collection exists
            existing_collections = await db.list_collection_names()
            if collection not in existing_collections:
                # Create collection
                await db.create_collection(collection)

        # Create updated indexes for translations
        await db.command(
            {
                "createIndexes": "event_definitions",
                "indexes": [
                    {
                        "key": {"name": 1},
                        "name": "event_name_unique",
                        "unique": True,
                    },
                    {
                        "key": {"entity_type": 1, "action": 1},
                        "name": "event_entity_type_action_lookup",
                    },
                ],
            }
        )
        await db.command(
            {
                "createIndexes": "event_subscriptions",
                "indexes": [
                    {
                        "key": {"entity_type": 1, "action": 1},
                        "name": "event_subscription_entity_type_action_lookup",
                    },
                    {
                        "key": {"module_name": 1},
                        "name": "event_subscription_module_name_lookup",
                    },
                ],
            }
        )
