from pydantic import BaseModel
from typing import List
from stufio.core.config import ModuleSettings, get_settings

settings = get_settings()

class EventsSettings(ModuleSettings, BaseModel):

    # Event settings
    EVENT_PROCESSING_CONCURRENCY: int = 10
    EVENT_RETRY_ATTEMPTS: int = 3
    EVENT_RETRY_DELAY_SECONDS: int = 60

    # Kafka settings
    KAFKA_ENABLED: bool = True
    KAFKA_BOOTSTRAP_SERVERS: List[str] = ["localhost:9092"]
    KAFKA_TOPIC_PREFIX: str = "stufio.events"
    KAFKA_GROUP_ID: str = "stufio-events-consumers"

    # HTTP webhook settings
    WEBHOOK_ENABLED: bool = True
    WEBHOOK_TIMEOUT_SECONDS: int = 30
    WEBHOOK_MAX_RETRIES: int = 3

    # Clickhouse settings
    CLICKHOUSE_ENABLED: bool = True
    CLICKHOUSE_EVENT_LOGS_TTL_DAYS: int = 90

    # Metrics collection
    COLLECT_METRICS: bool = True

# Register these settings with the core
settings.register_module_settings("events", EventsSettings)
