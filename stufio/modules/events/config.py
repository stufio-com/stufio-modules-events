from typing import List, Union, Any
from pydantic import Field, field_validator
from stufio.core.settings import ModuleSettings
from stufio.core.config import get_settings

settings = get_settings()

class EventsSettings(ModuleSettings):
    # App settings remain the same
    APP_CONSUME_ROUTES: bool = True
    APP_CONSUME_ROUTES_ONLY: bool = False
    APP_CONSUME_ROUTES_EXCLUDE: List[str] = []
    APP_CONSUME_ROUTES_INCLUDE: List[str] = []

    # Event settings remain the same
    EVENT_PROCESSING_CONCURRENCY: int = 10
    EVENT_RETRY_ATTEMPTS: int = 3
    EVENT_RETRY_DELAY_SECONDS: int = 60

    # Kafka settings with string handling for bootstrap servers
    KAFKA_ENABLED: bool = False
    KAFKA_BOOTSTRAP_SERVERS: Union[str, List[str]] = ["kafka:9092"]
    KAFKA_TOPIC_PREFIX: str = "stufio.events"
    ASYNCAPI_PREFIX: str = "stufio.events"
    ASYNCAPI_DOCS_ENABLED: bool = True
    KAFKA_GROUP_ID: str = "stufio-events"

    # Kafka topic settings
    KAFKA_DEFAULT_PARTITIONS: int = 3
    KAFKA_DEFAULT_HL_PARTITIONS: int = 9
    KAFKA_DEFAULT_REPLICATION_FACTOR: int = 1
    KAFKA_RETENTION_MS: int = 604800000  # 7 days
    KAFKA_SEGMENT_BYTES: int = 1073741824  # 1 GB

    # Kafka delayed topic settings
    KAFKA_DELAYED_TOPIC_ENABLED: bool = True
    KAFKA_DELAYED_TOPIC_NAME: str = "stufio.events.delayed"

    # Kafka consumer settings
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_ENABLE_AUTO_COMMIT: bool = True
    KAFKA_AUTO_COMMIT_INTERVAL_MS: int = 1000  # 1 second
    KAFKA_MAX_POLL_RECORDS: int = 10
    KAFKA_MAX_PARTITION_FETCH_BYTES: int = 1048576  # 1 MB
    KAFKA_FETCH_MAX_BYTES: int = 1048576  # 1 MB
    KAFKA_FETCH_WAIT_MAX_MS: int = 100  # 100 ms
    KAFKA_MAX_POLL_TIMEOUT_MS: int = 300000  # 5 minutes
    KAFKA_MAX_POLL_INTERVAL_MS: int = 300000  # 5 minutes
    KAFKA_SESSION_TIMEOUT_MS: int = 60000  # 1 minute
    KAFKA_HEARTBEAT_INTERVAL_MS: int = 20000  # 20 seconds

    # ClickHouse Kafka integration settings
    KAFKA_TOPIC_EXPIRED: str = "nameniac_scrape_expired"
    KAFKA_TOPIC_WHOIS: str = "nameniac_whois"
    KAFKA_GROUP_EXPIRED_CLICKHOUSE: str = "consumer-ch-scrapeexpired"
    KAFKA_GROUP_WHOIS_CLICKHOUSE: str = "consumer-ch-whois"

    @field_validator('KAFKA_BOOTSTRAP_SERVERS')
    @classmethod
    def parse_bootstrap_servers(cls, v: Any) -> List[str]:
        """Convert bootstrap servers string to list if needed."""
        if isinstance(v, str):
            # Split by comma if it contains commas
            if ',' in v:
                return [server.strip() for server in v.split(',')]
            # Otherwise treat as a single server
            return [v.strip()]
        elif isinstance(v, list):
            return v
        raise ValueError(f"Invalid bootstrap servers format: {v}")

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
