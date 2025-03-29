from stufio.core.setting_registry import (
    GroupMetadata, SubgroupMetadata, SettingMetadata, 
    SettingType, settings_registry
)


"""Register settings for this module"""
settings_registry.register_group(
    GroupMetadata(
        id="events",
        label="Events",
        description="Settings related to event processing and handling",
        icon="event",
        order=50,  # Between API and Database
    )
)

# Register subgroups
settings_registry.register_subgroup(
    SubgroupMetadata(
        id="processing",
        group_id="events",
        label="Processing",
        order=10
    ),
)
settings_registry.register_subgroup(
    SubgroupMetadata(
        id="kafka",
        group_id="events",
        label="Kafka",
        order=20
    ),
)
settings_registry.register_subgroup(
    SubgroupMetadata(
        id="webhook",
        group_id="events",
        label="Webhook",
        order=30
    ),
)
settings_registry.register_subgroup(
    SubgroupMetadata(
        id="clickhouse",
        group_id="events",
        label="Clickhouse",
        order=40
    ),
)
settings_registry.register_subgroup(
    SubgroupMetadata(
        id="metrics",
        group_id="events",
        label="Metrics",
        order=50
    ),
)
# Register settings
settings_registry.register_setting(
    SettingMetadata(
        key="EVENT_PROCESSING_CONCURRENCY",
        label="Event Processing Concurrency",
        description="Number of concurrent event processors",
        group="events",
        subgroup="processing",
        type=SettingType.NUMBER,
        order=10,
        module="events",
        default=10,
        min=1,
        max=100,
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="EVENT_RETRY_ATTEMPTS",
        label="Event Retry Attempts",
        description="Number of retry attempts for failed events",
        group="events",
        subgroup="processing",
        type=SettingType.NUMBER,
        order=20,
        module="events",
        default=3,
        min=0,
        max=10,
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="EVENT_RETRY_DELAY_SECONDS",
        label="Event Retry Delay (seconds)",
        description="Delay between retry attempts for failed events",
        group="events",
        subgroup="processing",
        type=SettingType.NUMBER,
        order=30,
        module="events",
        default=60,
        min=0,
        max=3600,
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="KAFKA_ENABLED",
        label="Enable Kafka",
        description="Enable Kafka for event processing",
        group="events",
        subgroup="kafka",
        type=SettingType.BOOLEAN,
        order=10,
        module="events",
        default=True,
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="KAFKA_BOOTSTRAP_SERVERS",
        label="Kafka Bootstrap Servers",
        
        description="Comma-separated list of Kafka bootstrap servers",
        group="events",
        subgroup="kafka",
        type=SettingType.TEXT,
        order=20,
        module="events",
        default="localhost:9092",
        placeholder="localhost:9092,otherhost:9092",
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="KAFKA_TOPIC_PREFIX",
        label="Kafka Topic Prefix",
        description="Prefix for Kafka topics",
        group="events",
        subgroup="kafka",
        type=SettingType.TEXT,
        order=30,
        module="events",
        default="stufio.events",
        placeholder="stufio.events",
        
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="KAFKA_GROUP_ID",
        label="Kafka Group ID",
        description="Kafka consumer group ID",
        group="events",
        subgroup="kafka",
        type=SettingType.TEXT,
        order=40,
        module="events",
        default="stufio-events-consumers",
        placeholder="stufio-events-consumers",
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="WEBHOOK_ENABLED",
        label="Enable Webhook",
        description="Enable HTTP webhook for event processing",
        group="events",
        subgroup="webhook",
        type=SettingType.BOOLEAN,
        order=10,
        module="events",
        default=True,
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="WEBHOOK_TIMEOUT_SECONDS",
        
        label="Webhook Timeout (seconds)",
        description="Timeout for webhook requests in seconds",
        group="events",
        subgroup="webhook",
        type=SettingType.NUMBER,
        order=20,
        module="events",
        default=30,
        min=1,
        max=120,
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="WEBHOOK_MAX_RETRIES",
        label="Webhook Max Retries",
        description="Maximum number of retries for webhook requests",
        group="events",
        subgroup="webhook",
        type=SettingType.NUMBER,
        order=30,
        module="events",
        default=3,
        min=0,
        max=10,
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="CLICKHOUSE_ENABLED",
        label="Enable Clickhouse",
        description="Enable Clickhouse for event logging",
        group="events",
        subgroup="clickhouse",
        type=SettingType.BOOLEAN,
        order=10,
        module="events",
        default=True,
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="CLICKHOUSE_EVENT_LOGS_TTL_DAYS",
        label="Clickhouse Event Logs TTL (days)",
        description="Time-to-live for event logs in Clickhouse",
        group="events",
        subgroup="clickhouse",
        type=SettingType.NUMBER,
        order=20,
        module="events",
        default=90,
        min=1,
        max=365,
    )
)
settings_registry.register_setting(
    SettingMetadata(
        key="COLLECT_METRICS",
        label="Collect Metrics",
        description="Enable collection of metrics for event processing",
        group="events",
        subgroup="metrics",
        type=SettingType.BOOLEAN,
        order=10,
        module="events",
        default=True,
    )
)