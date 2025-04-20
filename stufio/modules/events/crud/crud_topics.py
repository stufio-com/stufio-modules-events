from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime
import logging
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.structs import TopicPartition

from ..schemas.topic import (
    TopicConfig, 
    TopicConfigUpdate, 
    TopicInfo, 
    TopicMetrics, 
    TopicPartitionInfo,
    TopicListResponse,
    TopicRotationResponse,
    CleanupResponse,
    TopicHealthResponse
)
from stufio.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

class CRUDKafkaTopics:
    """CRUD operations for Kafka topic management."""

    def __init__(self):
        self.admin_client = None

    async def _get_admin_client(self) -> AIOKafkaAdminClient:
        """Get or create the Kafka admin client."""
        if self.admin_client is None:
            try:
                bootstrap_servers = getattr(settings, "events_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
                self.admin_client = AIOKafkaAdminClient(
                    bootstrap_servers=bootstrap_servers,
                    client_id=f"stufio-admin-topics"
                )
                await self.admin_client.start()
                logger.info(f"Connected to Kafka at {bootstrap_servers}")
            except Exception as e:
                logger.error(f"Failed to connect to Kafka: {e}", exc_info=True)
                raise
        return self.admin_client

    async def close_client(self) -> None:
        """Close the Kafka admin client."""
        if self.admin_client:
            await self.admin_client.close()
            self.admin_client = None
            logger.info("Closed Kafka admin client")

    async def list_topics(self) -> List[TopicInfo]:
        """Get all topics with their configurations."""
        try:
            admin_client = await self._get_admin_client()

            # Get topic names
            topic_names = await admin_client.list_topics()

            # Get topic configs
            topic_configs = await admin_client.describe_configs(
                [ConfigResource(ConfigResourceType.TOPIC, topic) for topic in topic_names]
            )

            # Get topic partitions
            topic_partitions = await admin_client.describe_topics(topic_names)

            # Convert topic_partitions to dictionary for easier lookup
            topic_partitions_dict = {item['topic']: item for item in topic_partitions}
            
            # Create a dictionary for topic_configs
            topic_configs_dict: Dict[str, Dict[str, Any]] = {}
            # Add debug info to understand the structure
            logger.debug(f"topic_configs type: {type(topic_configs)}")
            
            # If topic_configs is not a list or a tuple, make it a list so we can iterate
            if not isinstance(topic_configs, (list, tuple)):
                topic_configs = [topic_configs]
                
            for config_item in topic_configs:
                logger.debug(f"Config item type: {type(config_item)}")
                
                # Handle the structure: (0, '', 2, 'topic_name', [(config_tuples)])
                if isinstance(config_item, tuple) and len(config_item) > 3:
                    # Extract topic name from tuple (index 3)
                    topic_name = config_item[3]
                    
                    # Extract configs list from tuple (index 4) if available
                    if len(config_item) > 4 and isinstance(config_item[4], list):
                        configs_list = config_item[4]
                        topic_configs_dict[topic_name] = {"configs": configs_list}
                        logger.debug(f"Added topic {topic_name} with {len(configs_list)} configs")
                
                # Legacy object format handling - kept for backward compatibility
                elif hasattr(config_item, 'resources'):
                    resources = config_item.resources
                    logger.debug(f"Resources type: {type(resources)}")
                    
                    if isinstance(resources, list):
                        for resource in resources:
                            # Handle if resource is a tuple with expected structure
                            if isinstance(resource, tuple) and len(resource) > 3:
                                topic_name = resource[3]  # Topic name at index 3
                                configs_list = resource[4] if len(resource) > 4 else []  # Config list at index 4
                                if isinstance(configs_list, list):
                                    topic_configs_dict[topic_name] = {"configs": configs_list}
                                    logger.debug(f"Added topic {topic_name} with {len(configs_list)} configs from tuple resource")
                            # Handle legacy dictionary format (if any implementation changes in the future)
                            elif isinstance(resource, dict) and 'name' in resource:
                                topic_name = resource['name']
                                configs = resource.get('configs', [])
                                topic_configs_dict[topic_name] = {"configs": configs}
                                logger.debug(f"Added topic {topic_name} with {len(configs)} configs from dict resource")

            # Add detailed debugging to see actual config structure
            if topic_configs and len(topic_configs) > 0:
                sample_config = topic_configs[0]
                logger.debug(f"Sample config item: {sample_config}")
                logger.debug(f"Sample config type: {type(sample_config)}")
                
                # If it's a tuple, check its structure
                if isinstance(sample_config, tuple):
                    logger.debug(f"Config tuple length: {len(sample_config)}")
                    for i, item in enumerate(sample_config):
                        logger.debug(f"Config tuple item {i}: {type(item)}")
                        # If this is the configs list
                        if isinstance(item, list) and len(item) > 0 and isinstance(item[0], tuple):
                            logger.debug(f"First config entry: {item[0]}")
                            if len(item[0]) >= 3:
                                logger.debug(f"Config format: name={item[0][0]}, value={item[0][1]}, is_default={item[0][2]}")

            results: List[TopicInfo] = []
            for topic in topic_names:
                # Extract configs
                config: Dict[str, str] = {}
                if topic in topic_configs_dict:
                    configs_data = topic_configs_dict[topic]["configs"]
                    
                    # Process list of config tuples - each config is a tuple like:
                    # (name, value, is_default, ...)
                    if isinstance(configs_data, list):
                        for cfg in configs_data:
                            if isinstance(cfg, tuple) and len(cfg) >= 3:
                                name = cfg[0]       # Config name
                                value = cfg[1]      # Config value
                                is_default = cfg[2] # Whether it's default
                                if not is_default and value is not None:
                                    config[name] = value
                            # Also handle object types if needed
                            elif hasattr(cfg, 'name') and hasattr(cfg, 'value'):
                                if not getattr(cfg, 'is_default', False) and cfg.value is not None:
                                    config[cfg.name] = cfg.value
                    elif isinstance(configs_data, dict):
                        # Handle dictionary structure
                        for name, cfg in configs_data.items():
                            if hasattr(cfg, 'is_default') and hasattr(cfg, 'value'):
                                if not cfg.is_default and cfg.value is not None:
                                    config[name] = cfg.value
                            elif isinstance(cfg, dict):
                                if not cfg.get('is_default', True) and cfg.get('value') is not None:
                                    config[name] = cfg['value']

                # Extract partition info
                partitions: List[TopicPartitionInfo] = []
                for partition in topic_partitions_dict[topic]['partitions']:
                    # Log the actual structure for debugging
                    if isinstance(partition, dict):
                        logger.debug(f"Partition structure: {list(partition.keys())}")
                    
                    # Use safe extraction method for all fields
                    partitions.append(TopicPartitionInfo(
                        id=self._safe_get(partition, ['id', 'partition', 'partition_id'], 0),
                        leader=self._safe_get(partition, ['leader', 'leader_id'], -1),
                        replicas=self._safe_get(partition, ['replicas', 'replica_nodes'], []),
                        isr=self._safe_get(partition, ['isr', 'isr_nodes', 'in_sync_replicas'], [])
                    ))

                # Build response object
                topic_info = TopicInfo(
                    name=topic,
                    num_partitions=len(partitions),
                    partitions=partitions,
                    config=config,
                    cleanup_policy=config.get("cleanup.policy", "delete"),
                    retention_days=round(int(config.get("retention.ms", "604800000")) / (24 * 60 * 60 * 1000), 2)
                )
                results.append(topic_info)

            return results
        except Exception as e:
            logger.error(f"Failed to list topics: {e}", exc_info=True)
            return []
        finally:
            await self.close_client()

    async def get_topic_metrics(self, topic_name: str) -> Union[TopicMetrics, Dict[str, str]]:
        """Get detailed metrics for a specific topic."""
        try:
            admin_client = await self._get_admin_client()

            # Check if topic exists
            topics = await admin_client.list_topics()
            if topic_name not in topics:
                return {"error": f"Topic {topic_name} not found"}

            # Get topic config
            topic_configs = await admin_client.describe_configs(
                [ConfigResource(ConfigResourceType.TOPIC, topic_name)]
            )
            logger.debug(f"Topic {topic_name} configs type: {type(topic_configs)}")

            # Get topic metadata
            topic_metadata = await admin_client.describe_topics([topic_name])

            # Use the consumer API to get offsets
            from aiokafka import AIOKafkaConsumer
            consumer = AIOKafkaConsumer(
                bootstrap_servers=getattr(settings, "events_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                client_id="stufio-admin-metrics"
            )
            await consumer.start()

            # Get partition info
            partitions: List[TopicPartitionInfo] = []
            total_messages = 0

            # Find the topic metadata entry - topic_metadata is a list, not a dict
            topic_meta = None
            for meta_item in topic_metadata:
                if meta_item.get('topic') == topic_name:
                    topic_meta = meta_item
                    break
                    
            if topic_meta and 'partitions' in topic_meta:
                for partition in topic_meta['partitions']:
                    # Use the safe getter method
                    partition_id = self._safe_get(partition, ['id', 'partition', 'partition_id'], 0)
                    leader = self._safe_get(partition, ['leader', 'leader_id'], -1)
                    replicas = self._safe_get(partition, ['replicas', 'replica_nodes'], [])
                    isr = self._safe_get(partition, ['isr', 'isr_nodes', 'in_sync_replicas'], [])
                    
                    tp = TopicPartition(topic_name, partition_id)
                    
                    # Get beginning and end offsets
                    beginning_offset = await consumer.beginning_offsets([tp])
                    end_offset = await consumer.end_offsets([tp])

                    begin = beginning_offset.get(tp, 0)
                    end = end_offset.get(tp, 0)
                    message_count = end - begin
                    total_messages += message_count

                    partitions.append(TopicPartitionInfo(
                        id=partition_id,
                        leader=leader,
                        replicas=replicas,
                        isr=isr,
                        begin_offset=begin,
                        end_offset=end,
                        message_count=message_count
                    ))

            # Create a dictionary for topic_configs - using same approach as list_topics
            topic_configs_dict: Dict[str, Dict[str, Any]] = {}
            
            # If topic_configs is not a list or a tuple, make it a list so we can iterate
            if not isinstance(topic_configs, (list, tuple)):
                topic_configs = [topic_configs]
                
            for config_item in topic_configs:
                # Handle the structure: (0, '', 2, 'topic_name', [(config_tuples)])
                if isinstance(config_item, tuple) and len(config_item) > 3:
                    # Extract topic name from tuple (index 3)
                    config_topic_name = config_item[3]
                    
                    # Extract configs list from tuple (index 4) if available
                    if len(config_item) > 4 and isinstance(config_item[4], list):
                        configs_list = config_item[4]
                        topic_configs_dict[config_topic_name] = {"configs": configs_list}
                
                # Legacy object format handling - kept for backward compatibility
                elif hasattr(config_item, 'resources'):
                    resources = config_item.resources
                    
                    if isinstance(resources, list):
                        for resource in resources:
                            # Handle if resource is a tuple with expected structure
                            if isinstance(resource, tuple) and len(resource) > 3:
                                config_topic_name = resource[3]  # Topic name at index 3
                                configs_list = resource[4] if len(resource) > 4 else []  # Config list at index 4
                                if isinstance(configs_list, list):
                                    topic_configs_dict[config_topic_name] = {"configs": configs_list}
                            # Handle legacy dictionary format (if any implementation changes in the future)
                            elif isinstance(resource, dict) and 'name' in resource:
                                config_topic_name = resource['name']
                                configs = resource.get('configs', [])
                                topic_configs_dict[config_topic_name] = {"configs": configs}

            # Extract configs
            config: Dict[str, str] = {}
            if topic_name in topic_configs_dict:
                configs_data = topic_configs_dict[topic_name]["configs"]
                
                # Process list of config tuples - each config is a tuple like:
                # (name, value, is_default, ...)
                if isinstance(configs_data, list):
                    for cfg in configs_data:
                        if isinstance(cfg, tuple) and len(cfg) >= 3:
                            name = cfg[0]       # Config name
                            value = cfg[1]      # Config value
                            is_default = cfg[2] # Whether it's default
                            if not is_default and value is not None:
                                config[name] = value
                        # Also handle object types if needed
                        elif hasattr(cfg, 'name') and hasattr(cfg, 'value'):
                            if not getattr(cfg, 'is_default', False) and cfg.value is not None:
                                config[cfg.name] = cfg.value
                elif isinstance(configs_data, dict):
                    # Handle dictionary structure
                    for name, cfg in configs_data.items():
                        if hasattr(cfg, 'is_default') and hasattr(cfg, 'value'):
                            if not cfg.is_default and cfg.value is not None:
                                config[name] = cfg.value
                        elif isinstance(cfg, dict):
                            if not cfg.get('is_default', True) and cfg.get('value') is not None:
                                config[name] = cfg['value']

            await consumer.stop()

            # Calculate metrics
            retention_ms = int(config.get("retention.ms", 604800000))  # Default 7 days
            retention_days = round(retention_ms / (24 * 60 * 60 * 1000), 2)

            # Get estimated size if possible
            estimated_size = "Unknown"
            if "segment.bytes" in config:
                segment_bytes = int(config["segment.bytes"])
                segments_per_partition = 0
                for p in partitions:
                    # Rough estimate of segments
                    if p.message_count and p.message_count > 0:
                        segments_per_partition += max(1, p.message_count // 1000000)

                total_segments = max(1, segments_per_partition)
                estimated_size_bytes = total_segments * segment_bytes

                # Convert to human-readable format
                if estimated_size_bytes < 1024:
                    estimated_size = f"{estimated_size_bytes} bytes"
                elif estimated_size_bytes < 1024 * 1024:
                    estimated_size = f"{estimated_size_bytes / 1024:.2f} KB"
                elif estimated_size_bytes < 1024 * 1024 * 1024:
                    estimated_size = f"{estimated_size_bytes / (1024 * 1024):.2f} MB"
                else:
                    estimated_size = f"{estimated_size_bytes / (1024 * 1024 * 1024):.2f} GB"

            result = TopicMetrics(
                name=topic_name,
                num_partitions=len(partitions),
                partitions=partitions,
                total_messages=total_messages,
                config=config,
                retention_days=retention_days,
                cleanup_policy=config.get("cleanup.policy", "delete"),
                estimated_size=estimated_size,
                last_updated=datetime.now().isoformat()
            )

            return result
        except Exception as e:
            logger.error(f"Failed to get topic metrics: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            await self.close_client()

    async def update_topic_config(self, topic_name: str, config: Dict[str, str]) -> Dict[str, Any]:
        """Update the configuration for a topic."""
        try:
            admin_client = await self._get_admin_client()

            # Check if topic exists
            topics = await admin_client.list_topics()
            if topic_name not in topics:
                return {"error": f"Topic {topic_name} not found"}

            # Define allowed config parameters for security
            allowed_configs = {
                "cleanup.policy", 
                "retention.ms", 
                "retention.bytes",
                "segment.bytes",
                "min.compaction.lag.ms",
                "max.compaction.lag.ms",
                "min.cleanable.dirty.ratio"
            }

            # Filter out disallowed config parameters
            filtered_config = {k: v for k, v in config.items() if k in allowed_configs}

            if not filtered_config:
                return {"error": "No valid configuration parameters provided"}

            # Create config resource
            config_resource = ConfigResource(
                ConfigResourceType.TOPIC, topic_name, filtered_config
            )

            # Update topic configs
            await admin_client.alter_configs([config_resource])

            return {
                "status": "success",
                "topic": topic_name,
                "updated_config": filtered_config
            }
        except Exception as e:
            logger.error(f"Failed to update topic config: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            await self.close_client()

    async def rotate_topic_logs(self, topic_name: str) -> TopicRotationResponse:
        """Force a log rotation for a topic."""
        try:
            # This operation is more complex and requires direct JMX access
            # We'll simulate it by reducing segment.bytes temporarily
            admin_client = await self._get_admin_client()

            # Check if topic exists
            topics = await admin_client.list_topics()
            if topic_name not in topics:
                return {"error": f"Topic {topic_name} not found"}

            # Get current configs
            configs = await admin_client.describe_configs(
                [ConfigResource(ConfigResourceType.TOPIC, topic_name)]
            )

            current_segment_bytes = None
            if topic_name in configs:
                for cfg in configs[topic_name].configs.values():
                    if cfg.name == "segment.bytes" and cfg.value is not None:
                        current_segment_bytes = cfg.value

            if not current_segment_bytes:
                return {"error": "Could not retrieve current segment.bytes configuration"}

            # Create config resource with temporary small segment size
            temp_config = {
                "segment.bytes": "1000000"  # 1MB to force new segments
            }

            config_resource = ConfigResource(
                ConfigResourceType.TOPIC, topic_name, temp_config
            )

            # Update topic config temporarily
            await admin_client.alter_configs([config_resource])

            # Sleep to allow Kafka to react
            import asyncio
            await asyncio.sleep(2)

            # Restore original segment size
            restore_config = {
                "segment.bytes": current_segment_bytes
            }

            restore_resource = ConfigResource(
                ConfigResourceType.TOPIC, topic_name, restore_config
            )

            await admin_client.alter_configs([restore_resource])

            return TopicRotationResponse(
                status="success",
                topic=topic_name,
                message="Topic log rotation triggered"
            )
        except Exception as e:
            logger.error(f"Failed to rotate topic logs: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            await self.close_client()

    async def cleanup_empty_topics(self, prefix: Optional[str] = None, older_than_days: int = 30) -> CleanupResponse:
        """Delete empty topics that haven't been used in a while."""
        try:
            admin_client = await self._get_admin_client()

            # List all topics
            all_topics = await admin_client.list_topics()

            # Filter by prefix if provided
            topic_candidates = []
            for topic in all_topics:
                if prefix and not topic.startswith(prefix):
                    continue
                topic_candidates.append(topic)

            # Get message counts for each topic
            from aiokafka import AIOKafkaConsumer
            consumer = AIOKafkaConsumer(
                bootstrap_servers=getattr(settings, "events_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                client_id="stufio-admin-cleanup"
            )
            await consumer.start()

            empty_topics = []

            for topic in topic_candidates:
                # Skip internal topics
                if topic.startswith("__"):
                    continue

                # Get topic metadata
                partitions = await consumer.partitions_for_topic(topic)
                if partitions is None:
                    continue

                # Check if the topic is empty
                is_empty = True
                for partition in partitions:
                    tp = TopicPartition(topic, partition)
                    beginning_offset = await consumer.beginning_offsets([tp])
                    end_offset = await consumer.end_offsets([tp])

                    begin = beginning_offset.get(tp, 0)
                    end = end_offset.get(tp, 0)

                    if end > begin:
                        is_empty = False
                        break

                if is_empty:
                    empty_topics.append(topic)

            await consumer.stop()

            # Delete empty topics
            deleted_topics = []
            if empty_topics:
                await admin_client.delete_topics(empty_topics)
                deleted_topics = empty_topics

            return CleanupResponse(
                status="success",
                empty_topics_found=len(empty_topics),
                deleted_topics=deleted_topics
            )
        except Exception as e:
            logger.error(f"Failed to cleanup empty topics: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            await self.close_client()

    async def get_topics_health(self) -> TopicHealthResponse:
        """Get health status of topics."""
        try:
            topics = await self.list_topics()
            health_results = {}
            for topic in topics:
                # Check if all partitions have leaders
                missing_leaders = [p for p in topic.partitions if p.leader < 0]
                if missing_leaders:
                    health_results[topic.name] = "unhealthy"
                else:
                    health_results[topic.name] = "healthy"
            
            return TopicHealthResponse(topics=health_results)
        except Exception as e:
            logger.error(f"Failed to get topics health: {e}")
            return {"error": str(e)}

    def _safe_get(self, obj: Any, keys: List[str], default: Any = None) -> Any:
        """Safely get a value from nested dictionary or object attributes."""
        if obj is None:
            return default
            
        # For dictionaries with potentially different key names
        if isinstance(obj, dict):
            # Try common field name variations
            for key in keys:
                if key in obj:
                    return obj[key]
            return default
            
        # For objects with attributes
        for key in keys:
            if hasattr(obj, key):
                return getattr(obj, key)
        return default

# Create singleton instance
crud_kafka_topics = CRUDKafkaTopics()
