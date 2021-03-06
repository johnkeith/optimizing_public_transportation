"""Producer base-class providing common utilites and functionality"""
import logging
import time
from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
GROUP_ID = "CTA"

logger = logging.getLogger(__name__)

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "group.id": GROUP_ID,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        self.admin_client = AdminClient({ "bootstrap.servers": BROKER_URL })

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        if not self.topic_exists():
            futures = self.admin_client.create_topics([
                NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas,
                config={
                    "cleanup.policy": "compact",
                    "compression.type": "lz4",
                    "delete.retention.ms": 2000,
                    "file.delete.delay.ms": 2000
                }
                )
            ])

            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"succeeded in creating topic {self.topic_name}")
                except Exception as e:
                    print(f"failed to create topic {self.topic_name}: {e}")
                    raise

    def topic_exists(self):
        metadata = self.admin_client.list_topics(timeout=5)
        return metadata.topics.get(self.topic_name) is not None

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # TODO - make sure this is all we need to ensure it is closed correctly
        self.producer.flush(10000)

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
