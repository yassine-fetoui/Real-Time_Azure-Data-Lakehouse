"""
Custom Airflow sensor that blocks until Kafka consumer group lag
falls below a configurable threshold.

Solves the pipeline trigger timing problem:
  - Don't start dbt/Snowflake transforms until streaming has caught up
  - Configurable max_lag threshold per use case
"""

from __future__ import annotations

from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, KafkaException

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class KafkaLagSensor(BaseSensorOperator):
    """
    Pokes Kafka until the consumer group lag for a given topic
    is at or below `max_lag`.

    :param bootstrap_servers: Kafka bootstrap server string
    :param consumer_group:    Consumer group ID to monitor
    :param topic:             Kafka topic name
    :param max_lag:           Maximum tolerable lag (messages). Default: 10,000
    """

    ui_color = "#f0ad4e"
    template_fields = ("bootstrap_servers", "consumer_group", "topic")

    @apply_defaults
    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        topic: str,
        max_lag: int = 10_000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.topic = topic
        self.max_lag = max_lag

    def poke(self, context) -> bool:
        try:
            lag = self._get_consumer_lag()
            self.log.info(
                f"Consumer group '{self.consumer_group}' lag on '{self.topic}': "
                f"{lag:,} messages (threshold: {self.max_lag:,})"
            )
            if lag <= self.max_lag:
                self.log.info("✅ Lag within threshold. Proceeding.")
                return True
            self.log.info(f"⏳ Lag too high ({lag:,}). Waiting...")
            return False
        except KafkaException as e:
            self.log.warning(f"Kafka error during lag check: {e}. Retrying...")
            return False

    def _get_consumer_lag(self) -> int:
        """Compute total lag across all partitions for the consumer group."""
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group,
        })

        # Get committed offsets for all partitions
        metadata = admin.list_topics(self.topic, timeout=10)
        partitions = [
            TopicPartition(self.topic, p)
            for p in metadata.topics[self.topic].partitions
        ]

        committed = consumer.committed(partitions, timeout=10)
        total_lag = 0

        for tp in committed:
            # Get high-water mark (end offset)
            _, high_watermark = consumer.get_watermark_offsets(tp, timeout=10)
            committed_offset = tp.offset if tp.offset >= 0 else 0
            total_lag += max(0, high_watermark - committed_offset)

        consumer.close()
        return total_lag
