import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.producer.future import FutureRecordMetadata

from kafka_mongo_pipeline.config import AppSettings
from kafka_mongo_pipeline.models import DeadLetterEvent

log = structlog.get_logger()


class TopicProducer:

    def __init__(self, settings: AppSettings) -> None:
        kafka = settings.kafka
        config: dict = {
            "bootstrap_servers": kafka.bootstrap_servers,
            "security_protocol": kafka.security_protocol,
            "acks": "all",
        }
        if kafka.sasl_mechanism:
            config["sasl_mechanism"] = kafka.sasl_mechanism
        if kafka.sasl_username:
            config["sasl_plain_username"] = kafka.sasl_username
        if kafka.sasl_password:
            config["sasl_plain_password"] = kafka.sasl_password

        self._topic = settings.dead_letter_topic
        self._producer = KafkaProducer(**config)

    def publish(self, event: DeadLetterEvent) -> None:
        headers = [
            ("error_type",    event.error_type.encode("utf-8")),
            ("error_message", event.error_message.encode("utf-8")),
            ("source_topic",  event.kafka_topic.encode("utf-8")),
            ("source_partition", str(event.kafka_partition).encode("utf-8")),
            ("source_offset", str(event.kafka_offset).encode("utf-8")),
            ("ingested_at",   event.ingested_at.isoformat().encode("utf-8")),
        ]
        try:
            future: FutureRecordMetadata = self._producer.send(
                self._topic,
                value=event.raw_value,
                headers=headers,
            )
            self._producer.flush()
            future.get(timeout=10)
            log.info(
                "dlq.published",
                source_topic=event.kafka_topic,
                source_offset=event.kafka_offset,
                error_type=event.error_type,
            )
        except KafkaError:
            log.exception(
                "dlq.publish_failed",
                source_topic=event.kafka_topic,
                source_offset=event.kafka_offset,
            )
            raise

    def close(self) -> None:
        self._producer.close()