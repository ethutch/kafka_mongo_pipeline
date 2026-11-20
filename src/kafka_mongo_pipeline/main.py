"""Application entry point — wires dependencies and starts the multi-topic consumer."""
from __future__ import annotations

import base64
import json
import os
import signal
import sys
from signal import Signals
from types import FrameType

import boto3
from kafka import KafkaConsumer  # python-kafka-ng

from kafka_mongo_pipeline.config.settings import get_settings
from kafka_mongo_pipeline.models import (
    CustomerEvent,
    ESMKafkaEvent,
    ESMKafkaRecord,
    OrderEvent,
)
from kafka_mongo_pipeline.services import (
    CustomerRepository,
    EventDeserializer,
    MongoManager,
    OrderRepository,
    TopicProducer,
)
from kafka_mongo_pipeline.utils import configure_logging, get_logger

log = get_logger(__name__)

# Check if we are in debug mode
if os.environ.get("DEBUG") == "true":
    import debugpy
    # Listen on all interfaces inside the container on port 5678
    debugpy.listen(("0.0.0.0", 5678))
    print("⏳ Waiting for debugger attach on port 5678...", file=sys.stderr)
    
    # Optional: Pause execution until VS Code attaches
    #debugpy.wait_for_client() 
    
    # Optional: Force a breakpoint right after attachment
    # debugpy.breakpoint() 

_running = False

def run_pipeline() -> None:
    global _running
    _running = True 

    settings = get_settings()

    if sys.stdout.isatty():
        pass  # interactive terminal, skip JSON
    else:
        configure_logging()  # container/aggregator, use JSON
    
    log.info("pipeline.initializing", version="2026.1.1")

    log.info("Lamda URL: {setings.aws_services}")
    lambda_client = boto3.client(
    "lambda",
    endpoint_url=settings.aws_services,
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    )

    log.info("Lambda Client: {lambda_client}")
    
    _register_signal_handlers()  

    topic_producer= TopicProducer(settings)
    mongo_manager = MongoManager(settings.mongo)
    
    customer_repository = CustomerRepository(mongo_manager)
    order_repository    = OrderRepository(mongo_manager)
    event_deserializer  = EventDeserializer()

    log.info(f"Topics to consume: {settings.kafka.topics}")
    kafka_consumer = KafkaConsumer(
        *settings.kafka.topics,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.group_id,
        enable_auto_commit=False, # Manual commits only after successful DB writes
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,  # Check for cancel 5 seconds
    )

    log.info(f"pipeline.listening subscribed_topics={settings.kafka.topics}")
    
    try:
        while _running:        
            for message in kafka_consumer:
                log.debug("Processing message", topic=message.topic, offset=message.offset)
                if not _running:
                    log.info("pipeline.shutdown_initiated")
                    kafka_consumer.rollback()  # type: ignore
                    break  
                result = event_deserializer.process(message)
                log.debug(
                    "Event deserialized", 
                    result_type=type(result).__name__, 
                    message=result.__repr__())
                mongo_save_complete = False
                if not result or not result.is_success():
                    topic_producer.publish(message) # type: ignore
                    log.warn("deserialization.failed", topic=message.topic, offset=message.offset)
                    continue

                if isinstance(result, CustomerEvent):
                    mongo_save_complete = customer_repository.upsert_customer(
                        result.to_mongo_doc()
                    )
                elif isinstance(result, OrderEvent): 
                    mongo_save_complete = order_repository.insert_order(result.to_mongo_doc())
                
                if mongo_save_complete:
                    kafka_consumer.commit()
                else:
                    log.error("pipeline.persistence_failed", offset=message.offset)
                log.info("CREATING ESMKafkaRecord")
                # Now simulate AWS ESM that would also be a topic consumer for dynamodb writes
                record = ESMKafkaRecord(
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                    timestamp=message.timestamp,
                    timestamp_type={
                        0: "CREATE_TIME",
                        1: "LOG_APPEND_TIME",
                    }.get(message.timestamp_type, "NO_TIMESTAMP_TYPE"),
                    key=(
                        base64.b64encode(message.key).decode("ascii")
                        if message.key is not None
                        else None
                    ), 
                    value=base64.b64encode(message.value).decode("ascii"),
                    )
                event = ESMKafkaEvent(
                    eventSource="self.managed.kafka",
                    bootstrapServers=settings.kafka.bootstrap_servers,
                    records={
                    f"{message.topic}-{message.partition}": [record],
                    },
                )
                payload = json.dumps(event.model_dump(by_alias=True, mode="json"))

                log.info("ESMKafkaEvent: {event}")
                log.info("CALLING LAMBDA")
                response = lambda_client.invoke(
                    FunctionName="kafka-dynamodb-pipeline",
                    InvocationType="RequestResponse",
                    Payload=payload.encode("utf-8"),
                )
                log.info("LAMDA RETURNED")
                log.info(json.dumps(json.loads(response["Payload"].read()), indent=2))
                
    except StopIteration:
        pass
    except Exception as e:
        log.critical("pipeline.unexpected_error", error=str(e), exc_info=True)
    finally:
        topic_producer.close()
        kafka_consumer.close()
        mongo_manager.close()
        log.info("pipeline.stopped")


def _register_signal_handlers() -> None:
    """Handle SIGINT / SIGTERM for graceful shutdown."""
    signal.signal(signal.SIGINT, _handle_shutdown_signal) # type: ignore
    signal.signal(signal.SIGTERM, _handle_shutdown_signal) # type: ignore

def _handle_shutdown_signal(signum: int, frame: FrameType | None) -> None:
    global _running
    log.info("shutdown_signal_received", signal=Signals(signum).name)
    _running = False


def main() -> None:
    try:
        run_pipeline()
    except Exception:
        # Fallback to standard logging if startup fails before logger is ready
        log.critical("Startup failed", exc_info=True)
        sys.exit(1)
        

if __name__ == "__main__":
    main()