"""Application entry point — wires dependencies and starts the multi-topic consumer."""
from __future__ import annotations

import signal
import sys
from signal import Signals
from types import FrameType

from kafka import KafkaConsumer  # python-kafka-ng

from config.settings import get_settings
from models import CustomerEvent, OrderEvent
from services import (
    CustomerRepository,
    EventDeserializer,
    MongoManager,
    OrderRepository,
    TopicProducer,
)
from utils import configure_logging, get_logger

log = get_logger(__name__)

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
                log.debug("Event deserialized", result_type=type(result).__name__, message=result.__repr__())
                mongo_save_complete = False
                if not result or not result.is_success:
                    topic_producer.publish(message)
                    log.warn("deserialization.failed", topic=message.topic, offset=message.offset)
                    continue

                if isinstance(result, CustomerEvent):
                     mongo_save_complete = customer_repository.upsert_customer(result.to_mongo_doc())
                elif isinstance(result, OrderEvent): 
                    mongo_save_complete = order_repository.insert_order(result.to_mongo_doc())
                
                if mongo_save_complete:
                    kafka_consumer.commit()
                else:
                    log.error("pipeline.persistence_failed", offset=message.offset)
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
    except Exception as exc:
        # Fallback to standard logging if startup fails before logger is ready
        log.critical("Startup failed", exc_info=True)
        sys.exit(1)
        

if __name__ == "__main__":
    main()