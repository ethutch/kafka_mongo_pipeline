from __future__ import annotations

import struct
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, NamedTuple

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from kafka.consumer.fetcher import ConsumerRecord

from kafka_mongo_pipeline.config.settings import get_settings
from kafka_mongo_pipeline.models import CustomerEvent, DeadLetterEvent, OrderEvent, ProcessingResult
from kafka_mongo_pipeline.proto import customer_event_pb2, order_event_pb2
from kafka_mongo_pipeline.utils import get_logger

log = get_logger(__name__)

# Apicurio 2.x magic byte that starts every serialised message
_APICURIO_MAGIC_BYTE = 0x00
_APICURIO_HEADER_LENGTH = 9 
_APICURIO_MIN_MESSAGE_LENGTH = 10

class EventDeserializer:
    def __init__(self) -> None:
        settings = get_settings()
        
        self.supported_customer_events = settings.supported_customer_events
        self.supported_order_events = settings.supported_order_events
        log.info("ApicurioProcessor initialized",
                    supported_customer_events=self.supported_customer_events,
                    supported_order_events=self.supported_order_events)
        
        # Bind the class constructors directly to self so Python avoids module lookups in the loop
        self._customer_proto_factory = customer_event_pb2.CustomerEventMessage
        self._order_proto_factory = order_event_pb2.OrderEventMessage


    def process(self, message: ConsumerRecord) -> ProcessingResult: # type: ignore
         # First strip off the apicurio header
        try:
            log.debug("Process message", topic=message.topic, payload=message.value)
            payload = self._strip_apicurio_header(message.value) 
        except ValueError as e:
            log.warning("Failed to strip Apicurio header.", error=str(e))
            return self._create_dead_letter_event(
                message=message,
                error_type="apicurio_parsing_error",
                error_message=str(e),
            )
        
        # Handle Customer Events
        if message.topic == "customer-events": # type: ignore
            if (self.supported_customer_events is not None and
                 payload.schema_id in self.supported_customer_events):
                try:
                    return self._deserialize_and_build_event(
                        message=message,
                        payload=payload,
                        proto_factory=self._customer_proto_factory,  # Passes the pre-bound factory
                        target_event_class=CustomerEvent
                    )
                except Exception as e:
                    log.warning(
                        "Protobuf failed to deserialize CustomerEventMessage", 
                        error=str(e))
                    return self._create_dead_letter_event(
                        message=message,
                        error_type="protobuf_deserialization_error",
                        error_message=f"Protobuf decoding failed: {str(e)}",
                    )
            else:        
                log.warning(
                    f"Unsupported schema ID:{payload.schema_id} for topic:{message.topic}"
                )
                return self._create_dead_letter_event(
                    message=message,
                    error_type="unsupported_event_type",
                    error_message=(f"Unsupported schema ID: {payload.schema_id} for topic: "
                                   f"{message.topic}"),
                )
        
        # Handle Order Events
        elif message.topic == "order-events":
            if (self.supported_order_events is not None and
                 payload.schema_id in self.supported_order_events):
                try:
                    return self._deserialize_and_build_event(
                        message=message,
                        payload=payload,
                        proto_factory=self._order_proto_factory,  # Passes the pre-bound factory 
                        target_event_class=OrderEvent
                    )
                except Exception as e:
                    log.warning("Protobuf failed to deserialize OrderEventMessage", error=str(e))
                    return self._create_dead_letter_event(
                        message=message,
                        error_type="protobuf_deserialization_error",
                        error_message=f"Protobuf decoding failed: {str(e)}",
                    )
            else:        
                log.warning(
                    f"Unsupported schema ID:{payload.schema_id} for topic:{message.topic}"
                )
                return self._create_dead_letter_event(
                    message=message,
                    error_type="unsupported_event_type",
                    error_message=(f"Unsupported schema ID: {payload.schema_id} for topic: "
                                   f"{message.topic}"),
                )
        # No telling what we got.
        else:
            log.warning(f"Received message for unsupported topic: {message.topic}")
            return self._create_dead_letter_event(
                    message=message,
                    error_type="unsupported_topic",
                    error_message=(f"Received message for unsupported topic: {message.topic}"),
                )


    def _deserialize_and_build_event(
        self,
        message: ConsumerRecord,
        payload: Payload,
        proto_factory: Callable[[], Message],
        target_event_class: type[ProcessingResult]
    ) -> ProcessingResult:
        """
        Dynamically decodes a protobuf payload and constructs the matching Pydantic domain event.
        Falls back to a Dead Letter Event automatically on failure.
        Args:
            message: The original Kafka ConsumerRecord containing topic/partition/offset metadat.
            payload: The raw protobuf bytes from the message after stripping the Apicurio header.
            proto_factory: Callable returning a new instance of the correct Protobuf message class.
            target_event_class: The Pydantic domain model class to instantiate on successful
            decoding (e.g., CustomerEvent)."""
    
        # Dynamically grab the class name for clean error logging
        class_name = target_event_class.__name__
    
        try:
            # 1. Instantiate the correct Protobuf container via passed factory
            proto_instance: Message = proto_factory()
            proto_instance.ParseFromString(payload.message)

            # 2. Convert to standard dict
            event_dict: dict[str, Any] = MessageToDict(
                proto_instance,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True # type: ignore
            )

            # 3. Inject shared infrastructure metadata
            event_dict["kafka_topic"] = message.topic
            event_dict["kafka_partition"] = message.partition
            event_dict["kafka_offset"] = message.offset
            event_dict["ingested_at"] = datetime.now(tz=UTC)

            # 4. Instantiate the passed Pydantic domain class (e.g., CustomerEvent)
            return target_event_class(**event_dict)

        except Exception as e:
            log.warning(f"Protobuf failed to deserialize {class_name}", error=str(e))
            return self._create_dead_letter_event(
                message=message,
                error_type="protobuf_deserialization_error",
                error_message=f"Protobuf decoding failed for {class_name}: {str(e)}",
            )



    @staticmethod
    def _create_dead_letter_event(message: ConsumerRecord, error_type: str, 
                                  error_message: str) -> DeadLetterEvent:
        return DeadLetterEvent(
            kafka_topic=message.topic,
            kafka_partition=message.partition,
            kafka_offset=message.offset,
            raw_value=message.value,
            error_type=error_type,
            error_message=error_message,
        )
    
    @staticmethod
    def _strip_apicurio_header(value: bytes) -> Payload:
        """
        Validate and strip the Apicurio 2.x wire format header.

        Returns:
            Payload(proto_bytes, schema_id)

        Raises:
            ValueError if the magic byte is missing (message not Apicurio-encoded)
        """
        if len(value) < _APICURIO_MIN_MESSAGE_LENGTH:
            raise ValueError(
                f"Message too short to contain Apicurio header: {len(value)} bytes"
            )
        
        magic = value[0]
        if magic != _APICURIO_MAGIC_BYTE:
            raise ValueError(
                f"Missing Apicurio magic byte — expected 0x00, got {hex(magic)}. "
                f"Message may not be Apicurio-encoded."
            )
        schema_id: int = struct.unpack(">Q", value[1:9])[0]
        proto_bytes = value[_APICURIO_HEADER_LENGTH:]
        return Payload(schema_id=schema_id, message=proto_bytes)

    
class Payload(NamedTuple):
    schema_id: int
    message: bytes

