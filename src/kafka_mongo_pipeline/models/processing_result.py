
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class ProcessingResult(BaseModel):
    """Interface for all processing outcomes"""
    
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    ingested_at: datetime = Field(
        alias="ingestedAt",
        default_factory=lambda: datetime.now(tz=UTC)
    )  # type: ignore

    def is_success(self) -> bool:
        # Enforces that every subclass must implement this explicitly
        raise NotImplementedError("Subclasses must implement is_success()")

    def get_event_type(self) -> str:
        raise NotImplementedError("Subclasses must implement get_event_type()")

    def to_mongo_doc(self) -> dict[str, Any]:
        """Serialize the event using aliases and bundle Kafka metadata."""
        document = self.model_dump(by_alias=True)

        document["_meta"] = {
            "topic": document.pop("kafkaTopic"),
            "partition": document.pop("kafkaPartition"),
            "offset": document.pop("kafkaOffset"),
            "ingestedAt": document.pop("ingestedAt"),
        }

        document["_id"] = document.pop("correlationId")
        return document