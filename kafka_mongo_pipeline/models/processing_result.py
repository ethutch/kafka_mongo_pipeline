
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class ProcessingResult(BaseModel):
    """Interface for all processing outcomes"""
    
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    ingested_at: datetime = Field( # type: ignore
        default_factory=lambda: datetime.now(tz=UTC)
    ) 

    def is_success(self) -> bool:
        # Enforces that every subclass must implement this explicitly
        raise NotImplementedError("Subclasses must implement is_success()")

    def get_event_type(self) -> str:
        raise NotImplementedError("Subclasses must implement get_event_type()")

    def to_mongo_doc(self) -> dict[str, Any]:
        """
        Dynamically extracts child attributes, maps primary keys, 
        and bundles Kafka tracking data into a structured metadata block.
        """
        # 1. Automatically dumps ALL fields from the inheriting child class
        document = self.model_dump()
        
        # 2. Extract and bundle the tracking attributes
        document["_meta"] = {
            "topic": document.pop("kafka_topic"),
            "partition": document.pop("kafka_partition"),
            "offset": document.pop("kafka_offset"),
            "ingested_at": document.pop("ingested_at"),
        }
        
        # 3. Handle primary key assignment
        # Using pop guarantees correlation_id doesn't remain duplicated at the root
        document["_id"] = document.pop("correlation_id")
        
        return document