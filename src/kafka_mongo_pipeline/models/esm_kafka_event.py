from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class ESMKafkaRecord(BaseModel):
    """One record in an AWS Lambda Kafka ESM event."""
      
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
        extra="allow",
    ) 

    topic: str
    partition: int
    offset: int
    timestamp: int
    timestamp_type: str 

    key: str | None = None
    value: str
    headers: list[dict[str, list[int]]] = Field(default_factory=list)


class ESMKafkaEvent(BaseModel):
    """AWS Lambda Self-Managed Kafka event envelope."""

    model_config = ConfigDict(extra="allow")

    event_source: str = Field(
        alias="eventSource"
    )
    bootstrap_servers: str = Field(alias="bootstrapServers")
    records: dict[str, list[ESMKafkaRecord]]
