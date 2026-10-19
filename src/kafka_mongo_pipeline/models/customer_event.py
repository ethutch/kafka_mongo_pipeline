from __future__ import annotations

from datetime import datetime

from pydantic import ConfigDict, Field
from pydantic.alias_generators import to_camel

from kafka_mongo_pipeline.models.processing_result import ProcessingResult


class CustomerEvent(ProcessingResult):
    model_config = ConfigDict(
           populate_by_name=True,
           alias_generator=to_camel,
       ) 
    correlation_id: str = Field(
        default="", 
        description="Unique identifier for the event"
    )
    action: str = Field(
        default="", 
        description="CRUD Action to be performed, e.g., 'CREATE', 'UPDATE', 'DELETE'"
    )
    customer_id: str = Field(
        default="",
        description="Unique identifier for the customer, used as the partition key in DynamoDB",
    )
    company_name: str = Field(
        default="", 
        description="Name of the customer's company"
    )
    contact_name: str | None = Field(
        default=None, 
        description="Name of the customer's contact person"
    )
    contact_title: str | None = Field(
        default=None, 
        description="Title of the customer's contact person"
    )
    address: str | None = Field(
        default=None, 
        description="Customer's address")
    city: str | None = Field(
        default=None, 
        description="Customer's city")
    region: str | None = Field(
        default=None, 
        description="Customer's region or state")
    postal_code: str | None = Field(
        default=None, 
        description="Customer's postal code")
    country: str | None = Field(
        default=None, 
        description="Customer's country")
    phone: str | None = Field(
        default=None, 
        description="Customer's phone number")
    fax: str | None = Field(
        default=None, 
        description="Customer's fax number")
    created_by: str | None = Field(
        default=None, 
        description="User who created the customer record")
    updated_by: str | None = Field(
        default=None, description="User who updated the customer record")
    created_at: datetime | None = Field(
        default=None, 
        description="Timestamp of when the customer record was created"
    )
    updated_at: datetime | None = Field(
        default=None, 
        description="Timestamp of when the customer record was last updated"
    )

    def is_success(self) -> bool:
        return True

    def get_event_type(self) -> str:
        return "CUSTOMER"
