from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from models.processing_result import ProcessingResult

if TYPE_CHECKING:
    from proto.customer_event_pb2 import CustomerEventMessage

class CustomerEvent(ProcessingResult):

    correlation_id: str
    action: str
    customer_id: str
    company_name: str
    contact_name: str
    contact_title: str
    address: str
    city: str
    region: str
    postal_code: str
    country: str
    phone: str
    fax: str
    created_by: str
    updated_by: str
    created_at: datetime | None
    updated_at: datetime | None

    def is_success(self) -> bool:
        return True

    def get_event_type(self) -> str:
        return "CUSTOMER"

    