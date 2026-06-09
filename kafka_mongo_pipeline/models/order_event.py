from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from models.processing_result import ProcessingResult


def ms_to_datetime(epoch_ms: int) -> datetime | None:
    if not epoch_ms:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=UTC)


if TYPE_CHECKING:
    from proto.order_event_pb2 import OrderDetailMessage, OrderEventMessage

class OrderDetail(BaseModel):
    """Nested domain representation of an itemized order line."""
    product_id: int
    unit_price: float
    quantity: int
    discount: float

    @classmethod
    def from_proto(cls, proto_detail: OrderDetailMessage) -> OrderDetail:
        return cls(
            product_id=proto_detail.product_id,
            unit_price=proto_detail.unit_price,
            quantity=proto_detail.quantity,
            discount=proto_detail.discount,
        )

class OrderEvent(ProcessingResult):
    """Domain wrapper for OrderEventMessage with Kafka and Mongo capabilities."""
    correlation_id: str
    action: str
    order_id: int
    customer_id: str
    employee_id: int
    order_date: str
    required_date: str
    shipped_date: str
    freight: float
    ship_name: str
    ship_address: str
    ship_city: str
    ship_region: str
    ship_postal_code: str
    ship_country: str
    order_details: list[OrderDetail]
    created_by: str
    updated_by: str
    created_at: datetime | None
    updated_at: datetime | None


    def is_success(self) -> bool:
        return True

    def get_event_type(self) -> str:
        return "ORDER"
    
    