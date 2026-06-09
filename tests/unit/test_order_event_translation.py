from datetime import datetime, timezone

import pytest

from models import OrderDetail, OrderEvent


@pytest.fixture
def us_order_event() -> OrderEvent:
    """Provides a hydrated, US-based OrderEvent with nested details for testing."""
    return OrderEvent(
        kafka_topic="order-events",
        kafka_partition=0,
        kafka_offset=5000,
        ingested_at=datetime(2026, 6, 7, 17, 52, 52, 88423, tzinfo=timezone.utc),
        correlation_id="3dde64c1-ad87-47eb-85bc-ad19ae796ce0",
        action="CREATE",
        order_id=22,
        customer_id="WIDGET_INC",
        employee_id=5,
        order_date="2026-05-07",
        required_date="2026-05-01",
        shipped_date="2026-05-05",
        freight=32.38,
        ship_name="Widget Corp Shipping",
        ship_address="456 Industrial Way",
        ship_city="Denver",
        ship_region="CO",  # Proper US State
        ship_postal_code="80202",
        ship_country="USA",
        order_details=[
            OrderDetail(product_id=72, unit_price=34.8, quantity=5, discount=0.0),
            OrderDetail(product_id=42, unit_price=9.8, quantity=10, discount=0.0),
            OrderDetail(product_id=11, unit_price=14.0, quantity=12, discount=0.0)
        ],
        created_by="alice",
        updated_by="alice",
        created_at=datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc)
    )

def test_order_event_to_mongo_doc_exact_output(us_order_event: OrderEvent) -> None:
    """
    Proves the base class transformation successfully remaps keys, isolates
    Kafka tracking data, and deeply serializes nested OrderDetail objects.
    """
    # 1. Define the exact, uncompromising dictionary Mongo should receive
    expected_mongo_document = {
        "_id": "3dde64c1-ad87-47eb-85bc-ad19ae796ce0",
        "action": "CREATE",
        "order_id": 22,
        "customer_id": "WIDGET_INC",
        "employee_id": 5,
        "order_date": "2026-05-07",
        "required_date": "2026-05-01",
        "shipped_date": "2026-05-05",
        "freight": 32.38,
        "ship_name": "Widget Corp Shipping",
        "ship_address": "456 Industrial Way",
        "ship_city": "Denver",
        "ship_region": "CO",
        "ship_postal_code": "80202",
        "ship_country": "USA",
        # Notice that these are standard dicts now, not OrderDetail objects
        "order_details": [
            {"product_id": 72, "unit_price": 34.8, "quantity": 5, "discount": 0.0},
            {"product_id": 42, "unit_price": 9.8, "quantity": 10, "discount": 0.0},
            {"product_id": 11, "unit_price": 14.0, "quantity": 12, "discount": 0.0}
        ],
        "created_by": "alice",
        "updated_by": "alice",
        "created_at": datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc),
        "_meta": {
            "topic": "order-events",
            "partition": 0,
            "offset": 5000,
            "ingested_at": datetime(2026, 6, 7, 17, 52, 52, 88423, tzinfo=timezone.utc),
        }
    }

    # 2. Fire the transformation method
    actual_document = us_order_event.to_mongo_doc()

    # 3. Assert a 1:1 match.
    assert expected_mongo_document == actual_document