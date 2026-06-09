from datetime import datetime, timezone

import pytest

from models import CustomerEvent, ProcessingResult


@pytest.fixture
def us_customer_event() -> CustomerEvent:
    """Provides a hydrated, US-based CustomerEvent for testing."""
    return CustomerEvent(
        kafka_topic="customer-events",
        kafka_partition=0,
        kafka_offset=5000,
        ingested_at=datetime(2026, 6, 7, 14, 13, 13, 681735, tzinfo=timezone.utc),
        correlation_id="3dde64c1-ad87-47eb-85bc-ad19ae796ce0",
        action="CREATE",
        customer_id="ACME_001",
        company_name="Acme Corporation",
        contact_name="Wile E. Coyote",
        contact_title="Director of Engineering",
        address="123 Desert Road",
        city="Flagstaff",
        region="AZ",  # Proper US State
        postal_code="86001",
        country="USA",
        phone="(928) 555-1234", # Proper US Phone
        fax="(928) 555-4321",
        created_by="system_admin",
        updated_by="system_admin",
        created_at=datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc),
        updated_at=datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc)
    )


def test_to_mongo_doc_exact_output(us_customer_event: CustomerEvent) -> None:
    """
    Proves that the dictionary surgery performed by the base class
    outputs the EXACT expected structure, missing absolutely nothing.
    """
    # 1. Define the exact, uncompromising dictionary Mongo should receive
    expected_mongo_document = {
        "_id": "3dde64c1-ad87-47eb-85bc-ad19ae796ce0",
        "action": "CREATE",
        "customer_id": "ACME_001",
        "company_name": "Acme Corporation",
        "contact_name": "Wile E. Coyote",
        "contact_title": "Director of Engineering",
        "address": "123 Desert Road",
        "city": "Flagstaff",
        "region": "AZ",
        "postal_code": "86001",
        "country": "USA",
        "phone": "(928) 555-1234",
        "fax": "(928) 555-4321",
        "created_by": "system_admin",
        "updated_by": "system_admin",
        "created_at": datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc),
        "_meta": {
            "topic": "customer-events",
            "partition": 0,
            "offset": 5000,
            "ingested_at": datetime(2026, 6, 7, 14, 13, 13, 681735, tzinfo=timezone.utc),
        }
    }

    # 2. Fire the transformation method
    actual_document = us_customer_event.to_mongo_doc()

    # 3. Assert a 1:1 match using Pytest's native assert.
    # Pytest's assertion rewriting will automatically show a detailed dict diff if this fails.
    assert expected_mongo_document == actual_document