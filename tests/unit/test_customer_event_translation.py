from datetime import UTC, datetime, timezone

import pytest

from kafka_mongo_pipeline.models import CustomerEvent


@pytest.fixture
def us_customer_event() -> CustomerEvent:
    """Provides a hydrated, US-based CustomerEvent for testing."""
    return CustomerEvent(
        kafka_topic="customer-events",
        kafka_partition=0,
        kafka_offset=5000,
        correlation_id="3dde64c1-ad87-47eb-85bc-ad19ae796ce0",
        ingested_at=datetime(2026, 6, 7, 17, 52, 52, 88423, tzinfo=timezone.utc),
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
        created_at=datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=UTC),
        updated_at=datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=UTC)
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
        "customerId": "ACME_001",
        "companyName": "Acme Corporation",
        "contactName": "Wile E. Coyote",
        "contactTitle": "Director of Engineering",
        "address": "123 Desert Road",
        "city": "Flagstaff",
        "region": "AZ",
        "postalCode": "86001",
        "country": "USA",
        "phone": "(928) 555-1234",
        "fax": "(928) 555-4321",
        "createdBy": "system_admin",
        "updatedBy": "system_admin",
        "createdAt": datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc),
        "updatedAt": datetime(2026, 5, 28, 12, 24, 52, 144000, tzinfo=timezone.utc),
        "_meta": {
            "topic": "customer-events",
            "ingestedAt": datetime(2026, 6, 7, 17, 52, 52, 88423, tzinfo=timezone.utc),
        }
    }

    # 2. Fire the transformation method
    actual_document = us_customer_event.to_mongo_doc()

    # 3. Assert a 1:1 match using Pytest's native assert.
    # Pytest's assertion rewriting will automatically show a detailed dict diff if this fails.
    assert expected_mongo_document == actual_document