from unittest.mock import MagicMock

import pytest
from kafka.consumer.fetcher import ConsumerRecord

from kafka_mongo_pipeline.models.customer_event import CustomerEvent
from kafka_mongo_pipeline.models.order_event import OrderEvent

# Fixed: Using your correct absolute import path
from kafka_mongo_pipeline.services.event_deserializer import EventDeserializer

# Raw wire data hex strings parsed cleanly into binary arrays
ORDER_HEX = "".join('''
0000 0000 0000 0000 070a 2433 6464 6536  
3463 312d 6164 3837 2d34 3765 622d 3835  
6263 2d61 6431 3961 6537 3936 6365 3012 
0643 5245 4154 4518 1622 0556 494e 4554
2805 320a 3230 3236 2d30 352d 3037 3a0a 
3230 3236 2d30 352d 3031 420a 3230 3236
2d30 352d 3035 4d1f 8501 4252 1956 696e 
7320 6574 2061 6c63 6f6f 6c73 2043 6865
7661 6c69 6572 5a12 3539 2072 7565 2064  
6520 6c27 4162 6261 7965 6205 5265 696d 
7372 0535 3131 3030 7a06 4672 616e 6365
8201 0908 4815 3333 0b42 1805 8201 0908 
2a15 cdcc 1c41 180a 8201 0908 0b15 0000
6041 180c 8a01 0561 6c69 6365 9201 0561 
6c69 6365 9801 b0dd adf4 e633 a001 b0dd 
adf4 e633                              
'''.split())

CUSTOMER_HEX = "".join('''
 0000 0000 0000 0000 090a 2439 3236 6439  
 6531 312d 6638 3161 2d34 3162 322d 6236  
 3432 2d65 3363 6563 3837 3364 6363 3512  
 0655 5044 4154 451a 044f 5243 4c22 0a4f  
 7261 636c 6520 496e 632a 0d4c 6172 7279  
 2045 6c6c 6973 6f6e 3203 4345 4f3a 0c31  
 3233 3420 5061 726b 2041 5642 0c53 616e  
 2046 7261 6e73 6963 6f4a 0a43 616c 6966  
 6f72 6e69 6152 0539 3032 3031 620e 312d  
 3230 352d 3934 342d 3132 3334 6a0e 312d  
 3230 342d 3934 342d 3938 3736 7206 5379  
 7374 656d 7a05 616c 6963 6588 0189 f1fb  
 bbe7 33                                                      
'''.split())

ORDER_BYTES = bytes.fromhex(ORDER_HEX)
CUSTOMER_BYTES = bytes.fromhex(CUSTOMER_HEX)


@pytest.fixture
def deserializer() -> EventDeserializer:
    """Instantiate the EventDeserializer under test."""
    return EventDeserializer()


@pytest.fixture
def mock_record() -> MagicMock:
    """Mock an incoming Kafka ConsumerRecord wrapper object."""
    record = MagicMock(spec=ConsumerRecord)
    record.topic = "order-events"
    record.partition = 0
    record.offset = 5000
    record.value = b""
    return record


def test_order_event_parsing(deserializer: EventDeserializer, mock_record: MagicMock) -> None:
    mock_record.topic = "order-events"
    mock_record.value = ORDER_BYTES

    result = deserializer.process(mock_record)

    assert isinstance(result, OrderEvent)
    assert result.is_success() is True
    assert result.action == "CREATE"


def test_customer_event_parsing(deserializer: EventDeserializer, mock_record: MagicMock) -> None:
    mock_record.topic = "customer-events"
    mock_record.value = CUSTOMER_BYTES

    result = deserializer.process(mock_record)

    assert isinstance(result, CustomerEvent)
    assert result.is_success() is True
    assert result.action == "UPDATE"