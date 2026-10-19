from kafka_mongo_pipeline.proto import customer_event_pb2, order_event_pb2

CustomerEventMessage = customer_event_pb2.CustomerEventMessage
OrderEventMessage = order_event_pb2.OrderEventMessage

__all__ = ["CustomerEventMessage", "OrderEventMessage"]
