from kafka_mongo_pipeline.models.processing_result import ProcessingResult


class DeadLetterEvent(ProcessingResult):
    """Wraps unparseable messages from any stream for error tracking."""
    raw_value: bytes
    error_type: str
    error_message: str

    def is_success(self) -> bool:
        return False

    def get_event_type(self) -> str:
        return "DEAD_LETTER"

    model_config = {"json_encoders": {bytes: lambda v: v.hex()}}