from dataclasses import dataclass
import pickle
from typing import Any

from aio_pika import DeliveryMode, Message

@dataclass(frozen=True)
class DataWithCorrelationId:
    data: dict[str, Any]
    correlation_id: str

class PythonPickleMessage(Message):
    def __init__(self, src_data: DataWithCorrelationId):
        msg_body = pickle.dumps(src_data.data)
        content_type = "application/python-pickle"
        message_kwargs = {"correlation_id": src_data.correlation_id}
        super().__init__(body=msg_body, delivery_mode=DeliveryMode.PERSISTENT, content_type=content_type, **message_kwargs)