from typing import Type
from pydantic import BaseModel

from shared.infrastructure.serialization.serializer import Serializer

class PydanticJsonSerializer[T: BaseModel](Serializer[T]):
    def __init__(self, type: Type[T]):
        self._type = type

    def serialize(self, obj: T) -> str:
        return obj.model_dump_json(exclude_none=True)
    
    def deserialize(self, data: str) -> T:
        return self._type.model_validate_json(data)