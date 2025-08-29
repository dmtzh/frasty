import json

from shared.infrastructure.serialization.serializer import Serializer

class JsonSerializer[T](Serializer[T]):
    def serialize(self, obj: T) -> str:
        return json.dumps(obj)
    
    def deserialize(self, data: str) -> T:
        return json.loads(data)