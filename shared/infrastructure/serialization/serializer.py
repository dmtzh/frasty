from abc import ABC, abstractmethod

class Serializer[T](ABC):
    @abstractmethod
    def serialize(self, obj: T) -> str:
        pass

    @abstractmethod
    def deserialize(self, data: str) -> T:
        pass