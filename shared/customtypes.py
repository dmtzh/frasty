from dataclasses import dataclass
from typing import Any, TypeVar

from .utils.crockfordid import CrockfordId

@dataclass(frozen=True)
class Error:
    message: str

    @classmethod
    def from_exception(cls, exception: Exception):
        return cls(message = str(exception))
    
    @classmethod
    def from_error(cls, error):
        return cls(message=str(error))

class IdValue(str):
    _length: int = 8
    def __new__(cls, value):
        instance = super().__new__(cls, value)
        return instance
    
    def to_value_with_checksum(self):
        return CrockfordId(self).get_value_with_checksum()
    
    @classmethod
    def new_id(cls):
        crockford_id = CrockfordId.new_id(cls._length)
        return cls(crockford_id)
    
    @classmethod
    def from_value_with_checksum(cls, value_with_checksum: str):
        opt_crockford_id = CrockfordId.from_value_with_checksum(value_with_checksum, cls._length)
        match opt_crockford_id:
            case None:
                return None
            case crockford_id:
                return cls(crockford_id)
    
    @classmethod
    def from_value(cls, value: str):
        opt_crockford_id = CrockfordId.from_value(value, cls._length)
        match opt_crockford_id:
            case None:
                return None
            case crockford_id:
                return cls(crockford_id)

class TaskIdValue(IdValue):
    '''Task id'''
    _length: int = 3

class RunIdValue(IdValue):
    '''Run id'''

class DefinitionIdValue(IdValue):
    '''Definition id'''

class StepIdValue(IdValue):
    '''Step id'''

class ScheduleIdValue(IdValue):
    '''Schedule id'''

IdVal = TypeVar("IdVal", bound=IdValue)

class Metadata(dict):
    '''Metadata'''
    def set(self, key: str, value: Any):
        self[key] = value
    
    def set_id(self, key: str, id: IdValue):
        self.set(key, id.to_value_with_checksum())
    
    def set_task_id(self, task_id: TaskIdValue):
        self.set_id("task_id", task_id)
    
    def set_definition_id(self, definition_id: DefinitionIdValue):
        self.set_id("definition_id", definition_id)
    
    def clone(self):
        return Metadata(self)
    
    def to_dict(self):
        return self.copy()
    
    def set_from(self, value: str):
        self.set("from", value)
    
    def get_from(self) -> str | None:
        return self.get("from", None)
    
    def get_task_id(self) -> TaskIdValue | None:
        return TaskIdValue.from_value_with_checksum(self.get("task_id", ""))
    
    def get_id(self, key: str, id_type: type[IdVal]):
        return id_type.from_value_with_checksum(self.get(key, ""))