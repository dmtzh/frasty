from dataclasses import dataclass

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
    def __new__(cls, value):
        instance = super().__new__(cls, value)
        return instance
    
    def to_value_with_checksum(self):
        return CrockfordId(self).get_value_with_checksum()
    
    @classmethod
    def new_id(cls):
        crockford_id = CrockfordId.new_id()
        return cls(crockford_id)
    
    @classmethod
    def from_value_with_checksum(cls, value_with_checksum: str):
        opt_crockford_id = CrockfordId.from_value_with_checksum(value_with_checksum)
        match opt_crockford_id:
            case None:
                return None
            case crockford_id:
                return cls(crockford_id)