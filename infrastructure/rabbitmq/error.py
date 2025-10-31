from collections.abc import Callable
from dataclasses import dataclass

from shared.customtypes import Error

@dataclass(frozen=True)
class RabbitMessageErrorExtraData:
    location: str
    correlation_id: str

@dataclass(frozen=True)
class ParseError(RabbitMessageErrorExtraData, Error):
    pass

@dataclass(frozen=True)
class ValidationError(RabbitMessageErrorExtraData, Error):
    pass
    
type RabbitMessageError = ParseError | ValidationError

type RabbitMessageErrorCreator = Callable[[type[RabbitMessageError], str], RabbitMessageError]

def rabbit_message_error_creator(location: str, correlation_id: str):
    def create_error(error_type: type[RabbitMessageError], message: str) -> RabbitMessageError:
        return error_type(message, location, correlation_id)
    return create_error