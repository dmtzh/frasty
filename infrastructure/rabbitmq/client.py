import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar

from aio_pika import Message
from expression import Result
from faststream.broker.types import SubscriberMiddleware

from shared.customtypes import Error as CustomError
from shared.utils.result import ResultTag

from .broker import RabbitMQBroker, Error as BrokerError

P = ParamSpec("P")
R = TypeVar("R")

class Error:
    @dataclass(frozen=True)
    class SendCommandTimeout:
        command: str
    
    @dataclass(frozen=True)
    class CommandRecipientNotFound:
        command: str
    
    class UnexpectedError(CustomError):
        '''Unexpected error when send command'''

class RabbitMQClient:
    def __init__(self, broker: RabbitMQBroker):
        self._broker = broker
    
    async def send_command(self, command: str, message: Message) -> Result[None, Error.CommandRecipientNotFound | Error.SendCommandTimeout]:
        publish_task = asyncio.create_task(self._broker.publish_to_default_exchange(command, message))
        try:
            five_seconds = 5
            publish_res = await asyncio.wait_for(publish_task, timeout=five_seconds)
            match publish_res:
                case Result(tag=ResultTag.OK, ok=_):
                    return Result.Ok(None)
                case Result(tag=ResultTag.ERROR, error=BrokerError.RouteNotFound(routing_key)) if routing_key == command:
                    return Result.Error(Error.CommandRecipientNotFound(command))
                case _:
                    raise RuntimeError("This should never happen")
        except asyncio.TimeoutError:
            publish_task.cancel()
            return Result.Error(Error.SendCommandTimeout(command))
    
    def command_handler(self, command: str, message_decoder: Callable, middlewares: Sequence[SubscriberMiddleware[Any]] = ()):
        return self._broker.command_subscriber(command=command, decoder=message_decoder, no_reply=True, middlewares=middlewares)
