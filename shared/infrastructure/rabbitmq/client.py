import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import ParamSpec, TypeVar

from aio_pika import Message
from expression import Result
from faststream.rabbit import RabbitQueue

from shared.customtypes import Error as CustomError
from shared.infrastructure.rabbitmq.broker import RabbitMQBroker, Error as BrokerError
from shared.utils.crockfordid import CrockfordId
from shared.utils.result import ResultTag

P = ParamSpec("P")
R = TypeVar("R")

class Error:
    @dataclass(frozen=True)
    class SendCommandTimeout:
        command: str
    
    @dataclass(frozen=True)
    class SendEventTimeout:
        event: str

    @dataclass(frozen=True)
    class CommandRecipientNotFound:
        command: str
    
    @dataclass(frozen=True)
    class EventGroupNotFound:
        event_group: str

    @dataclass(frozen=True)
    class EventRecipientNotFound:
        event: str
    
    class UnexpectedError(CustomError):
        '''Unexpected error when send command/event'''

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
    
    # def send_command_with_reply(self, command: str, data: Mapping[str, Any], correlation_id: str, reply_command: str):
    #     queue_name = command
    #     message_kwargs = {"correlation_id": correlation_id, "reply_to": reply_command}
    #     return self._publish_to_queue(queue_name=queue_name, data=data, **message_kwargs)
    
    async def send_event(self, event: str, event_group: str, message: Message) -> Result[None, Error.EventGroupNotFound | Error.EventRecipientNotFound | Error.SendEventTimeout]:
        publish_task = asyncio.create_task(self._broker.publish_to_exchange(event_group, event, message))
        try:
            five_seconds = 5
            publish_res = await asyncio.wait_for(publish_task, timeout=five_seconds)
            match publish_res:
                case Result(tag=ResultTag.OK, ok=_):
                    return Result.Ok(None)
                case Result(tag=ResultTag.ERROR, error=BrokerError.ExchangeNotFound(exchange_name)) if exchange_name == event_group:
                    return Result.Error(Error.EventGroupNotFound(event_group))
                case Result(tag=ResultTag.ERROR, error=BrokerError.RouteNotFound(routing_key)) if routing_key == event:
                    return Result.Error(Error.EventRecipientNotFound(event))
                case _:
                    raise RuntimeError("This should never happen")
        except asyncio.TimeoutError:
            publish_task.cancel()
            return Result.Error(Error.SendEventTimeout(f"{event} ({event_group})"))
    
    def event_handler(self, event: str, event_group: str, queue_name: str | None, message_decoder: Callable):
        if queue_name is not None:
            queue = RabbitQueue(name=queue_name, passive=True)
        else:
            name = CrockfordId.new_id(4)
            self._broker.create_auto_delete_queue(name)
            queue = RabbitQueue(name=name, passive=True)
        self._broker.bind_queue_to_exchange(queue.name, event_group, event)
        return self._broker.subscriber(queue=queue, message_decoder=message_decoder, no_reply=True, retry=False)
    
    def command_handler(self, command: str, message_decoder: Callable):
        queue = RabbitQueue(name=command, passive=True)
        return self._broker.subscriber(queue=queue, message_decoder=message_decoder, no_reply=True, retry=False)

