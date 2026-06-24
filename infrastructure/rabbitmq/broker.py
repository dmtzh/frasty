from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Optional, ParamSpec, override
from urllib.parse import urlparse

from aio_pika import (
    Message,
    RobustChannel
)
from aio_pika.abc import AbstractExchange
from aiormq import ChannelInvalidStateError, ChannelNotFoundEntity, ConnectionChannelError
from aiormq.abc import DeliveredMessage
from expression import Result
from faststream.broker.types import CustomCallable, SubscriberMiddleware
from faststream.rabbit import RabbitBroker, RabbitQueue
from faststream.rabbit.message import RabbitMessage
from pamqp import commands as spec

from shared.utils.parse import parse_bool_str

P = ParamSpec("P")

@dataclass(frozen=True)
class RabbitMQUrl:
    value: str

    @staticmethod
    def parse(url: str) -> Optional['RabbitMQUrl']:
        """
        Parses a string into a RabbitMQUrl instance if it is a valid rabbitmq URL.

        Args:
            url (str): The rabbitmq URL string to parse.

        Returns:
            RabbitMQUrl | None: A RabbitMQUrl instance if the string is a valid rabbitmq URL, 
                                or None if the string is None, empty, or invalid.
        """
        if url is None:
            return None
        match url.strip():
            case "":
                return None
            case url_stripped:
                result = urlparse(url_stripped)
                # If the URL does not have a scheme (e.g., amqp)
                # or a network location (e.g., www.example.com),
                # or if the scheme is not amqp, it's not a valid URL
                has_scheme_and_netloc = all([result.scheme, result.netloc])
                has_amqp_scheme = result.scheme in ["amqp"]
                if has_scheme_and_netloc and has_amqp_scheme:
                    return RabbitMQUrl(url_stripped)
                else:
                    return None

@dataclass(frozen=True)
class RabbitMQConfig:
    url: RabbitMQUrl
    publisher_confirms: bool

    @staticmethod
    def parse(url: str, publisher_confirms: str) -> Optional['RabbitMQConfig']:
        opt_url = RabbitMQUrl.parse(url)
        opt_publisher_confirms = parse_bool_str(publisher_confirms)
        if opt_url is None or opt_publisher_confirms is None:
            return None
        return RabbitMQConfig(url=opt_url, publisher_confirms=opt_publisher_confirms)

class RabbitMQBrokerNotConnectedError(Exception):
    '''
    Raised when an attempt is made to interact with a RabbitMQ broker that is not connected.
    '''
class RabbitMQBrokerUnexpectedError(Exception):
    '''
    Raised when an unexpected error occurs while interacting with a RabbitMQ broker.
    '''

class Error:
    @dataclass(frozen=True)
    class ExchangeNotFound:
        '''Exchange with exchange name not found'''
        exchange_name: str
    @dataclass(frozen=True)
    class RouteNotFound:
        '''Route with routing key not found'''
        routing_key: str

@dataclass
class AsyncCommandSubscriber:
    command: str
    decoder: CustomCallable | None
    no_reply: bool
    middlewares: Sequence[SubscriberMiddleware[RabbitMessage]]
    func: Callable | None = None

    def __call__(self, func: Callable):
        self.func = func
    
class RabbitMQBroker(RabbitBroker):
    _command_subscribers: tuple[AsyncCommandSubscriber, ...] = ()
    def command_subscriber(self, command: str, decoder: CustomCallable | None = None, no_reply: bool = False, middlewares: Sequence[SubscriberMiddleware[RabbitMessage]] = ()):
        command_subscriber = AsyncCommandSubscriber(command, decoder, no_reply, middlewares)
        self._command_subscribers += (command_subscriber,)
        return command_subscriber

    def publish_to_default_exchange(self, routing_key: str, message: Message):
        if self._channel is None:
            raise RabbitMQBrokerNotConnectedError("RabbitMQ broker is not connected. Please call start() to establish a connection.")
        return RabbitMQBroker._publish_to_exchange(self._channel, self._channel.default_exchange, routing_key, message, 2)

    @staticmethod
    async def _publish_to_exchange(channel: RobustChannel, exchange: AbstractExchange, routing_key: str, message: Message, retry_count: int) -> Result[None, Error.ExchangeNotFound | Error.RouteNotFound]:
        try:
            publish_res = await exchange.publish(message, routing_key)
        except ChannelNotFoundEntity as ch_ex:
            args = tuple(arg for arg in ch_ex.args if isinstance(arg, str))
            if isinstance(exchange, str):
                has_exchange_name = any(True for arg in args if exchange in arg)
                if has_exchange_name:
                    return Result.Error(Error.ExchangeNotFound(exchange))
            raise RabbitMQBrokerUnexpectedError(*ch_ex.args)
        except (ChannelInvalidStateError, ConnectionChannelError) as ch_inv_err:
            if retry_count > 0:
                # await asyncio.sleep(RobustChannel.RESTORE_RETRY_DELAY)
                await channel.ready()
                return await RabbitMQBroker._publish_to_exchange(channel, exchange, routing_key, message, retry_count -1)
            raise ch_inv_err
        except Exception as ex:
            raise ex
        match publish_res:
            case DeliveredMessage(delivery=delivery, header=_, body=_, channel=_):
                if isinstance(delivery, spec.Basic.Return) and delivery.reply_code == 312:
                    route_not_found = Error.RouteNotFound(routing_key)
                    return Result.Error(route_not_found)
                raise RabbitMQBrokerUnexpectedError(delivery)
            case _:
                return Result.Ok(None)

    @override
    def start(self):
        self._setup_command_subscribers()
        return super().start()

    def _setup_command_subscribers(self):
        if not self._command_subscribers:
            return
        
        for cmd_subscriber in self._command_subscribers:
            queue = RabbitQueue(name=cmd_subscriber.command, passive=True)
            subscriber = self.subscriber(queue=queue, decoder=cmd_subscriber.decoder, no_reply=cmd_subscriber.no_reply, middlewares=cmd_subscriber.middlewares)
            subscriber(cmd_subscriber.func)
