from collections.abc import Callable
from dataclasses import dataclass
import functools
from typing import ParamSpec

from aio_pika import (
    Message,
    connect_robust
)
from aio_pika.abc import AbstractRobustChannel, AbstractExchange
from aiormq import ChannelInvalidStateError, ChannelNotFoundEntity, ConnectionChannelError
from aiormq.abc import DeliveredMessage
from expression import Result
from faststream.rabbit import RabbitQueue
from faststream.rabbit.subscriber.asyncapi import AsyncAPISubscriber
from pamqp import commands as spec

from shared.infrastructure.rabbitmq.config import RabbitMQConfig

P = ParamSpec("P")

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
    
class RabbitMQBroker:
    def __init__(self, subscriber: Callable[P, AsyncAPISubscriber]):
        self._async_startup_tasks = []
        self._subscriber = subscriber
    
    async def connect(self, config: RabbitMQConfig) -> None:
        print(f"RabbitMQBroker.connect({config})")
        try:
            self._rabbit_connection = await connect_robust(config.url.value)
            channel: AbstractRobustChannel = await self._rabbit_connection.channel(publisher_confirms=config.publisher_confirms)
            self._rabbit_channel = channel
            for async_task in self._async_startup_tasks:
                await async_task()
        except Exception:
            await self.disconnect()
            raise
    
    async def disconnect(self) -> None:
        if hasattr(self, "_rabbit_channel") and not self._rabbit_channel.is_closed:
            await self._rabbit_channel.close()
        if hasattr(self, "_rabbit_connection") and not self._rabbit_connection.is_closed:
            await self._rabbit_connection.close()
        self._rabbit_connection = None
        self._rabbit_channel = None
        print("RabbitMQBroker disconnected")

    def _add_async_startup_task(self, func):
        self._async_startup_tasks.append(func)
    
    async def _bind_queue_to_exchange(self, queue_name: str, exchange_name: str, routing_key: str):
        queue = await self._rabbit_channel.get_queue(name=queue_name, ensure=False)
        exchange = await self._rabbit_channel.get_exchange(name=exchange_name, ensure=False)
        return await queue.bind(exchange=exchange, routing_key=routing_key)
    
    def bind_queue_to_exchange(self, queue_name: str, exchange_name: str, routing_key: str):
        startup_task = functools.partial(self._bind_queue_to_exchange, queue_name, exchange_name, routing_key)
        self._add_async_startup_task(startup_task)
    
    def _create_auto_delete_queue(self, queue_name: str):
        return self._rabbit_channel.declare_queue(name=queue_name, auto_delete=True)
    
    def create_auto_delete_queue(self, queue_name: str):
        startup_task = functools.partial(self._create_auto_delete_queue, queue_name)
        self._add_async_startup_task(startup_task)
    
    def subscriber(self, queue: RabbitQueue, message_decoder: Callable, no_reply: bool, retry: bool | int) -> AsyncAPISubscriber:
        return self._subscriber(queue=queue, decoder=message_decoder, no_reply=no_reply, retry=retry)
    
    async def _publish_to_exchange(self, exchange: AbstractExchange | str, routing_key: str, message: Message, retry_count: int) -> Result[None, Error.ExchangeNotFound | Error.RouteNotFound]:
        try:
            if isinstance(exchange, str):
                exchange_to_publish = await self._rabbit_channel.get_exchange(name=exchange, ensure=True)
            else:
                exchange_to_publish = exchange
            publish_res = await exchange_to_publish.publish(message, routing_key)
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
                await self._rabbit_channel.ready()
                return await self._publish_to_exchange(message, exchange, routing_key, retry_count -1)
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
    
    def publish_to_default_exchange(self, routing_key: str, message: Message):
        if self._rabbit_channel is None:
            raise RabbitMQBrokerNotConnectedError("RabbitMQ broker is not connected. Please call connect() to establish a connection.")
        return self._publish_to_exchange(self._rabbit_channel.default_exchange, routing_key, message, 2)
    
    def publish_to_exchange(self, exchange_name: str, routing_key: str, message: Message):
        if self._rabbit_channel is None:
            raise RabbitMQBrokerNotConnectedError("RabbitMQ broker is not connected. Please call connect() to establish a connection.")
        return self._publish_to_exchange(exchange_name, routing_key, message, 2)
    