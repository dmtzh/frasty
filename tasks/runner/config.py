from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from faststream import FastStream
from faststream.rabbit import RabbitBroker

from infrastructure import rabbitruntask as rabbit_run_task
from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.rabbitmq.broker import RabbitMQBroker
from shared.infrastructure.rabbitmq.client import RabbitMQClient
from shared.infrastructure.rabbitmq.config import RabbitMQConfig

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

_raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
_raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
_rabbitmqconfig = RabbitMQConfig.parse(_raw_rabbitmq_url, _raw_rabbitmq_publisher_confirms)
if _rabbitmqconfig is None:
    raise ValueError("Invalid RabbitMQ configuration")
_log_fmt = '%(asctime)s %(levelname)-8s - %(exchange)-4s | %(queue)-10s | %(message_id)-10s - %(message)s'
_broker = RabbitBroker(url=_rabbitmqconfig.url.value, publisher_confirms=_rabbitmqconfig.publisher_confirms, log_fmt=_log_fmt)
_rabbit_broker = RabbitMQBroker(_broker.subscriber)
rabbit_client = RabbitMQClient(_rabbit_broker)

class run_task_handler[T]:
    def __init__(self, input_adapter: Callable[[TaskIdValue, RunIdValue, dict], T]):
        self._input_adapter = input_adapter
    
    def __call__(self, handler: Callable[[T], Coroutine[Any, Any, Result | None]]):
        async def err_to_none(_):
            return None
        async def run_task_handler_wrapper(input_res: Result[T, Any]) -> Result | None:
            return await input_res\
                .map(handler)\
                .map_error(err_to_none)\
                .merge()
        return rabbit_run_task.handler(rabbit_client, self._input_adapter)(run_task_handler_wrapper)

@asynccontextmanager
async def lifespan():
    raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
    raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
    rabbitmqconfig = RabbitMQConfig.parse(raw_rabbitmq_url, raw_rabbitmq_publisher_confirms)
    if rabbitmqconfig is None:
        raise ValueError("Invalid RabbitMQ configuration")
    await _rabbit_broker.connect(rabbitmqconfig)
    yield
    await _rabbit_broker.disconnect()

app = FastStream(broker=_broker, lifespan=lifespan)