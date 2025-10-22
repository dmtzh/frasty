from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from fastapi import FastAPI
from faststream.rabbit import RabbitBroker

from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitruntask as rabbit_task
from infrastructure.rabbitmiddlewares import RequeueChance
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue, TaskIdValue
from shared.infrastructure.rabbitmq.broker import RabbitMQBroker
from shared.infrastructure.rabbitmq.client import RabbitMQClient, Error as RabbitClientError
from shared.infrastructure.rabbitmq.config import RabbitMQConfig
from shared.utils.asyncresult import async_ex_to_error_result

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
ADD_DEFINITION_URL = os.environ['ADD_DEFINITION_URL']
CHANGE_SCHEDULE_URL = os.environ['CHANGE_SCHEDULE_URL']

_raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
_raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
_rabbitmqconfig = RabbitMQConfig.parse(_raw_rabbitmq_url, _raw_rabbitmq_publisher_confirms)
if _rabbitmqconfig is None:
    raise ValueError("Invalid RabbitMQ configuration")
_log_fmt = '%(asctime)s %(levelname)-8s - %(exchange)-4s | %(queue)-10s | %(message_id)-10s - %(message)s'
_broker = RabbitBroker(url=_rabbitmqconfig.url.value, publisher_confirms=_rabbitmqconfig.publisher_confirms, log_fmt=_log_fmt)
_rabbit_broker = RabbitMQBroker(_broker.subscriber)
rabbit_client = RabbitMQClient(_rabbit_broker)

class definition_completed_subscriber[T]:
    def __init__(self, input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], T]):
        self._input_adapter = input_adapter

    def __call__(self, handler: Callable[[T], Coroutine[Any, Any, Result | None]]):
        async def err_to_none(_):
            return None
        async def definition_completed_subscriber_wrapper(input_res: Result[T, Any]) -> Result | None:
            res = await input_res\
                .map(handler)\
                .map_error(err_to_none)\
                .merge()
            return res
        return rabbit_definition_completed.subscriber(rabbit_client, self._input_adapter, queue_name=None, requeue_chance=RequeueChance.LOW)(definition_completed_subscriber_wrapper)

def run_task(task_id: TaskIdValue,run_id: RunIdValue, from_: str, metadata: dict) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_task = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_task.run)
    return rabbit_run_task(rabbit_client, task_id, run_id, from_, metadata)

@asynccontextmanager
async def lifespan(app: FastAPI):
    raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
    raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
    rabbitmqconfig = RabbitMQConfig.parse(raw_rabbitmq_url, raw_rabbitmq_publisher_confirms)
    if rabbitmqconfig is None:
        raise ValueError("Invalid RabbitMQ configuration")
    await _rabbit_broker.connect(rabbitmqconfig)
    await _broker.start()
    yield
    await _rabbit_broker.disconnect()
    await _broker.stop()
