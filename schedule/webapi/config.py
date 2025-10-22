from collections.abc import Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from fastapi import FastAPI
from faststream.rabbit import RabbitBroker

from infrastructure import rabbitchangetaskschedule as rabbit_change_task_schedule
from shared.commands import Command
from shared.customtypes import ScheduleIdValue, TaskIdValue
from shared.infrastructure.rabbitmq.broker import RabbitMQBroker
from shared.infrastructure.rabbitmq.client import RabbitMQClient, Error as RabbitClientError
from shared.infrastructure.rabbitmq.config import RabbitMQConfig
from shared.utils.asyncresult import async_ex_to_error_result

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

def change_task_schedule(task_id: TaskIdValue, schedule_id: ScheduleIdValue, command: Command) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_change_schedule = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_change_task_schedule.run)
    return rabbit_change_schedule(rabbit_client, task_id, schedule_id, command)

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
