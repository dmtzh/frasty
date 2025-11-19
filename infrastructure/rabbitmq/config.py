from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from faststream import FastStream
from faststream.rabbit import RabbitBroker

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, ScheduleIdValue, StepIdValue, TaskIdValue
from shared.domaindefinition import StepDefinition
from shared.pipeline.handlers import Handler, StepDefinitionType, StepHandler, Subscriber
from shared.pipeline.types import CompleteStepData, CompletedDefinitionData, RunDefinitionData, RunTaskData, StepData
from shared.utils.asyncresult import async_ex_to_error_result

from . import rabbitchangetaskschedule as rabbit_change_task_schedule
from . import rabbitcompletestep as rabbit_complete_step
from . import rabbitrundefinition as rabbit_definition
from . import rabbitdefinitioncompleted as rabbit_definition_completed
from . import rabbitrunstep as rabbit_step
from . import rabbitruntask as rabbit_task
from .broker import RabbitMQBroker, RabbitMQConfig
from .client import RabbitMQClient, Error as RabbitClientError
from .rabbitmiddlewares import RequeueChance

_raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
_raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
_rabbitmqconfig = RabbitMQConfig.parse(_raw_rabbitmq_url, _raw_rabbitmq_publisher_confirms)
if _rabbitmqconfig is None:
    raise ValueError("Invalid RabbitMQ configuration")
_log_fmt = '%(asctime)s %(levelname)-8s - %(exchange)-4s | %(queue)-10s | %(message_id)-10s - %(message)s'
_broker = RabbitBroker(url=_rabbitmqconfig.url.value, publisher_confirms=_rabbitmqconfig.publisher_confirms, log_fmt=_log_fmt)
_rabbit_broker = RabbitMQBroker(_broker.subscriber)
_rabbit_client = RabbitMQClient(_rabbit_broker)

def run_task(data: RunTaskData) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_task = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_task.run)
    return rabbit_run_task(_rabbit_client, data.task_id, data.run_id, data.metadata.to_dict())

def run_task_handler() -> Handler[RunTaskData]:
    def input_adapter(task_id: TaskIdValue, run_id: RunIdValue, metadata: dict):
        return RunTaskData(task_id, run_id, Metadata(metadata))
    return rabbit_task.handler(_rabbit_client, input_adapter)

def run_definition(data: RunDefinitionData) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_definition = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_definition.run)
    return rabbit_run_definition(_rabbit_client, data.run_id, data.definition_id, data.metadata.to_dict())

def run_definition_handler() -> Handler[RunDefinitionData]:
    def input_adapter(run_id: RunIdValue, definition_id: DefinitionIdValue, metadata: dict):
        return RunDefinitionData(run_id, definition_id, Metadata(metadata))
    return rabbit_definition.handler(_rabbit_client, input_adapter)

def run_step(data: StepData) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_step = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_step.run)
    return rabbit_run_step(_rabbit_client, data.run_id, data.step_id, data.definition, data.data, data.metadata.to_dict())

def step_handler[TCfg](step_definition_type: StepDefinitionType[TCfg]) -> StepHandler[TCfg, Any]:
    def data_validator(data: Any) -> Result[Any, Any]:
        return Result.Ok(data)
    def input_adapter(run_id: RunIdValue, step_id: StepIdValue, step_definition: StepDefinition[TCfg], data: Any, metadata: dict):
        return StepData[TCfg, Any](run_id, step_id, step_definition, data, Metadata(metadata))
    return rabbit_step.handler(_rabbit_client, step_definition_type, data_validator, input_adapter)

def complete_step(data: CompleteStepData) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_complete_step = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_complete_step.run)
    return rabbit_run_complete_step(_rabbit_client, data.run_id, data.step_id, data.result, data.metadata.to_dict())

def complete_step_handler() -> Handler[CompleteStepData]:
    def input_adapter(run_id: RunIdValue, step_id: StepIdValue, completed_result: CompletedResult, metadata: dict):
        return CompleteStepData(run_id, step_id, completed_result, Metadata(metadata))
    return rabbit_complete_step.handler(_rabbit_client, input_adapter)

def publish_completed_definition(data: CompletedDefinitionData) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_publish_definition_completed = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_definition_completed.publish)
    return rabbit_publish_definition_completed(_rabbit_client, data.run_id, data.definition_id, data.result, data.metadata.to_dict())

def definition_completed_subscriber(queue_name: str | None, requeue_chance: RequeueChance) -> Subscriber:
    def input_adapter(run_id: RunIdValue, definition_id: DefinitionIdValue, completed_result: CompletedResult, metadata: dict):
        return CompletedDefinitionData(run_id, definition_id, completed_result, Metadata(metadata))
    return rabbit_definition_completed.subscriber(_rabbit_client, input_adapter, queue_name, requeue_chance)

def change_task_schedule(task_id: TaskIdValue, schedule_id: ScheduleIdValue, command_dto: dict):
    rabbit_change_schedule = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_change_task_schedule.run)
    return rabbit_change_schedule(_rabbit_client, task_id, schedule_id, command_dto)

def change_task_schedule_handler[T](input_adapter: Callable[[TaskIdValue, ScheduleIdValue, dict], T]) -> Handler[T]:
    return rabbit_change_task_schedule.handler(_rabbit_client, input_adapter)

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

def create_faststream_app():
    return FastStream(broker=_broker, lifespan=lifespan)