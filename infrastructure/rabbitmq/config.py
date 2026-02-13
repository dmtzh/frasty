from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from faststream import FastStream
from faststream.rabbit import RabbitBroker

from shared.pipeline.actionhandler import ActionInput
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, StepIdValue
from shared.domaindefinition import StepDefinition
from shared.pipeline.handlers import StepDefinitionType, StepHandlerContinuation, Subscriber
from shared.pipeline.types import CompleteStepData, CompletedDefinitionData, RunTaskData, StepData
from shared.utils.asyncresult import async_ex_to_error_result

from . import rabbitcompletestep as rabbit_complete_step
from . import rabbitdefinitioncompleted as rabbit_definition_completed
from . import rabbitrunaction as rabbit_action
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

def run_action(action_name: str, action_input: ActionInput) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_action = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_action.run)
    return rabbit_run_action(_rabbit_client, action_name, action_input)

def action_handler(action_name: str, action_handler: Callable[[Result[ActionInput, Any]], Coroutine]):
    return rabbit_action.handler(_rabbit_client, action_name)(action_handler)

def run_task(data: RunTaskData) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_task = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_task.run)
    return rabbit_run_task(_rabbit_client, data.task_id, data.run_id, data.metadata.to_dict())

def step_handler[TCfg](step_definition_type: StepDefinitionType[TCfg], step_handler: StepHandlerContinuation[TCfg, Any]):
    def data_validator(data: Any) -> Result[Any, Any]:
        return Result.Ok(data)
    def input_adapter(run_id: RunIdValue, step_id: StepIdValue, step_definition: StepDefinition[TCfg], data: Any, metadata: dict):
        return StepData[TCfg, Any](run_id, step_id, step_definition, data, Metadata(metadata))
    return rabbit_step.handler(_rabbit_client, step_definition_type, data_validator, input_adapter)(step_handler)

def complete_step(data: CompleteStepData) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_complete_step = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_complete_step.run)
    return rabbit_run_complete_step(_rabbit_client, data.run_id, data.step_id, data.result, data.metadata.to_dict())

def definition_completed_subscriber(queue_name: str | None, requeue_chance: RequeueChance) -> Subscriber:
    def input_adapter(run_id: RunIdValue, definition_id: DefinitionIdValue, completed_result: CompletedResult, metadata: dict):
        return CompletedDefinitionData(run_id, definition_id, completed_result, Metadata(metadata))
    return rabbit_definition_completed.subscriber(_rabbit_client, input_adapter, queue_name, requeue_chance)

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