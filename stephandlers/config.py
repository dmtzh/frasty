from collections.abc import Callable, Coroutine, Generator
from contextlib import asynccontextmanager
import functools
import os
from typing import Any

from expression import Result, effect
from faststream import FastStream
from faststream.rabbit import RabbitBroker

from infrastructure import rabbitcompletestep as rabbit_complete_step
from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrunstep as rabbit_run_step
from infrastructure import rabbitruntask as rabbit_task
from infrastructure.rabbitmiddlewares import RequeueChance
from shared.completedresult import CompletedResult
from shared.customtypes import RunIdValue, StepIdValue, TaskIdValue
from shared.definitioncompleteddata import DefinitionCompletedData
from shared.domaindefinition import StepDefinition
from shared.infrastructure.rabbitmq.broker import RabbitMQBroker
from shared.infrastructure.rabbitmq.client import RabbitMQClient, Error as RabbitClientError
from shared.infrastructure.rabbitmq.config import RabbitMQConfig
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.runstepdata import RunStepData
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.parse import parse_from_dict
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from fetchnewdata.fetchidvalue import FetchIdValue
from getcontentfromjson.definition import GetContentFromJson
from sendtoviberchannel.config import ViberApiConfig
from sendtoviberchannel.definition import SendToViberChannel
from wrapper import wrap_step_handler as step_handler_wrapper

step_definitions: list[type[StepDefinition]] = [
    FetchNewData, RequestUrl, FilterSuccessResponse,
    FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml,
    SendToViberChannel,
    GetContentFromJson
]
for step_definition in step_definitions:
    step_definition_creators_storage.add(step_definition)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

_viber_api_config = ViberApiConfig.parse(os.environ["VIBER_API_URL"], os.environ["VIBER_API_HTTP_METHOD"])
if _viber_api_config is None:
    raise ValueError("Invalid Viber API configuration")
viber_api_config = _viber_api_config

_raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
_raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
_rabbitmqconfig = RabbitMQConfig.parse(_raw_rabbitmq_url, _raw_rabbitmq_publisher_confirms)
if _rabbitmqconfig is None:
    raise ValueError("Invalid RabbitMQ configuration")
_log_fmt = '%(asctime)s %(levelname)-8s - %(exchange)-4s | %(queue)-10s | %(message_id)-10s - %(message)s'
_broker = RabbitBroker(url=_rabbitmqconfig.url.value, publisher_confirms=_rabbitmqconfig.publisher_confirms, log_fmt=_log_fmt)
_rabbit_broker = RabbitMQBroker(_broker.subscriber)
rabbit_client = RabbitMQClient(_rabbit_broker)

def complete_step(run_id: RunIdValue, step_id: StepIdValue, completed_result: CompletedResult, metadata: dict) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_complete_step = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_complete_step.run)
    return rabbit_run_complete_step(rabbit_client, run_id, step_id, completed_result, metadata)

class step_handler[TCfg, D]:
    def __init__(self, step_definition_type: type[StepDefinition[TCfg]], data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D, dict], RunStepData[TCfg, D]]):
        self._step_definition_type = step_definition_type
        self._data_validator = data_validator
        self._input_adapter = input_adapter
    
    def __call__(self, handler: Callable[[RunStepData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
        def rabbit_send_response_handler(run_step_data: RunStepData[TCfg, D], result: CompletedResult):
            return complete_step(run_step_data.run_id, run_step_data.step_id, result, run_step_data.metadata)
        handler_wrapper = functools.partial(step_handler_wrapper(handler), rabbit_send_response_handler)
        return rabbit_run_step.handler(rabbit_client, self._step_definition_type, self._data_validator, self._input_adapter)(handler_wrapper)

def run_task(task_id: TaskIdValue, run_id: RunIdValue, from_: str, metadata: dict) -> Coroutine[Any, Any, Result[None, Any]]:
    rabbit_run_task = async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)(rabbit_task.run)
    return rabbit_run_task(rabbit_client, task_id, run_id, from_, metadata)

def fetch_data(step_data: RunStepData[None, FetchNewDataInput], fetch_id: FetchIdValue):
    metadata = {
        "fetch_id": fetch_id.to_value_with_checksum(),
        "parent_metadata": step_data.metadata
    }
    return run_task(step_data.data.task_id, step_data.run_id, "fetch new data step", metadata)

class data_fetched_handler[T]:
    def __init__(self, input_adapter: Callable[[FetchIdValue, TaskIdValue, RunIdValue, CompletedResult, dict], T]):
        self._input_adapter = input_adapter

    @effect.result[T, str]()
    def _definition_to_fetched_data(self, data: DefinitionCompletedData) -> Generator[Any, Any, T]:
        yield from parse_from_dict(data.metadata, "from", lambda s: True if s == "fetch new data step" else None)
        fetch_id = yield from parse_from_dict(data.metadata, "fetch_id", FetchIdValue.from_value_with_checksum)
        task_id = yield from parse_from_dict(data.metadata, "task_id", TaskIdValue.from_value_with_checksum)
        metadata = yield from parse_from_dict(data.metadata, "parent_metadata", lambda pm: pm if type(pm) is dict else None)
        return self._input_adapter(fetch_id, task_id, data.run_id, data.result, metadata)

    def __call__(self, handler: Callable[[T], Coroutine[Any, Any, Result | None]]):
        async def err_to_none(_):
            return None
        async def data_fetched_handler_wrapper(input_res: Result[DefinitionCompletedData, Any]) -> Result | None:
            res = await input_res\
                .bind(self._definition_to_fetched_data)\
                .map(handler)\
                .map_error(err_to_none)\
                .merge()
            return res
        return rabbit_definition_completed.subscriber(rabbit_client, DefinitionCompletedData, queue_name="fetchnewdata_completed_tasks", requeue_chance=RequeueChance.HIGH)(data_fetched_handler_wrapper)

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