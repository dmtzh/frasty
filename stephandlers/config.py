from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
import functools
import os
from typing import Any

from expression import Result

from faststream import FastStream
from faststream.rabbit import RabbitBroker

from infrastructure import rabbitcompletestep as rabbit_complete_step
from infrastructure import rabbitrunstep as rabbit_run_step
from shared.completedresult import CompletedResult
from shared.customtypes import RunIdValue, StepIdValue
from shared.domaindefinition import StepDefinition
from shared.infrastructure.rabbitmq.broker import RabbitMQBroker
from shared.infrastructure.rabbitmq.client import RabbitMQClient, Error as RabbitClientError
from shared.infrastructure.rabbitmq.config import RabbitMQConfig
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.runstepdata import RunStepData
from shared.utils.asyncresult import async_ex_to_error_result
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData

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

class run_step_handler[TCfg, D]:
    def __init__(self, step_definition_type: type[StepDefinition[TCfg]], data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D, dict], RunStepData[TCfg, D]]):
        self._step_definition_type = step_definition_type
        self._data_validator = data_validator
        self._input_adapter = input_adapter
    
    def __call__(self, handler: Callable[[RunStepData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
        @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
        def rabbit_send_response_handler(run_step_data: RunStepData[TCfg, D], result: CompletedResult):
            return rabbit_complete_step.run(rabbit_client, run_step_data.run_id, run_step_data.step_id, result, run_step_data.metadata)
        handler_wrapper = functools.partial(step_handler_wrapper(handler), rabbit_send_response_handler)
        return rabbit_run_step.handler(rabbit_client, self._step_definition_type, self._data_validator, self._input_adapter)(handler_wrapper)

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