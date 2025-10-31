from collections.abc import Callable, Generator
import os
from typing import Any

from expression import effect

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import RunIdValue, TaskIdValue
from shared.definitioncompleteddata import DefinitionCompletedData
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, StepHandlerAdapterFactory, map_handler
from shared.runstepdata import RunStepData
from shared.utils.parse import parse_from_dict
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from fetchnewdata.fetchidvalue import FetchIdValue
from getcontentfromjson.definition import GetContentFromJson
from sendtoviberchannel.config import ViberApiConfig
from sendtoviberchannel.definition import SendToViberChannel

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

complete_step = config.complete_step

step_handler = StepHandlerAdapterFactory(config.step_handler, complete_step)

def fetch_data(step_data: RunStepData[None, FetchNewDataInput], fetch_id: FetchIdValue):
    metadata = {
        "fetch_id": fetch_id.to_value_with_checksum(),
        "parent_metadata": step_data.metadata
    }
    return config.run_task(step_data.data.task_id, step_data.run_id, "fetch new data step", metadata)

def data_fetched_handler[T](input_adapter: Callable[[FetchIdValue, TaskIdValue, RunIdValue, CompletedResult, dict], T]):
    @effect.result[T, str]()
    def definition_to_fetched_data(data: DefinitionCompletedData) -> Generator[Any, Any, T]:
        yield from parse_from_dict(data.metadata, "from", lambda s: True if s == "fetch new data step" else None)
        fetch_id = yield from parse_from_dict(data.metadata, "fetch_id", FetchIdValue.from_value_with_checksum)
        task_id = yield from parse_from_dict(data.metadata, "task_id", TaskIdValue.from_value_with_checksum)
        metadata = yield from parse_from_dict(data.metadata, "parent_metadata", lambda pm: pm if type(pm) is dict else None)
        return input_adapter(fetch_id, task_id, data.run_id, data.result, metadata)
    subscriber = config.definition_completed_subscriber(DefinitionCompletedData, "fetchnewdata_completed_tasks", config.RequeueChance.HIGH)
    fetched_data_subscriber = map_handler(subscriber, lambda data_res: data_res.bind(definition_to_fetched_data))
    return DefinitionCompletedSubscriberAdapter(fetched_data_subscriber)

app = config.create_faststream_app()