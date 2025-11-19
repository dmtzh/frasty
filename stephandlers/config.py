from collections.abc import Callable, Coroutine, Generator
from dataclasses import dataclass
import os
from typing import Any

from expression import Result, effect

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import Metadata, RunIdValue, TaskIdValue
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, StepHandlerAdapterFactory, map_handler, only_from, with_input_output_logging_subscriber
from shared.pipeline.types import CompletedDefinitionData, RunTaskData, StepData
from shared.utils.parse import parse_value
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

def fetch_data(step_data: StepData[None, FetchNewDataInput], fetch_id: FetchIdValue):
    metadata = Metadata()
    metadata.set_from("fetch new data step")
    metadata.set_id("fetch_id", fetch_id)
    metadata.set("parent_metadata", step_data.metadata.to_dict())
    data = RunTaskData(step_data.data.task_id, step_data.run_id, metadata)
    return config.run_task(data)

@dataclass(frozen=True)
class FetchedData:
    fetch_id: FetchIdValue
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedResult
    metadata: Metadata

def data_fetched_subscriber(func: Callable[[FetchedData], Coroutine[Any, Any, Result | None]]):
    @effect.result[FetchedData, str]()
    def definition_to_fetched_data(data: CompletedDefinitionData) -> Generator[Any, Any, FetchedData]:
        fetch_id = yield from parse_value(data.metadata, "fetch_id", lambda m: Metadata.get_id(m, "fetch_id", FetchIdValue))
        task_id = yield from parse_value(data.metadata, "task_id", lambda m: Metadata.get_id(m, "task_id", TaskIdValue))
        metadata_dict = yield from parse_value(data.metadata.get("parent_metadata"), "parent_metadata", lambda pm: pm if type(pm) is dict else None)
        metadata = Metadata(metadata_dict)
        return FetchedData(fetch_id, task_id, data.run_id, data.result, metadata)
    subscriber = config.definition_completed_subscriber("fetchnewdata_completed_tasks", config.RequeueChance.HIGH)
    fetch_new_data_step_subscriber = only_from(subscriber, "fetch new data step")
    with_logging_subscriber = with_input_output_logging_subscriber(fetch_new_data_step_subscriber, "data_fetched")
    fetched_data_subscriber = map_handler(with_logging_subscriber, lambda data_res: data_res.bind(definition_to_fetched_data))
    return DefinitionCompletedSubscriberAdapter(fetched_data_subscriber)(func)

app = config.create_faststream_app()