from collections.abc import Callable, Coroutine, Generator
from dataclasses import dataclass
import os
from typing import Any

from expression import Result, effect

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import Metadata, RunIdValue, TaskIdValue
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import get_step_definition_name, step_definition_creators_storage
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, StepDefinitionType, map_handler, only_from, step_handler_adapter, validated_data_to_any_data
from shared.pipeline.logging import with_input_output_logging, with_input_output_logging_subscriber
from shared.pipeline.types import CompletedDefinitionData, RunTaskData, StepData
from shared.utils.parse import parse_value
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from fetchnewdata.fetchidvalue import FetchIdValue

step_definitions: list[type[StepDefinition]] = [
    FetchNewData, RequestUrl, FilterSuccessResponse,
    FilterHtmlResponse, GetContentFromHtml
]
for step_definition in step_definitions:
    step_definition_creators_storage.add(step_definition)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

complete_step = config.complete_step

def step_handler[TCfg, D](step_definition_type: StepDefinitionType[TCfg], data_validator: Callable[[Any], Result[D, Any]]):
    def wrapper(func: Callable[[StepData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
        step_handler = step_handler_adapter(func, config.complete_step)
        message_prefix = get_step_definition_name(step_definition_type)
        step_handler_with_logging = with_input_output_logging(step_handler, message_prefix)
        config_step_handler = validated_data_to_any_data(step_handler_with_logging, data_validator)
        return config.step_handler(step_definition_type, config_step_handler)
    return wrapper

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