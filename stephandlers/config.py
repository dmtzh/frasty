from collections.abc import Callable, Coroutine
import os
from typing import Any

from expression import Result

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import Metadata
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import get_step_definition_name, step_definition_creators_storage
from shared.pipeline.handlers import StepDefinitionType, step_handler_adapter, validated_data_to_any_data
from shared.pipeline.logging import with_input_output_logging
from shared.pipeline.types import RunTaskData, StepData
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from fetchnewdata.fetchidvalue import FetchIdValue

step_definitions: list[type[StepDefinition]] = [
    FetchNewData, RequestUrl
]
for step_definition in step_definitions:
    step_definition_creators_storage.add(step_definition)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

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

app = config.create_faststream_app()