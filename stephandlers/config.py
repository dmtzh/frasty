from collections.abc import Callable, Coroutine
import os
from typing import Any

from expression import Result

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import get_step_definition_name, step_definition_creators_storage
from shared.pipeline.handlers import StepDefinitionType, step_handler_adapter, validated_data_to_any_data
from shared.pipeline.logging import with_input_output_logging
from shared.pipeline.types import StepData
from stepdefinitions.requesturl import RequestUrl

step_definitions: list[type[StepDefinition]] = [
    RequestUrl
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

app = config.create_faststream_app()