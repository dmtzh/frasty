from collections.abc import Callable, Coroutine
import os
from typing import Any

from expression import Result

from infrastructure.rabbitmq import config
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.pipeline.handlers import to_continuation
from shared.pipeline.logging import with_input_output_logging
from shared.pipeline.types import CompleteStepData, RunDefinitionData
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData
from stepdefinitions.viber import SendToViberChannel
from stephandlers.getcontentfromjson.definition import GetContentFromJson

run_action = config.run_action
action_handler = config.action_handler

step_definitions: list[type[StepDefinition]] = [
    RequestUrl, FilterSuccessResponse,
    FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml,
    FetchNewData, SendToViberChannel,
    GetContentFromJson
]
for step_definition in step_definitions:
    step_definition_creators_storage.add(step_definition)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def run_definition_handler(func: Callable[[RunDefinitionData], Coroutine[Any, Any, Result | None]]):
    handler = to_continuation(func)
    handler_with_logging = with_input_output_logging(handler, "run_definition")
    return config.run_definition_handler(handler_with_logging)

run_step = config.run_step

def complete_step_handler(func: Callable[[CompleteStepData], Coroutine[Any, Any, Result | None]]):
    handler = to_continuation(func)
    handler_with_logging = with_input_output_logging(handler, "complete_step")
    return config.complete_step_handler(handler_with_logging)

publish_completed_definition = config.publish_completed_definition

app = config.create_faststream_app()