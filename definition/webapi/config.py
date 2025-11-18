from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue
from shared.domaindefinition import StepDefinition
from shared.domainrunning import RunningDefinitionState
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, only_from, with_input_output_logging_subscriber
from shared.pipeline.types import CompletedDefinitionData
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData
from stepdefinitions.viber import SendToViberChannel
from stephandlers.getcontentfromjson.definition import GetContentFromJson

step_definitions: list[type[StepDefinition]] = [
    RequestUrl, FilterSuccessResponse,
    FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml,
    FetchNewData, SendToViberChannel,
    GetContentFromJson
]
for step_definition in step_definitions:
    step_definition_creators_storage.add(step_definition)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def run_first_step_manually(manual_run_id: RunIdValue, manual_definition_id: DefinitionIdValue, evt: RunningDefinitionState.Events.StepRunning):
    metadata = Metadata()
    metadata.set_from("definition manual run webapi")
    metadata.set_definition_id(manual_definition_id)
    return config.run_step(manual_run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)

def manual_run_definition_completed_subscriber(func: Callable[[CompletedDefinitionData], Coroutine[Any, Any, Result | None]]):
    subscriber = config.definition_completed_subscriber(None, config.RequeueChance.LOW)
    manual_run_completed_subscriber = only_from(subscriber, "definition manual run webapi")
    with_logging_subscriber = with_input_output_logging_subscriber(manual_run_completed_subscriber, "manual_run_definition_completed")
    return DefinitionCompletedSubscriberAdapter(with_logging_subscriber)(func)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)