from collections.abc import Callable
from contextlib import asynccontextmanager
import os

from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
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

run_step = config.run_step

class definition_completed_subscriber[T](config.definition_completed_subscriber[T]):
    def __init__(self, input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], T]):
        super().__init__(input_adapter, None, config.RequeueChance.LOW)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)