from collections.abc import Callable, Coroutine, Generator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Any

from expression import effect
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.definition import Definition, DefinitionAdapter
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, run_action_adapter
from shared.utils.parse import parse_from_dict
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData
from stepdefinitions.viber import SendToViberChannel
from stephandlers.getcontentfromjson.definition import GetContentFromJson

run_action = config.run_action

MANUAL_RUN_ACTION = Action(ActionName("manual_run"), ActionType.SERVICE)
@dataclass(frozen=True)
class ManualRunInput:
    definition: Definition
    def to_dto(self):
        return {"definition": DefinitionAdapter.to_list(self.definition)}
    @effect.result['ManualRunInput', str]()
    @staticmethod
    def from_dto(dto: dict[str, Any]) -> Generator[Any, Any, 'ManualRunInput']:
        list_definition = yield from parse_from_dict(dto, "definition", lambda lst: lst if isinstance(lst, list) else None)
        definition = yield from DefinitionAdapter.from_list(list_definition).map_error(str)
        return ManualRunInput(definition)
async def run_manual_run_action(data: ActionData[None, ManualRunInput]):
    run_action_dto = ActionData(data.run_id, data.step_id, data.config, data.input.to_dto(), data.metadata)
    run_action_res = await run_action_adapter(config.run_action)(MANUAL_RUN_ACTION, run_action_dto)
    return run_action_res.map(lambda _: data)
def manual_run_handler(func: Callable[[ActionData[None, ManualRunInput]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        MANUAL_RUN_ACTION,
        lambda dto_list: ManualRunInput.from_dto(dto_list[0])
    )(func)

COMPLETE_MANUAL_RUN_ACTION = Action(ActionName("complete_manual_run"), ActionType.SERVICE)
def complete_manual_run_handler(func: Callable[[ActionData[None, CompletedResult]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        COMPLETE_MANUAL_RUN_ACTION,
        complete_manual_run_handler_input_validator
    )(func)
def complete_manual_run_handler_input_validator(data: list[dict[str, Any]]):
    return CompletedResultAdapter.from_dict(data[0])

step_definitions: list[type[StepDefinition]] = [
    RequestUrl, FilterSuccessResponse,
    FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml,
    FetchNewData, SendToViberChannel,
    GetContentFromJson
]
for step_definition in step_definitions:
    step_definition_creators_storage.add(step_definition)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)