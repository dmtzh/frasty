from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, StepIdValue
from shared.definition import Definition, DefinitionAdapter
from shared.domaindefinition import StepDefinition
from shared.domainrunning import RunningDefinitionState
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.pipeline.actionhandler import ActionData, ActionDataDto, ActionHandlerFactory
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, only_from
from shared.pipeline.logging import with_input_output_logging_subscriber
from shared.pipeline.types import CompletedDefinitionData, StepData
from shared.utils.parse import parse_value
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData
from stepdefinitions.viber import SendToViberChannel
from stephandlers.getcontentfromjson.definition import GetContentFromJson

EXECUTE_DEFINITION_ACTION = Action(ActionName("execute_definition"), ActionType.CORE)

class ExecuteDefinitionData(ActionData[None, Definition]):
    def to_dto(self) -> ActionDataDto:
        run_id_str = self.run_id.to_value_with_checksum()
        step_id_str = self.step_id.to_value_with_checksum()
        data_dto = DefinitionAdapter.to_list(self.input)
        metadata_dict = self.metadata.to_dict()
        return ActionDataDto(run_id_str, step_id_str, data_dto, metadata_dict)
    
    @staticmethod
    def validate_input(data: dict | list):
        list_data_res = parse_value(data, "data", lambda lst: lst if isinstance(lst, list) else None)
        definition_res = list_data_res.bind(lambda lst: DefinitionAdapter.from_list(lst).map_error(str))
        return definition_res

def run_execute_definition_action(definition: Definition):
    metadata = Metadata()
    metadata.set_from("action manual run webapi")
    run_id = RunIdValue.new_id()
    step_id = StepIdValue.new_id()
    data = ExecuteDefinitionData(run_id, step_id, None, definition, metadata)
    action_name = EXECUTE_DEFINITION_ACTION.get_name()
    dto_data = data.to_dto()
    return config.run_action(action_name, dto_data)

def execute_definition_handler(func: Callable[[ActionData[None, Definition]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        EXECUTE_DEFINITION_ACTION,
        ExecuteDefinitionData.validate_input
    )(func)

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
    data = StepData(manual_run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)
    return config.run_step(data)

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