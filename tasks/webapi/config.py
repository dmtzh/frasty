from collections.abc import Callable, Coroutine, Generator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Any

from expression import effect, Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, TaskIdValue
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, DataDto, run_action_adapter
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, map_handler, only_from
from shared.pipeline.logging import with_input_output_logging_subscriber
from shared.pipeline.types import CompletedDefinitionData, RunTaskData
from shared.utils.parse import parse_from_dict, parse_value

run_action = config.run_action

EXECUTE_TASK_ACTION = Action(ActionName("execute_task"), ActionType.SERVICE)
@dataclass(frozen=True)
class ExecuteTaskInput:
    task_id: TaskIdValue
    definition_id: DefinitionIdValue
    def to_dto(self) -> DataDto:
        return {
            "task_id": self.task_id.to_value_with_checksum(),
            "definition_id": self.definition_id.to_value_with_checksum()
        }
    @effect.result['ExecuteTaskInput', str]()
    @staticmethod
    def from_dto(dto: DataDto) -> Generator[Any, Any, 'ExecuteTaskInput']:
        task_id = yield from parse_from_dict(dto, "task_id", TaskIdValue.from_value_with_checksum)
        definition_id = yield from parse_from_dict(dto, "definition_id", DefinitionIdValue.from_value_with_checksum)
        return ExecuteTaskInput(task_id, definition_id)
async def run_execute_task_action(data: ActionData[None, ExecuteTaskInput]):
    run_action_dto = ActionData(data.run_id, data.step_id, data.config, data.input.to_dto(), data.metadata)
    run_action_res = await run_action_adapter(config.run_action)(EXECUTE_TASK_ACTION, run_action_dto)
    return run_action_res.map(lambda _: data)
def execute_task_handler(func: Callable[[ActionData[None, ExecuteTaskInput]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        EXECUTE_TASK_ACTION,
        lambda dto_list: ExecuteTaskInput.from_dto(dto_list[0])
    )(func)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
ADD_DEFINITION_URL = os.environ['ADD_DEFINITION_URL']
CHANGE_SCHEDULE_URL = os.environ['CHANGE_SCHEDULE_URL']

def run_webapi_task(task_id: TaskIdValue, run_id: RunIdValue):
    metadata = Metadata()
    metadata.set_from("run task webapi")
    data = RunTaskData(task_id, run_id, metadata)
    return config.run_task(data)

@dataclass(frozen=True)
class CompletedTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedResult

def webapi_task_completed_subscriber(func: Callable[[CompletedTaskData], Coroutine[Any, Any, Result | None]]):
    def to_completed_task_data(data: CompletedDefinitionData):
        task_id_res = parse_value(data.metadata, "task_id", Metadata.get_task_id)
        return task_id_res.map(lambda task_id: CompletedTaskData(task_id, data.run_id, data.result))
    subscriber = config.definition_completed_subscriber(None, config.RequeueChance.LOW)
    run_task_webapi_subscriber = only_from(subscriber, "run task webapi")
    with_logging_subscriber = with_input_output_logging_subscriber(run_task_webapi_subscriber, "webapi_task_completed")
    webapi_task_completed_s = map_handler(
        with_logging_subscriber,
        lambda data_res: data_res.bind(to_completed_task_data)
    )
    return DefinitionCompletedSubscriberAdapter(webapi_task_completed_s)(func)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)
