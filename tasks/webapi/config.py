from collections.abc import Callable, Coroutine, Generator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Any

from expression import effect
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, TaskIdValue
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, DataDto, run_action_adapter
from shared.utils.parse import parse_from_dict

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)
