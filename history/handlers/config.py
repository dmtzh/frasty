from collections.abc import Callable, Coroutine, Generator
from dataclasses import dataclass
import os
from typing import Any

from expression import effect

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.customtypes import DefinitionIdValue, TaskIdValue
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, DataDto
from shared.utils.parse import parse_from_dict

ADD_TASK_RESULT_TO_HISTORY_ACTION = Action(ActionName("add_task_result_to_history"), ActionType.SERVICE)
@dataclass(frozen=True)
class AddTaskResultToHistoryConfig:
    task_id: TaskIdValue
    execution_id: DefinitionIdValue
    @effect.result['AddTaskResultToHistoryConfig', str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, 'AddTaskResultToHistoryConfig']:
        task_id = yield from parse_from_dict(data, "task_id", TaskIdValue.from_value_with_checksum)
        execution_id = yield from parse_from_dict(data, "execution_id", DefinitionIdValue.from_value_with_checksum)
        return AddTaskResultToHistoryConfig(task_id, execution_id)
def add_task_result_to_history_input_validator(data: list[DataDto]):
    return CompletedResultAdapter.from_dict(data[0])
def add_task_result_to_history_handler(func: Callable[[ActionData[AddTaskResultToHistoryConfig, CompletedResult]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create(
        ADD_TASK_RESULT_TO_HISTORY_ACTION,
        AddTaskResultToHistoryConfig.from_dict,
        add_task_result_to_history_input_validator
    )(func)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

app = config.create_faststream_app()