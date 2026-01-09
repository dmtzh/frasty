from collections.abc import Callable, Coroutine
import os
from typing import Any

from expression import Result

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.customtypes import TaskIdValue
from shared.executedefinitionaction import ExecuteDefinitionInput, run_execute_definition_action
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory
from shared.pipeline.handlers import to_continuation
from shared.pipeline.logging import with_input_output_logging
from shared.pipeline.types import RunTaskData
from shared.utils.parse import parse_from_dict

EXECUTE_TASK_ACTION = Action(ActionName("execute_task"), ActionType.SERVICE)
def execute_task_handler(func: Callable[[ActionData[None, TaskIdValue]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        EXECUTE_TASK_ACTION,
        lambda dto_list: parse_from_dict(dto_list[0], "task_id", TaskIdValue.from_value_with_checksum)
    )(func)

def execute_definition(data: ActionData[None, ExecuteDefinitionInput]):
    return run_execute_definition_action(config.run_action, data)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def run_task_handler(func: Callable[[RunTaskData], Coroutine[Any, Any, Result | None]]):
    handler = to_continuation(func)
    handler_with_logging = with_input_output_logging(handler, "run_task")
    return config.run_task_handler(handler_with_logging)

run_definition = config.run_definition

publish_completed_definition = config.publish_completed_definition

app = config.create_faststream_app()