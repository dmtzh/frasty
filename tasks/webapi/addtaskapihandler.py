from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.customtypes import DefinitionIdValue, TaskIdValue
from shared.task import Task, TaskName
from shared.utils.asyncresult import AsyncResult, async_result, coroutine_result
from shared.utils.parse import parse_value

# ---------------------------
# inputs
# ---------------------------
@dataclass(frozen=True)
class AddTaskResource:
    name: str
    definition: list[dict[str, Any]]
@dataclass(frozen=True)
class AddTaskRequest():
    resource: AddTaskResource

# ---------------------------
# workflow
# ---------------------------
class TaskNameMissing:
    '''Task name is missing'''
@dataclass(frozen=True)
class AddDefinitionError:
    '''Add definition error'''
    error: Any
@dataclass(frozen=True)
class AddToStorageError:
    '''Add to storage error'''
    error: Any

# ==================================
# Workflow implementation
# ==================================
@coroutine_result[TaskNameMissing | AddDefinitionError | AddToStorageError]()
async def add_task_workflow[TErr, TErr1](
        add_definition_handler: Callable[[list[dict[str, Any]]], Coroutine[Any, Any, Result[DefinitionIdValue, TErr]]],
        add_to_storage_handler: Callable[[TaskIdValue, Task], Coroutine[Any, Any, Result[None, TErr1]]],
        resource: AddTaskResource):
    task_name = await AsyncResult.from_result(parse_value(resource.name, "name", TaskName.parse)).map_error(lambda _: TaskNameMissing())
    definition_id = await async_result(add_definition_handler)(resource.definition).map_error(AddDefinitionError)
    task_id = TaskIdValue.new_id()
    task = Task(name=task_name, definition_id=definition_id, schedule_id=None)
    await async_result(add_to_storage_handler)(task_id, task).map_error(AddToStorageError)
    return task_id

# ==================================
# API endpoint handler
# ==================================
