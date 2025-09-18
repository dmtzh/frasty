from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.customtypes import DefinitionIdValue, Error, TaskIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.task import Task, TaskName
from shared.tasksstore import tasks_storage
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

# ---------------------------
# inputs
# ---------------------------
@dataclass(frozen=True)
class AddTaskResource:
    name: str
    definition: list[dict[str, str]]

# ---------------------------
# workflow
# ---------------------------
class TaskNameMissing:
    '''Task name is missing'''
@dataclass(frozen=True)
class InputValidationError:
    error: TaskNameMissing
@dataclass(frozen=True)
class DefinitionValidationError:
    errors: list
class AddDefinitionError(Error):
    '''Add definition error'''
class TasksStorageError(StorageError):
    '''Unexpected tasks storage error'''
type WorkflowError = InputValidationError | DefinitionValidationError | AddDefinitionError | TasksStorageError

# ==================================
# Workflow implementation
# ==================================
@async_result
@make_async
def validate_task_name(raw_task_name: str) -> Result[TaskName, TaskNameMissing]:
    opt_task_name = TaskName.parse(raw_task_name)
    match opt_task_name:
        case None:
            return Result.Error(TaskNameMissing())
        case task_name:
            return Result.Ok(task_name)

@async_result
@async_ex_to_error_result(TasksStorageError.from_exception)
def apply_add_task(id: TaskIdValue, task: Task):
    return tasks_storage.add(id, task)

@coroutine_result[WorkflowError]()
async def add_task_workflow(add_definition_handler: Callable[[list[dict[str, str]]], Coroutine[Any, Any, Result[DefinitionIdValue, DefinitionValidationError | AddDefinitionError]]], resource: AddTaskResource):
    task_name = await validate_task_name(resource.name).map_error(InputValidationError)
    definition_id = await async_result(add_definition_handler)(resource.definition)
    id = TaskIdValue.new_id()
    task = Task(name=task_name, definition_id=definition_id, schedule_id=None)
    await apply_add_task(id, task)
    return id