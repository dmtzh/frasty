from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any, Callable

from expression import Result

from shared.customtypes import RunIdValue, TaskIdValue, DefinitionIdValue
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.task import Task
from shared.tasksstore import tasks_storage
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

@dataclass(frozen=True)
class RunTaskDefinitionCommand:
    task_id: TaskIdValue
    run_id: RunIdValue

@async_result
@async_ex_to_error_result(StorageError.from_exception)
async def get_task(task_id: TaskIdValue) -> Result[Task, NotFoundError]:
    opt_task = await tasks_storage.get(task_id)
    match opt_task:
        case None:
            return Result.Error(NotFoundError(f"Task {task_id} not found"))
        case task:
            return Result.Ok(task)

@coroutine_result()
async def handle(run_definition_handler: Callable[[DefinitionIdValue], Coroutine[Any, Any, Result]], cmd: RunTaskDefinitionCommand):
    task = await get_task(cmd.task_id)
    await async_result(run_definition_handler)(task.definition_id)
    return task.definition_id