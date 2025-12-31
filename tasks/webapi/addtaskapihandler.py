from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import functools
from typing import Any

from expression import Result

from shared.customtypes import DefinitionIdValue, TaskIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.task import Task, TaskName
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import lift_param

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

# ==================================
# Workflow implementation
# ==================================
async def add_task_workflow[TErr](
        add_definition_handler: Callable[[list[dict[str, Any]]], Coroutine[Any, Any, Result[DefinitionIdValue, TErr]]],
        add_to_storage_handler: Callable[[TaskIdValue, Task], Coroutine[Any, Any, None]],
        resource: AddTaskResource) -> Result[TaskIdValue, TaskNameMissing | TErr | StorageError]:
    opt_task_name = TaskName.parse(resource.name)
    if opt_task_name is None:
        return Result.Error(TaskNameMissing())
    definition_id_res = await add_definition_handler(resource.definition)
    task_res = definition_id_res.map(lambda definition_id: Task(name=opt_task_name, definition_id=definition_id, schedule_id=None))
    id = TaskIdValue.new_id()
    apply_add_task = functools.partial(async_ex_to_error_result(StorageError.from_exception)(add_to_storage_handler), id)
    add_task_res = await lift_param(apply_add_task)(task_res)
    return add_task_res.map(lambda _: id)

# ==================================
# API endpoint handler
# ==================================
