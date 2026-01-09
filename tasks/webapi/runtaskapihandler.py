from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any
from expression import Result
from fastapi import HTTPException

from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.tasksstore import tasks_storage
from shared.utils.asyncresult import AsyncResult, async_ex_to_error_result, async_result, coroutine_result
from shared.utils.parse import parse_value
from shared.utils.result import ResultTag
from shared.validation import InvalidId

# ---------------------------
# workflow
# ---------------------------
@dataclass(frozen=True)
class RunTaskError:
    error: Any

# ==================================
# Workflow implementation
# ==================================
@async_result
@async_ex_to_error_result(StorageError.from_exception)
async def get_task(task_id: TaskIdValue):
    opt_task = await tasks_storage.get(task_id)
    return Result.Ok(opt_task) if opt_task is not None else Result.Error(NotFoundError(f"Task {task_id} not found"))

@coroutine_result[InvalidId | NotFoundError | StorageError | RunTaskError]()
async def run_task_workflow(run_task_handler: Callable[[TaskIdValue], Coroutine[Any, Any, Result[RunIdValue, Any]]], raw_task_id_with_checksum: str):
    task_id = await AsyncResult.from_result(parse_value(raw_task_id_with_checksum, "task_id", TaskIdValue.from_value_with_checksum)).map_error(lambda _: InvalidId())
    await get_task(task_id)
    run_id = await async_result(run_task_handler)(task_id).map_error(RunTaskError)
    return run_id

# ==================================
# API endpoint handler
# ==================================
async def handle(run_task_handler: Callable[[TaskIdValue], Coroutine[Any, Any, Result[RunIdValue, Any]]], raw_task_id_with_checksum: str):
    res = await run_task_workflow(run_task_handler, raw_task_id_with_checksum)
    match res:
        case Result(tag=ResultTag.OK, ok=run_id) if type(run_id) is RunIdValue:
            id_with_checksum = run_id.to_value_with_checksum()
            return {"id": id_with_checksum}
        case Result(tag=ResultTag.ERROR, error=InvalidId()):
            raise HTTPException(status_code=404)
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            raise HTTPException(status_code=404)
        case Result(tag=ResultTag.ERROR):
            raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
