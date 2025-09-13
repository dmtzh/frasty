from collections.abc import Callable
from expression import Result
from fastapi import HTTPException

from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result
from shared.utils.result import ResultTag
from shared.validation import InvalidId

from webapitaskrunstate import WebApiTaskRunState
from webapitaskrunstore import web_api_task_run_storage

# ---------------------------
# workflow
# ---------------------------
class WebApiTaskRunStorageError(StorageError):
    '''Unexpected web api task run storage error'''

# ==================================
# Workflow implementation
# ==================================
@async_result
@make_async
def parse_id_from_value_with_checksum[T](id_parser: Callable[[str], T | None], raw_id_with_checksum: str) -> Result[T, InvalidId]:
    opt_id = id_parser(raw_id_with_checksum)
    match opt_id:
        case None:
            return Result.Error(InvalidId())
        case valid_id:
            return Result.Ok(valid_id)

@async_result
@async_ex_to_error_result(WebApiTaskRunStorageError.from_exception)
async def get_state(task_id: TaskIdValue, run_id: RunIdValue) -> Result[WebApiTaskRunState, NotFoundError]:
    opt_state = await web_api_task_run_storage.get(task_id, run_id)
    match opt_state:
        case None:
            return Result.Error(NotFoundError(f"State for task {task_id} with run_id {run_id} not found"))
        case state:
            return Result.Ok(state)

@coroutine_result()
async def get_run_state_workflow(raw_id_with_checksum: str, raw_run_id_with_checksum: str):
    task_id = await parse_id_from_value_with_checksum(TaskIdValue.from_value_with_checksum, raw_id_with_checksum)
    run_id = await parse_id_from_value_with_checksum(RunIdValue.from_value_with_checksum, raw_run_id_with_checksum)
    state = await get_state(task_id, run_id)
    return state

# ==================================
# API endpoint handler
# ==================================
async def handle(raw_id_with_checksum: str, raw_run_id_with_checksum: str):
    res = await get_run_state_workflow(raw_id_with_checksum, raw_run_id_with_checksum)
    match res:
        case Result(tag=ResultTag.OK, ok=state) if type(state) is WebApiTaskRunState:
            return state
        case Result(tag=ResultTag.ERROR, error=error):
            match error:
                case InvalidId() | NotFoundError():
                    raise HTTPException(status_code=404)
                case WebApiTaskRunStorageError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
