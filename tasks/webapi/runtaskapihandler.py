from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any
from expression import Result
from fastapi import HTTPException

from infrastructure import rabbitruntask as rabbit_task
from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.task import Task
from shared.tasksstore import tasks_storage
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import async_catch_ex, async_ex_to_error_result, async_result, coroutine_result
from shared.utils.result import ResultTag
from shared.validation import InvalidId
from webapi.webapitaskrunstate import WebApiTaskRunState

from config import rabbit_client
from webapitaskrunstore import web_api_task_run_storage

# ---------------------------
# workflow
# ---------------------------
class TasksStorageError(StorageError):
    '''Unexpected tasks storage error'''
@dataclass(frozen=True)
class RunTaskCommand:
    task_id: TaskIdValue
    run_id: RunIdValue
class WebApiTaskRunStorageError(StorageError):
    '''Unexpected web api task run storage error'''
@dataclass(frozen=True)
class RunTaskError:
    cmd: RunTaskCommand
    error: Any
type WorkflowError = InvalidId | TasksStorageError | NotFoundError | WebApiTaskRunStorageError | RunTaskError

# ==================================
# Workflow implementation
# ==================================
@async_result
@make_async
def validate_id(raw_id_with_checksum: str) -> Result[TaskIdValue, InvalidId]:
    opt_id = TaskIdValue.from_value_with_checksum(raw_id_with_checksum)
    match opt_id:
        case None:
            return Result.Error(InvalidId())
        case id:
            return Result.Ok(id)

@async_result
@async_ex_to_error_result(TasksStorageError.from_exception)
async def get_task(task_id: TaskIdValue) -> Result[Task, NotFoundError]:
    opt_task = await tasks_storage.get(task_id)
    match opt_task:
        case None:
            return Result.Error(NotFoundError(f"Task {task_id} not found"))
        case task:
            return Result.Ok(task)

@async_result
@async_ex_to_error_result(WebApiTaskRunStorageError.from_exception)
@web_api_task_run_storage.with_storage
def apply_add_webapi_task_run(state: WebApiTaskRunState | None):
    if state is not None:
        raise ValueError("web api task run state already exists")
    state = WebApiTaskRunState.create_running()
    return (None, state)

@coroutine_result[WorkflowError]()
async def run_task_workflow(run_task_handler: Callable[[RunTaskCommand], Coroutine[Any, Any, Result]], raw_id_with_checksum: str):
    task_id = await validate_id(raw_id_with_checksum)
    await get_task(task_id)
    run_id = RunIdValue.new_id()
    await apply_add_webapi_task_run(task_id, run_id)
    cmd = RunTaskCommand(task_id, run_id)
    await async_result(run_task_handler)(cmd).map_error(lambda err: RunTaskError(cmd, err))
    return run_id

async def clean_up_failed_run(error: WorkflowError):
    match error:
        case RunTaskError(cmd=cmd, error=error):
            await async_catch_ex(web_api_task_run_storage.delete)(cmd.task_id, cmd.run_id)

# ==================================
# API endpoint handler
# ==================================
@async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
def _rabbit_run_task_handler(cmd: RunTaskCommand):
    return rabbit_task.run(rabbit_client, cmd.task_id, cmd.run_id, "webapi", {})

async def handle(raw_id_with_checksum: str):
    res = await run_task_workflow(_rabbit_run_task_handler, raw_id_with_checksum)
    if res.is_error():
        await clean_up_failed_run(res.error)
    match res:
        case Result(tag=ResultTag.OK, ok=id) if type(id) is RunIdValue:
            id_with_checksum = id.to_value_with_checksum()
            return {"id": id_with_checksum}
        case Result(tag=ResultTag.ERROR, error=error):
            match error:
                case InvalidId() | NotFoundError():
                    raise HTTPException(status_code=404)
                case TasksStorageError() | WebApiTaskRunStorageError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
                case RunTaskError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")