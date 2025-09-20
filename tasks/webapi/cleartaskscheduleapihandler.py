import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import aiohttp
from expression import Result
from fastapi import HTTPException

from shared.customtypes import ScheduleIdValue, TaskIdValue, Error
from shared.infrastructure.storage.repository import NotFoundError, StorageError, NotFoundException
from shared.task import Task
from shared.tasksstore import tasks_storage
from shared.utils.asyncresult import AsyncResult, coroutine_result, async_ex_to_error_result, async_result
from shared.utils.parse import parse_from_str
from shared.utils.result import ResultTag
from shared.validation import InvalidId

import config

# ---------------------------
# workflow
# ---------------------------
@dataclass(frozen=True)
class ClearTaskScheduleCommand:
    task_id: TaskIdValue
    schedule_id: ScheduleIdValue
class ClearScheduleUnexpectedError(Error):
    '''Unexpected clear schedule error'''
@dataclass(frozen=True)
class ClearTaskScheduleError:
    cmd: ClearTaskScheduleCommand
    error: Any

# ==================================
# Workflow implementation
# ==================================
def validate_task_id(raw_id_with_checksum: str) -> Result[TaskIdValue, InvalidId]:
    return parse_from_str(raw_id_with_checksum, "task_id", TaskIdValue.from_value_with_checksum).map_error(lambda _: InvalidId())

def parse_schedule_id(raw_schedule_id_with_checksum: str) -> Result[ScheduleIdValue, InvalidId]:
    return parse_from_str(raw_schedule_id_with_checksum, "schedule_id", ScheduleIdValue.from_value_with_checksum).map_error(lambda _: InvalidId())

@async_result
@async_ex_to_error_result(StorageError.from_exception)
async def get_task_with_schedule_id(task_id: TaskIdValue, schedule_id: ScheduleIdValue) -> Result[Task, NotFoundError]:
    opt_task = await tasks_storage.get(task_id)
    match opt_task:
        case None:
            return Result.Error(NotFoundError(f"Task {task_id} not found"))
        case task if task.schedule_id == schedule_id:
            return Result.Ok(task)
        case _:
            return Result.Error(NotFoundError(f"Task {task_id} with schedule {schedule_id} not found"))

@async_result
@async_ex_to_error_result(StorageError.from_exception)
@async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
@tasks_storage.with_storage
def apply_clear_schedule_id(task: Task | None, schedule_id: ScheduleIdValue):
    if task is None:
        raise NotFoundException()
    if task.schedule_id != schedule_id:
        raise NotFoundException()
    new_task = Task(task.name, task.definition_id, None)
    return None, new_task

@coroutine_result()
async def clear_schedule_workflow(clear_schedule_handler: Callable[[ClearTaskScheduleCommand], Coroutine[Any, Any, Result]], raw_id_with_checksum: str, raw_schedule_id_with_checksum: str):
    task_id = await AsyncResult.from_result(validate_task_id(raw_id_with_checksum))
    schedule_id = await AsyncResult.from_result(parse_schedule_id(raw_schedule_id_with_checksum))
    await get_task_with_schedule_id(task_id, schedule_id)
    cmd = ClearTaskScheduleCommand(task_id, schedule_id)
    await async_result(clear_schedule_handler)(cmd).map_error(lambda err: ClearTaskScheduleError(cmd, err))
    await apply_clear_schedule_id(task_id, schedule_id)
    return schedule_id

# ==================================
# API endpoint handler
# ==================================
@async_ex_to_error_result(ClearScheduleUnexpectedError.from_exception)
async def http_request_clear_schedule_handler(cmd: ClearTaskScheduleCommand) -> Result[None, NotFoundError | ClearScheduleUnexpectedError]:
    task_id_url_part = cmd.task_id.to_value_with_checksum()
    schedule_id_url_part = cmd.schedule_id.to_value_with_checksum()
    clear_schedule_url = urljoin(config.CHANGE_SCHEDULE_URL + "/", f"{task_id_url_part}/{schedule_id_url_part}")
    timeout_15_seconds = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.delete(clear_schedule_url, timeout=timeout_15_seconds) as response:
                match response.status:
                    case 202:
                        return Result.Ok(None)
                    case 404:
                        return Result.Error(NotFoundError(f"Task {cmd.task_id} with schedule {cmd.schedule_id} not found"))
                    case error_status:
                        str_response = await response.text()
                        return Result.Error(ClearScheduleUnexpectedError(f"{error_status}, {str_response}"))
        except asyncio.TimeoutError:
            return Result.Error(ClearScheduleUnexpectedError(f"Request timeout {timeout_15_seconds.total} seconds when connect to {clear_schedule_url}"))
        except aiohttp.client_exceptions.ClientConnectorError:
            return Result.Error(ClearScheduleUnexpectedError(f"Cannot connect to {clear_schedule_url})"))

async def handle(raw_id_with_checksum: str, raw_schedule_id_with_checksum: str):
    res = await clear_schedule_workflow(http_request_clear_schedule_handler, raw_id_with_checksum, raw_schedule_id_with_checksum)
    match res:
        case Result(tag=ResultTag.OK, ok=id) if type(id) is ScheduleIdValue:
            id_with_checksum = id.to_value_with_checksum()
            return {"id": id_with_checksum}
        case Result(tag=ResultTag.ERROR, error=error):
            match error:
                case InvalidId() | NotFoundError():
                    raise HTTPException(status_code=404)
                case StorageError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
                case ClearTaskScheduleError(cmd=_, error=NotFoundError()):
                    raise HTTPException(status_code=404)
                case ClearTaskScheduleError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
                