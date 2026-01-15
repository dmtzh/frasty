import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import aiohttp
from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

from shared.customtypes import ScheduleIdValue, TaskIdValue, Error
from shared.infrastructure.storage.repository import NotFoundError, StorageError, NotFoundException
from shared.task import Task
from shared.tasksstore import TasksStore
from shared.utils.asyncresult import AsyncResult, coroutine_result, async_ex_to_error_result, async_result
from shared.utils.result import ResultTag
from shared.validation import InvalidId

import config

# ---------------------------
# inputs
# ---------------------------
@dataclass(frozen=True)
class SetScheduleResource:
    cron: str

@dataclass(frozen=True)
class SetScheduleRequest:
    resource: SetScheduleResource

# ---------------------------
# workflow
# ---------------------------
@dataclass(frozen=True)
class ScheduleValidationError:
    errors: list
class SetScheduleUnexpectedError(Error):
    '''Unexpected set schedule error'''

# ==================================
# Workflow implementation
# ==================================
def validate_id(raw_id_with_checksum: str) -> Result[TaskIdValue, InvalidId]:
    opt_id = TaskIdValue.from_value_with_checksum(raw_id_with_checksum)
    match opt_id:
        case None:
            return Result.Error(InvalidId())
        case id:
            return Result.Ok(id)

@coroutine_result()
async def set_schedule_workflow(
    set_schedule_handler: Callable[[TaskIdValue, SetScheduleResource], Coroutine[Any, Any, Result[ScheduleIdValue, ScheduleValidationError | SetScheduleUnexpectedError]]],
    tasks_storage: TasksStore,
    raw_id_with_checksum: str,
    resource: SetScheduleResource):
    @async_result
    @async_ex_to_error_result(StorageError.from_exception)
    async def get_task(task_id: TaskIdValue) -> Result[Task, NotFoundError]:
        opt_task = await tasks_storage.get(task_id)
        match opt_task:
            case None:
                return Result.Error(NotFoundError(f"Task {task_id} not found"))
            case task:
                return Result.Ok(task)
    @async_result
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    @tasks_storage.with_storage
    def apply_set_schedule_id(task: Task | None, schedule_id: ScheduleIdValue):
        if task is None:
            raise NotFoundException()
        new_task = Task(task.name, task.definition_id, schedule_id)
        return None, new_task
    
    task_id = await AsyncResult.from_result(validate_id(raw_id_with_checksum))
    await get_task(task_id)
    schedule_id = await async_result(set_schedule_handler)(task_id, resource)
    await apply_set_schedule_id(task_id, schedule_id)
    return schedule_id

# ==================================
# API endpoint handler
# ==================================
@async_ex_to_error_result(SetScheduleUnexpectedError.from_exception)
async def http_request_set_schedule_handler(id: TaskIdValue, resource: SetScheduleResource) -> Result[ScheduleIdValue, ScheduleValidationError | SetScheduleUnexpectedError]:
    task_id_url_part = id.to_value_with_checksum()
    set_schedule_url = urljoin(config.CHANGE_SCHEDULE_URL + "/", task_id_url_part)
    timeout_15_seconds = aiohttp.ClientTimeout(total=15)
    json_data = {"resource": {"cron": resource.cron}}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(set_schedule_url, json=json_data, timeout=timeout_15_seconds) as response:
                match response.status:
                    case 202:
                        json_response = await response.json()
                        schedule_id_with_checksum = json_response.get("id")
                        opt_schedule_id = ScheduleIdValue.from_value_with_checksum(schedule_id_with_checksum)
                        match opt_schedule_id:
                            case None:
                                return Result.Error(SetScheduleUnexpectedError(f"Unexpected response when set schedule: {json_response}"))
                            case definition_id:
                                return Result.Ok(definition_id)
                    case 422:
                        json_response = await response.json()
                        errors = json_response["detail"]
                        return Result.Error(ScheduleValidationError(errors))
                    case error_status:
                        str_response = await response.text()
                        return Result.Error(SetScheduleUnexpectedError(f"{error_status}, {str_response}"))
        except asyncio.TimeoutError:
            return Result.Error(SetScheduleUnexpectedError(f"Request timeout {timeout_15_seconds.total} seconds when connect to {set_schedule_url}"))
        except aiohttp.client_exceptions.ClientConnectorError:
            return Result.Error(SetScheduleUnexpectedError(f"Cannot connect to {set_schedule_url})"))

async def handle(tasks_storage: TasksStore, raw_id_with_checksum: str, request: SetScheduleRequest):
    res = await set_schedule_workflow(http_request_set_schedule_handler, tasks_storage, raw_id_with_checksum, request.resource)
    match res:
        case Result(tag=ResultTag.OK, ok=id) if type(id) is ScheduleIdValue:
            id_with_checksum = id.to_value_with_checksum()
            return {"id": id_with_checksum}
        case Result(tag=ResultTag.ERROR, error=error):
            match error:
                case InvalidId() | NotFoundError():
                    raise HTTPException(status_code=404)
                case ScheduleValidationError(errors=errors):
                    raise RequestValidationError(errors)
                case SetScheduleUnexpectedError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
                case StorageError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")