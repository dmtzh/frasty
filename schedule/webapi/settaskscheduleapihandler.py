from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

from shared.domainschedule import CronSchedule, TaskSchedule
from shared.customtypes import TaskIdValue, ScheduleIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.utils.asyncresult import AsyncResult, async_result, coroutine_result
from shared.utils.parse import parse_from_str
from shared.utils.result import ResultTag
from shared.validation import InvalidId

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
class InvalidSchedule:
    '''Invalid schedule value'''
@dataclass(frozen=True)
class SetTaskScheduleCommand:
    task_id: TaskIdValue
    schedule: TaskSchedule
@dataclass(frozen=True)
class SetTaskScheduleError:
    cmd: SetTaskScheduleCommand
    error: Any

# ==================================
# Workflow implementation
# ==================================
def parse_task_id(raw_task_id: str) -> Result[TaskIdValue, InvalidId]:
    return parse_from_str(raw_task_id, "task_id", TaskIdValue.from_value_with_checksum).map_error(lambda _: InvalidId())

def parse_cron_schedule(raw_cron: str) -> Result[CronSchedule, InvalidSchedule]:
    return parse_from_str(raw_cron, "cron", CronSchedule.parse).map_error(lambda _: InvalidSchedule())

@coroutine_result()
async def set_schedule_workflow(set_task_schedule_handler: Callable[[SetTaskScheduleCommand], Coroutine[Any, Any, Result]], raw_task_id_with_checksum: str, resource: SetScheduleResource):
    task_id = await AsyncResult.from_result(parse_task_id(raw_task_id_with_checksum))
    cron_schedule = await AsyncResult.from_result(parse_cron_schedule(resource.cron))
    schedule_id = ScheduleIdValue.new_id()
    task_schedule = TaskSchedule(schedule_id, cron_schedule)
    cmd = SetTaskScheduleCommand(task_id, task_schedule)
    await async_result(set_task_schedule_handler)(cmd).map_error(lambda err: SetTaskScheduleError(cmd, err))
    return schedule_id

# ==================================
# API endpoint handler
# ==================================
async def handle(set_task_schedule_handler: Callable[[SetTaskScheduleCommand], Coroutine[Any, Any, Result]], raw_task_id_with_checksum: str, request: SetScheduleRequest):
    res = await set_schedule_workflow(set_task_schedule_handler, raw_task_id_with_checksum, request.resource)
    match res:
        case Result(tag=ResultTag.OK, ok=id) if type(id) is ScheduleIdValue:
            id_with_checksum = id.to_value_with_checksum()
            return {"id": id_with_checksum}
        case Result(tag=ResultTag.ERROR, error=InvalidId()):
            errors = [{"type": "value_error", "loc": ["body", "resource", "task_id"], "msg": "Invalid task id"}]
            raise RequestValidationError(errors)
        case Result(tag=ResultTag.ERROR, error=InvalidSchedule()):
            errors = [{"type": "value_error", "loc": ["body", "resource", "cron"], "msg": "Invalid cron expression"}]
            raise RequestValidationError(errors)
        case Result(tag=ResultTag.ERROR, error=StorageError()):
            raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
