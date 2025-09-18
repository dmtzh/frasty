from dataclasses import dataclass

from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

from shared.domainschedule import CronSchedule, TaskSchedule
from shared.customtypes import TaskIdValue, ScheduleIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.tasksschedulesstore import tasks_schedules_storage
from shared.utils.asyncresult import AsyncResult, async_ex_to_error_result, async_result, coroutine_result
from shared.utils.result import ResultTag
from shared.validation import InvalidId

# ---------------------------
# inputs
# ---------------------------
@dataclass(frozen=True)
class SetScheduleResource:
    task_id: str
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

# ==================================
# Workflow implementation
# ==================================
def parse_task_id(raw_task_id: str) -> Result[TaskIdValue, InvalidId]:
    opt_task_id = TaskIdValue.from_value_with_checksum(raw_task_id)
    match opt_task_id:
        case None:
            return Result.Error(InvalidId())
        case task_id:
            return Result.Ok(task_id)

def parse_cron_schedule(raw_cron: str) -> Result[CronSchedule, InvalidSchedule]:
    opt_cron_schedule = CronSchedule.parse(raw_cron)
    match opt_cron_schedule:
        case None:
            return Result.Error(InvalidSchedule())
        case cron_schedule:
            return Result.Ok(cron_schedule)

@async_result
@async_ex_to_error_result(StorageError.from_exception)
def apply_set_task_schedule(task_id: TaskIdValue, schedule: TaskSchedule):
    return tasks_schedules_storage.set_task_schedule(task_id, schedule)

@coroutine_result()
async def set_schedule_workflow(resource: SetScheduleResource):
    task_id = await AsyncResult.from_result(parse_task_id(resource.task_id))
    cron_schedule = await AsyncResult.from_result(parse_cron_schedule(resource.cron))
    schedule_id = ScheduleIdValue.new_id()
    task_schedule = TaskSchedule(schedule_id, cron_schedule)
    await apply_set_task_schedule(task_id, task_schedule)
    return schedule_id

# ==================================
# API endpoint handler
# ==================================
async def handle(request: SetScheduleRequest):
    res = await set_schedule_workflow(request.resource)
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
