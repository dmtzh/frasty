from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result
from fastapi import HTTPException

from shared.customtypes import ScheduleIdValue, TaskIdValue
from shared.domainschedule import TaskSchedule
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.tasksschedulesstore import tasks_schedules_storage
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result, AsyncResult
from shared.utils.parse import parse_value
from shared.utils.result import ResultTag
from shared.validation import InvalidId

# ---------------------------
# workflow
# ---------------------------
@dataclass(frozen=True)
class ClearTaskScheduleCommand:
    task_id: TaskIdValue
    schedule_id: ScheduleIdValue
@dataclass(frozen=True)
class ClearTaskScheduleError:
    cmd: ClearTaskScheduleCommand
    error: Any

# ==================================
# Workflow implementation
# ==================================
def parse_task_id(raw_task_id_with_checksum: str) -> Result[TaskIdValue, InvalidId]:
    return parse_value(raw_task_id_with_checksum, "task_id", TaskIdValue.from_value_with_checksum).map_error(lambda _: InvalidId())

def parse_schedule_id(raw_schedule_id_with_checksum: str) -> Result[ScheduleIdValue, InvalidId]:
    return parse_value(raw_schedule_id_with_checksum, "schedule_id", ScheduleIdValue.from_value_with_checksum).map_error(lambda _: InvalidId())

@async_result
@async_ex_to_error_result(StorageError.from_exception)
async def get_schedule(task_id: TaskIdValue, schedule_id: ScheduleIdValue) -> Result[TaskSchedule, NotFoundError]:
    schedules = await tasks_schedules_storage.get_schedules()
    opt_schedule = schedules.get(task_id)
    match opt_schedule:
        case None:
            return Result.Error(NotFoundError(f"Schedule {schedule_id} not found for task {task_id}"))
        case schedule if schedule.schedule_id == schedule_id:
            return Result.Ok(schedule)
        case _:
            return Result.Error(NotFoundError(f"Schedule {schedule_id} not found for task {task_id}"))

@coroutine_result()
async def clear_task_schedule_workflow(clear_task_schedule_handler: Callable[[ClearTaskScheduleCommand], Coroutine[Any, Any, Result]], raw_task_id_with_checksum: str, raw_schedule_id_with_checksum: str):
    task_id = await AsyncResult.from_result(parse_task_id(raw_task_id_with_checksum))
    schedule_id = await AsyncResult.from_result(parse_schedule_id(raw_schedule_id_with_checksum))
    await get_schedule(task_id, schedule_id)
    cmd = ClearTaskScheduleCommand(task_id, schedule_id)
    await async_result(clear_task_schedule_handler)(cmd).map_error(lambda err: ClearTaskScheduleError(cmd, err))
    return schedule_id

# ==================================
# API endpoint handler
# ==================================
async def handle(clear_task_schedule_handler: Callable[[ClearTaskScheduleCommand], Coroutine[Any, Any, Result]], raw_task_id_with_checksum: str, raw_schedule_id_with_checksum: str):
    res = await clear_task_schedule_workflow(clear_task_schedule_handler, raw_task_id_with_checksum, raw_schedule_id_with_checksum)
    match res:
        case Result(tag=ResultTag.OK, ok=id) if type(id) is ScheduleIdValue:
            id_with_checksum = id.to_value_with_checksum()
            return {"id": id_with_checksum}
        case Result(tag=ResultTag.ERROR, error=error):
            match error:
                case InvalidId() | NotFoundError():
                    raise HTTPException(status_code=404)
                case StorageError() | ClearTaskScheduleError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
    