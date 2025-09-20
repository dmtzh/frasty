from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.customtypes import ScheduleIdValue, TaskIdValue
from shared.domainschedule import TaskSchedule
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.tasksschedulesstore import tasks_schedules_storage
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result


@dataclass(frozen=True)
class ClearTaskScheduleCommand:
    task_id: TaskIdValue
    schedule_id: ScheduleIdValue

@async_result
@async_ex_to_error_result(StorageError.from_exception)
async def apply_clear_task_schedule(cmd: ClearTaskScheduleCommand) -> Result[TaskSchedule, NotFoundError]:
    opt_schedule = await tasks_schedules_storage.clear_task_schedule(cmd.task_id, cmd.schedule_id)
    match opt_schedule:
        case None:
            return Result.Error(NotFoundError(f"Schedule {cmd.schedule_id} not found for task {cmd.task_id}"))
        case schedule:
            return Result.Ok(schedule)

@coroutine_result()
async def handle(clear_task_schedule_handler: Callable[[TaskSchedule], Coroutine[Any, Any, Result]], cmd: ClearTaskScheduleCommand):
    cleared_task_schedule = await apply_clear_task_schedule(cmd)
    await async_result(clear_task_schedule_handler)(cleared_task_schedule)
    return cleared_task_schedule