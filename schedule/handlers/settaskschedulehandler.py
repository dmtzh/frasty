from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.customtypes import TaskIdValue
from shared.domainschedule import TaskSchedule
from shared.infrastructure.storage.repository import StorageError
from shared.tasksschedulesstore import tasks_schedules_storage
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

@dataclass(frozen=True)
class SetTaskScheduleCommand:
    task_id: TaskIdValue
    schedule: TaskSchedule

@async_result
@async_ex_to_error_result(StorageError.from_exception)
async def get_schedule(task_id: TaskIdValue):
    schedules = await tasks_schedules_storage.get_schedules()
    return schedules.get(task_id)

@async_result
@async_ex_to_error_result(StorageError.from_exception)
def apply_set_schedule(task_id: TaskIdValue, schedule: TaskSchedule):
    return tasks_schedules_storage.set_task_schedule(task_id, schedule)

@coroutine_result()
async def handle(set_task_schedule_handler: Callable[[TaskSchedule | None, TaskSchedule], Coroutine[Any, Any, Result]], cmd: SetTaskScheduleCommand):
    opt_curr_schedule = await get_schedule(cmd.task_id)
    new_schedule = await apply_set_schedule(cmd.task_id, cmd.schedule)
    await async_result(set_task_schedule_handler)(opt_curr_schedule, new_schedule)
    return new_schedule