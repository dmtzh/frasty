from collections.abc import Callable
import functools
from typing import Any, Coroutine

import aiocron
from expression import Result

from shared.customtypes import TaskIdValue, ScheduleIdValue
from shared.domainschedule import TaskSchedule
from shared.infrastructure.storage.repository import Repository

class ScheduledTasks:
    def __init__(self, schedules_storage: Repository[ScheduleIdValue, aiocron.Cron], schedule_func: Callable[[TaskIdValue, TaskSchedule], Coroutine[Any, Any, Result]]):
        self._schedules_storage = schedules_storage
        self._schedule_func = schedule_func

    def add(self, task_id: TaskIdValue, schedule: TaskSchedule):
        schedule_func = functools.partial(self._schedule_func, task_id, schedule)
        cron_scheduled_task = aiocron.crontab(schedule.cron, func=schedule_func)
        self._schedules_storage.add(schedule.schedule_id, cron_scheduled_task)

    def remove(self, schedule: TaskSchedule):
        opt_cron_scheduled_task = self._schedules_storage.get(schedule.schedule_id)
        match opt_cron_scheduled_task:
            case None:
                return None
            case cron_scheduled_task:
                cron_scheduled_task.stop()
                self._schedules_storage.delete(schedule.schedule_id)
                return cron_scheduled_task