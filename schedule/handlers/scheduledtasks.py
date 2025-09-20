from collections.abc import Callable
import functools
from typing import Any, Coroutine

import aiocron
from expression import Result

from shared.customtypes import ScheduleIdValue
from shared.domainschedule import TaskSchedule
from shared.infrastructure.storage.repository import Repository

class ScheduledTasks:
    def __init__(self, schedules_storage: Repository[ScheduleIdValue, aiocron.Cron]):
        self._schedules_storage = schedules_storage

    def add(self, schedule: TaskSchedule, schedule_func: Callable[[TaskSchedule], Coroutine[Any, Any, Result]]):
        cron_schedule_func = functools.partial(schedule_func, schedule)
        cron_scheduled_task = aiocron.crontab(schedule.cron, func=cron_schedule_func)
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