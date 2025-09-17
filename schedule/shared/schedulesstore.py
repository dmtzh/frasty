import os

from shared.customtypes import ScheduleIdValue
from shared.domainschedule import CronSchedule, CronScheduleAdapter
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

import config

class SchedulesStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "SchedulesStorage")
        file_repo_with_ver = FileWithVersion[ScheduleIdValue, CronSchedule, dict[str, str]](
            CronSchedule.__name__,
            CronScheduleAdapter.to_dict,
            CronScheduleAdapter.from_dict,
            JsonSerializer[dict[str, str]](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)

    def add(self, id: ScheduleIdValue, schedule: CronSchedule):
        if not isinstance(schedule, CronSchedule):
            raise ValueError(f"schedule must be of type {CronSchedule.__name__}")
        def add_func(s: CronSchedule | None):
            if s is not None:
                raise ValueError("Schedule already exists")
            return None, schedule
        return self._item_action(add_func)(id)

schedules_storage = SchedulesStore()