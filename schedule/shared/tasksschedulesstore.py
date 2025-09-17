import os

from shared.customtypes import ScheduleIdValue, TaskIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

import config

type ItemType = dict[TaskIdValue, ScheduleIdValue]

class TasksSchedulesStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "SchedulesStorage")
        file_repo_with_ver = FileWithVersion[str, ItemType, ItemType](
            "",
            lambda x: x,
            lambda x: x,
            JsonSerializer[ItemType](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)

    def set_task_schedule(self, task_id: TaskIdValue, schedule_id: ScheduleIdValue):
        def add_or_update_schedule(tasks_schedules: ItemType | None):
            tasks_schedules = tasks_schedules or {}
            tasks_schedules[task_id] = schedule_id
            return None, tasks_schedules
        return self._item_action(add_or_update_schedule)("TASKS_SCHEDULES")

tasks_schedules_storage = TasksSchedulesStore()