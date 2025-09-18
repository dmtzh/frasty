import os

from shared.customtypes import TaskIdValue
from shared.domainschedule import TaskSchedule, TaskScheduleAdapter
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

import config

type ItemType = dict[TaskIdValue, TaskSchedule]
type DtoItemType = dict[TaskIdValue, dict[str, str]]

def _item_to_dto(item: ItemType) -> DtoItemType:
    schedules_dto = {task_id: TaskScheduleAdapter.to_dict(schedule) for task_id, schedule in item.items()}
    return schedules_dto

def _dto_to_item(dto: DtoItemType) -> ItemType:
    all_schedules = {TaskIdValue.from_value(raw_task_id): TaskScheduleAdapter.from_dict(raw_schedule) for raw_task_id, raw_schedule in dto.items()}
    valid_schedules = {opt_task_id: schedule_res.ok for opt_task_id, schedule_res in all_schedules.items() if opt_task_id is not None and schedule_res.is_ok()}
    return valid_schedules

class TasksSchedulesStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "SchedulesStorage")
        file_repo_with_ver = FileWithVersion[str, ItemType, DtoItemType](
            "",
            _item_to_dto,
            _dto_to_item,
            JsonSerializer[DtoItemType](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)

    def set_task_schedule(self, task_id: TaskIdValue, schedule: TaskSchedule):
        def add_or_update_schedule(tasks_schedules: ItemType | None):
            tasks_schedules = tasks_schedules or {}
            tasks_schedules[task_id] = schedule
            return None, tasks_schedules
        return self._item_action(add_or_update_schedule)("TASKS_SCHEDULES")
    
    async def get_schedules(self) -> ItemType:
        opt_item_with_ver = await self._file_repo_with_ver.get("TASKS_SCHEDULES")
        match opt_item_with_ver:
            case None:
                return {}
            case (_, schedules):
                return schedules

tasks_schedules_storage = TasksSchedulesStore()