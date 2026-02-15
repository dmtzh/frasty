from collections.abc import Generator
from dataclasses import dataclass
import os
from typing import Any

from expression import Result, effect

from shared.customtypes import TaskIdValue, RunIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversionlimited import FileWithVersionLimited
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.utils.parse import parse_from_dict, parse_positive_int

import config
from .fetchidvalue import FetchIdValue

type ItemType = dict[FetchIdValue, ExecutingTaskData]
type DtoItemType = dict[str, dict[str, str]]

@dataclass(frozen=True)
class ExecutingTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    timestamp: int

    @staticmethod
    def to_dict(data: "ExecutingTaskData") -> dict[str, str]:
        return {
            "task_id": data.task_id.to_value_with_checksum(),
            "run_id": data.run_id.to_value_with_checksum(),
            "timestamp": str(data.timestamp)
        }
    
    @staticmethod
    @effect.result["ExecutingTaskData", str]()
    def from_dict(raw_data: dict[str, str]) -> Generator[Any, Any, "ExecutingTaskData"]:
        dict_data = yield from Result.Ok(raw_data) if isinstance(raw_data, dict) else Result.Error(f"Invalid data type {type(raw_data)}")
        task_id = yield from parse_from_dict(dict_data, "task_id", TaskIdValue.from_value_with_checksum)
        run_id = yield from parse_from_dict(dict_data, "run_id", RunIdValue.from_value_with_checksum)
        timestamp = yield from parse_from_dict(dict_data, "timestamp", parse_positive_int)
        return ExecutingTaskData(task_id, run_id, timestamp)

class ExecutingTasksDataAdapter:
    @staticmethod
    def to_dict(data: ItemType) -> DtoItemType:
        tasks_dict = {str(fetch_id): ExecutingTaskData.to_dict(task_data) for fetch_id, task_data in data.items()}
        return tasks_dict
    
    @staticmethod
    def from_dict(raw_data: DtoItemType):
        all_tasks = {FetchIdValue.from_value(raw_fetch_id): ExecutingTaskData.from_dict(raw_task).default_value(None) for raw_fetch_id, raw_task in raw_data.items()}
        valid_tasks = {opt_fetch_id: opt_task for opt_fetch_id, opt_task in all_tasks.items() if opt_fetch_id is not None and opt_task is not None}
        return valid_tasks

class ExecutingTasksStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "FetchNewDataStorage")
        file_repo_with_ver = FileWithVersionLimited[str, ItemType, DtoItemType](
            "",
            ExecutingTasksDataAdapter.to_dict,
            ExecutingTasksDataAdapter.from_dict,
            JsonSerializer[DtoItemType](),
            "json",
            folder_path,
            10
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def add(self, fetch_id: FetchIdValue, data: ExecutingTaskData):
        def add_task(executing_tasks: ItemType | None):
            executing_tasks = executing_tasks or {}
            executing_tasks[fetch_id] = data
            return None, executing_tasks
        return self._item_action(add_task)("EXECUTING_TASKS")
    
    def remove(self, fetch_id: FetchIdValue):
        def remove_task(executing_tasks: ItemType | None):
            executing_tasks = executing_tasks or {}
            opt_removed_task_data = executing_tasks.pop(fetch_id, None) 
            return opt_removed_task_data, executing_tasks
        return self._item_action(remove_task)("EXECUTING_TASKS")
    
    async def get(self, fetch_id: FetchIdValue):
        opt_items_with_ver = await self._file_repo_with_ver.get("EXECUTING_TASKS")
        match opt_items_with_ver:
            case (_, items):
                return items.get(fetch_id, None)
            case _:
                return None

executing_tasks_storage = ExecutingTasksStore()