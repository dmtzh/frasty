from collections.abc import Callable
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from infrastructure.persistence.filesystem.filewithversionlimited import FileWithVersionLimited
from shared.customtypes import TaskIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.task import Task, TaskAdapter

P = ParamSpec("P")
R = TypeVar("R")

class TasksStore:
    def __init__(self, root_folder: str):
        folder_path = os.path.join(root_folder, "TasksStorage")
        file_repo_with_ver = FileWithVersionLimited[TaskIdValue, Task, dict[str, Any]](
            Task.__name__,
            TaskAdapter.to_dict,
            TaskAdapter.from_dict,
            JsonSerializer[dict[str, Any]](),
            "json",
            folder_path,
            1
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)

    def add(self, id: TaskIdValue, task: Task):
        if not isinstance(task, Task):
            raise ValueError(f"task must be of type {Task.__name__}")
        def add_func(t: Task | None):
            if t is not None:
                raise ValueError("Task already exists")
            return None, task
        return self._item_action(add_func)(id)
    
    async def get(self, id: TaskIdValue):
        opt_ver_with_value = await self._file_repo_with_ver.get(id)
        match opt_ver_with_value:
            case (_, value):
                return value
            case None:
                return None
    
    def with_storage(self, func: Callable[Concatenate[Task | None, P], tuple[R, Task]]):
        return self._item_action(func)
