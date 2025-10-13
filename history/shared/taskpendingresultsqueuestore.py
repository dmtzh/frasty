from collections.abc import Callable, Coroutine
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from shared.customtypes import TaskIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversionlimited import FileWithVersionLimited
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.taskpendingresultsqueue import TaskPendingResultsQueue, TaskPendingResultsQueueAdapter

import config

P = ParamSpec("P")
R = TypeVar("R")

class TaskPendingResultsQueueStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "HistoryStorage")
        file_repo_with_ver = FileWithVersionLimited[TaskIdValue, TaskPendingResultsQueue, dict[str, Any]](
            TaskPendingResultsQueue.__name__,
            TaskPendingResultsQueueAdapter.to_dict,
            TaskPendingResultsQueueAdapter.from_dict,
            JsonSerializer[dict[str, Any]](),
            "json",
            folder_path,
            5
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def with_storage(self, func: Callable[Concatenate[TaskPendingResultsQueue | None, P], tuple[R, TaskPendingResultsQueue]]):
        def wrapper(task_id: TaskIdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            return self._item_action(func)(task_id, *args, **kwargs)
        return wrapper
    
taskpendingresultsqueue_storage = TaskPendingResultsQueueStore()