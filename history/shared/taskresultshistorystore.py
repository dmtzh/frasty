from collections.abc import Callable, Coroutine
from functools import wraps
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.taskresulthistory import TaskResultHistoryItem, TaskResultHistoryItemAdapter

import config

P = ParamSpec("P")
R = TypeVar("R")

class TaskResultsHistoryStore:
    def __init__(self):
        self._folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "HistoryStorage", "TaskResults")
    
    def _get_task_id_file_repo_with_ver(self, task_id: TaskIdValue):
        return FileWithVersion[RunIdValue, TaskResultHistoryItem, dict[str, Any]](
            task_id,
            TaskResultHistoryItemAdapter.to_dict,
            TaskResultHistoryItemAdapter.from_dict,
            JsonSerializer[dict[str, Any]](),
            "json",
            self._folder_path
        )
    
    def with_storage(self, func: Callable[Concatenate[TaskResultHistoryItem | None, P], tuple[R, TaskResultHistoryItem]]):
        @wraps(func)
        def wrapper(task_id: TaskIdValue, run_id: RunIdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            file_repo_with_ver = self._get_task_id_file_repo_with_ver(task_id)
            item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
            return item_action(func)(run_id, *args, **kwargs)
        return wrapper

taskresultshistory_storage = TaskResultsHistoryStore()