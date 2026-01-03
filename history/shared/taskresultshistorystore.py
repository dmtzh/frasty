from collections.abc import Callable, Coroutine
from functools import wraps
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from expression import Result

from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.taskresulthistory import LegacyTaskResultHistoryItemAdapter

import config

P = ParamSpec("P")
R = TypeVar("R")

class TaskResultsHistoryStore[T]:
    def __init__(self, items_sub_folder_name: str, to_dict: Callable[[T], dict[str, Any]], from_dict: Callable[[dict[str, Any]], Result[T, Any]]):
        self._folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "HistoryStorage", items_sub_folder_name)
        self._to_dict = to_dict
        self._from_dict = from_dict
    
    def _get_task_id_file_repo_with_ver(self, task_id: TaskIdValue):
        return FileWithVersion[RunIdValue, T, dict[str, Any]](
            task_id,
            self._to_dict,
            self._from_dict,
            JsonSerializer[dict[str, Any]](),
            "json",
            self._folder_path
        )
    
    def with_storage(self, func: Callable[Concatenate[T | None, P], tuple[R, T]]):
        @wraps(func)
        def wrapper(task_id: TaskIdValue, run_id: RunIdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            file_repo_with_ver = self._get_task_id_file_repo_with_ver(task_id)
            item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
            return item_action(func)(run_id, *args, **kwargs)
        return wrapper
    
    async def get(self, task_id: TaskIdValue, run_id: RunIdValue):
        file_repo_with_ver = self._get_task_id_file_repo_with_ver(task_id)
        opt_ver_with_data = await file_repo_with_ver.get(run_id)
        match opt_ver_with_data:
            case (_, data):
                return data
            case None:
                return None

legacy_taskresultshistory_storage = TaskResultsHistoryStore(
    "LegacyTaskResults",
    LegacyTaskResultHistoryItemAdapter.to_dict,
    LegacyTaskResultHistoryItemAdapter.from_dict
)