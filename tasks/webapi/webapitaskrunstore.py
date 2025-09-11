from collections.abc import Callable, Coroutine
from functools import wraps
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from shared.customtypes import IdValue, RunIdValue, TaskIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

import config
from webapitaskrunstate import WebApiTaskRunState, WebApiTaskRunStateAdapter

P = ParamSpec("P")
R = TypeVar("R")

class WebApiTaskRunStore:
    def __init__(self):
        self._folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "TasksStorage", WebApiTaskRunState.__name__)
    
    def _get_task_id_file_repo_with_ver(self, task_id: TaskIdValue):
        return FileWithVersion[IdValue, WebApiTaskRunState, dict[str, Any]](
            task_id,
            WebApiTaskRunStateAdapter.to_dict,
            WebApiTaskRunStateAdapter.from_dict,
            JsonSerializer[dict[str, Any]](),
            "json",
            self._folder_path
        )
    
    def with_storage(self, func: Callable[Concatenate[WebApiTaskRunState | None, P], tuple[R, WebApiTaskRunState]]):
        @wraps(func)
        def wrapper(task_id: TaskIdValue, run_id: RunIdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            file_repo_with_ver = self._get_task_id_file_repo_with_ver(task_id)
            item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
            return item_action(func)(run_id, *args, **kwargs)
        return wrapper
    
    def delete(self, task_id: TaskIdValue, run_id: RunIdValue):
        file_repo_with_ver = self._get_task_id_file_repo_with_ver(task_id)
        return file_repo_with_ver.delete(run_id)
    
    async def get(self, task_id: TaskIdValue, run_id: RunIdValue):
        file_repo_with_ver = self._get_task_id_file_repo_with_ver(task_id)
        opt_ver_with_state = await file_repo_with_ver.get(run_id)
        match opt_ver_with_state:
            case (_, state):
                return state
            case None:
                return None

web_api_task_run_storage = WebApiTaskRunStore()