from collections.abc import Callable, Coroutine
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from infrastructure.persistence.filesystem.filewithversion import FileWithVersion
from shared.customtypes import RunIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

from manualrunstate import ManualRunState, ManualRunStateAdapter

P = ParamSpec("P")
R = TypeVar("R")

class ManualRunStore:
    def __init__(self, root_folder: str):
        folder_path = os.path.join(root_folder, "DefinitionsStorage")
        file_repo_with_ver = FileWithVersion[RunIdValue, ManualRunState, dict[str, Any]](
            ManualRunState.__name__,
            ManualRunStateAdapter.to_dict,
            ManualRunStateAdapter.from_dict,
            JsonSerializer[dict[str, Any]](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def with_storage(self, func: Callable[Concatenate[ManualRunState | None, P], tuple[R, ManualRunState]]):
        def wrapper(manual_run_id: RunIdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            return self._item_action(func)(manual_run_id, *args, **kwargs)
        return wrapper
    
    def delete(self, manual_run_id: RunIdValue):
        return self._file_repo_with_ver.delete(manual_run_id)
    
    async def get(self, manual_run_id: RunIdValue):
        opt_ver_with_state = await self._file_repo_with_ver.get(manual_run_id)
        match opt_ver_with_state:
            case (_, state):
                return state
            case None:
                return None
