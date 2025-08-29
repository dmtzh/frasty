from collections.abc import Callable, Coroutine
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

import config
from manualrunstate import ManualRunState, ManualRunStateAdapter
from shared.customtypes import IdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

P = ParamSpec("P")
R = TypeVar("R")

class ManualRunStore:
    def __init__(self):
        # This is a singleton
        self.__class__.__new__ = lambda cls: self
        self.__class__.__init__ = lambda self: None

        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "DefinitionsStorage")
        file_repo_with_ver = FileWithVersion[IdValue, ManualRunState, dict[str, Any]](
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
        def wrapper(manual_run_id: IdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            return self._item_action(func)(manual_run_id, *args, **kwargs)
        return wrapper
    
    def delete(self, manual_run_id: IdValue):
        return self._file_repo_with_ver.delete(manual_run_id)
    
    async def get(self, manual_run_id: IdValue):
        opt_ver_with_state = await self._file_repo_with_ver.get(manual_run_id)
        match opt_ver_with_state:
            case (_, state):
                return state
            case None:
                return None

manual_run_storage = ManualRunStore()