from collections.abc import Callable, Coroutine
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

import config
from .domainrunning import RunningDefinitionState
from .dtorunning import RunningDefinitionStateAdapter
from shared.customtypes import IdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

P = ParamSpec("P")
R = TypeVar("R")

class RunningDefinitionsStore:
    def __init__(self):
        # This is a singleton
        self.__class__.__new__ = lambda cls: self
        self.__class__.__init__ = lambda self: None

        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "DefinitionsStorage")
        file_repo_with_ver = FileWithVersion[str, RunningDefinitionState, list[dict[str, Any]]](
            RunningDefinitionState.__name__,
            RunningDefinitionStateAdapter.to_list,
            RunningDefinitionStateAdapter.from_list,
            JsonSerializer[list[dict[str, Any]]](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def with_storage(self, func: Callable[Concatenate[RunningDefinitionState | None, P], tuple[R, RunningDefinitionState]]):
        def wrapper(run_id: IdValue, definition_id: IdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            id_str = f"{run_id}_{definition_id}"
            return self._item_action(func)(id_str, *args, **kwargs)
        return wrapper
    
    def delete(self, run_id: IdValue, definition_id: IdValue):
        id_str = f"{run_id}_{definition_id}"
        return self._file_repo_with_ver.delete(id_str)

running_definitions_storage = RunningDefinitionsStore()