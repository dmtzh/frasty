from collections.abc import Callable, Coroutine
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from expression import Result

import config
from .domainrunning import RunningDefinitionState
from .dtorunning import RunningDefinitionStateAdapter
from . import runningdefinition
from shared.customtypes import IdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

P = ParamSpec("P")
R = TypeVar("R")

class RunningDefinitionsStore[T]:
    def __init__(self, items_sub_folder_name: str, to_list: Callable[[T], list[dict[str, Any]]], from_list: Callable[[list[dict[str, Any]]], Result[T, Any]]):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "DefinitionsStorage")
        file_repo_with_ver = FileWithVersion[str, T, list[dict[str, Any]]](
            items_sub_folder_name,
            to_list,
            from_list,
            JsonSerializer[list[dict[str, Any]]](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def with_storage(self, func: Callable[Concatenate[T | None, P], tuple[R, T]]):
        def wrapper(run_id: IdValue, definition_id: IdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            id_str = f"{run_id}_{definition_id}"
            return self._item_action(func)(id_str, *args, **kwargs)
        return wrapper
    
    def delete(self, run_id: IdValue, definition_id: IdValue):
        id_str = f"{run_id}_{definition_id}"
        return self._file_repo_with_ver.delete(id_str)

running_definitions_storage = RunningDefinitionsStore(
    RunningDefinitionState.__name__,
    RunningDefinitionStateAdapter.to_list,
    RunningDefinitionStateAdapter.from_list
)

running_action_definitions_storage = RunningDefinitionsStore(
    f"{runningdefinition.RunningDefinitionState.__name__}Action",
    runningdefinition.RunningDefinitionStateAdapter.to_list,
    runningdefinition.RunningDefinitionStateAdapter.from_list
)