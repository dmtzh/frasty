from collections.abc import Callable, Coroutine
import os
from typing import Any, Concatenate, ParamSpec, TypeVar

from expression import Result

from shared.customtypes import DefinitionIdValue, IdValue, RunIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

from .runningdefinition import RunningDefinitionState, RunningDefinitionStateAdapter

P = ParamSpec("P")
R = TypeVar("R")

class GenericFileStoreWithVersioning[T]:
    def __init__(self, folder_path: str, items_sub_folder_name: str, to_list: Callable[[T], list[dict[str, Any]]], from_list: Callable[[list[dict[str, Any]]], Result[T, Any]]):
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

class RunningDefinitionsStore(GenericFileStoreWithVersioning[RunningDefinitionState]):
    def __init__(self, root_folder: str):
        folder_path = os.path.join(root_folder, "DefinitionsStorage")
        super().__init__(folder_path, RunningDefinitionState.__name__, RunningDefinitionStateAdapter.to_list, RunningDefinitionStateAdapter.from_list)
    
    def with_storage(self, func: Callable[Concatenate[RunningDefinitionState | None, P], tuple[R, RunningDefinitionState]]):
        def wrapper(run_id: RunIdValue, definition_id: DefinitionIdValue, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
            id_str = f"{run_id}_{definition_id}"
            return self._item_action(func)(id_str, *args, **kwargs)
        return wrapper
    
    def delete(self, run_id: IdValue, definition_id: IdValue):
        id_str = f"{run_id}_{definition_id}"
        return self._file_repo_with_ver.delete(id_str)
