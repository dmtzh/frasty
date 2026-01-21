from collections.abc import Callable
import os
from typing import Any

from expression import Result

import config
from shared.customtypes import DefinitionIdValue
from shared.definition import Definition, DefinitionAdapter
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repository import AlreadyExistsException, NotFoundError, NotFoundException, StorageError
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.utils.asyncresult import async_ex_to_error_result

class DefinitionsStore[T]:
    def __init__(self, items_sub_folder_name: str, to_list: Callable[[T], list[dict[str, Any]]], from_list: Callable[[list[dict[str, Any]]], Result[T, Any]]):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "DefinitionsStorage")
        file_repo_with_ver = FileWithVersion[DefinitionIdValue, T, list[dict[str, Any]]](
            items_sub_folder_name,
            to_list,
            from_list,
            JsonSerializer[list[dict[str, Any]]](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def add(self, id: DefinitionIdValue, definition: T):
        def add_func(opt_def: T | None):
            if opt_def is not None:
                raise AlreadyExistsException(f"Definition {id} already exists")
            return None, definition
        return self._item_action(add_func)(id)
    
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    def update(self, id: DefinitionIdValue, definition: T):
        def update_func(opt_def: T | None):
            if opt_def is None:
                raise NotFoundException(f"Definition {id} not found")
            return None, definition
        return self._item_action(update_func)(id)
    
    async def get_with_ver(self, id: DefinitionIdValue):
        opt_ver_with_definition = await self._file_repo_with_ver.get(id)
        if opt_ver_with_definition is None:
            return None
        ver, definition = opt_ver_with_definition
        return (definition, ver)

definitions_storage = DefinitionsStore(
    Definition.__name__,
    DefinitionAdapter.to_list,
    DefinitionAdapter.from_list
)