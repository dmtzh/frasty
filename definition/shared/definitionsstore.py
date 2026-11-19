from collections.abc import Callable
import os
from typing import Any

from expression import Result

from infrastructure.persistence.filesystem.filewithversion import FileWithVersion
from shared.customtypes import DefinitionIdValue
from shared.definition import Definition, DefinitionAdapter
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.repository import AlreadyExistsException, NotFoundError, NotFoundException, StorageError
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion
from shared.utils.exceptiondecorators import async_ex_to_error_result

class DefinitionsStore:
    def __init__(self, root_folder: str):
        folder_path = os.path.join(root_folder, "DefinitionsStorage")
        file_repo_with_ver = FileWithVersion[DefinitionIdValue, Definition, list[dict[str, Any]]](
            Definition.__name__,
            DefinitionAdapter.to_list,
            DefinitionAdapter.from_list,
            JsonSerializer[list[dict[str, Any]]](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def add(self, id: DefinitionIdValue, definition: Definition):
        def add_func(opt_def: Definition | None):
            if opt_def is not None:
                raise AlreadyExistsException(f"Definition {id} already exists")
            return None, definition
        return self._item_action(add_func)(id)
    
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    def update(self, id: DefinitionIdValue, definition: Definition):
        def update_func(opt_def: Definition | None):
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
