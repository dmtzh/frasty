import os
from typing import Any, ParamSpec, TypeVar

import config
from shared.customtypes import IdValue
import shared.domaindefinition as shdomaindef
import shared.dtodefinition as shdtodef
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

P = ParamSpec("P")
R = TypeVar("R")

class DefinitionsStore:
    def __init__(self):
        # This is a singleton
        self.__class__.__new__ = lambda cls: self
        self.__class__.__init__ = lambda self: None

        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "DefinitionsStorage")
        file_repo_with_ver = FileWithVersion[IdValue, shdomaindef.Definition, list[dict[str, Any]]](
            shdomaindef.Definition.__name__,
            shdtodef.DefinitionAdapter.to_list,
            shdtodef.DefinitionAdapter.from_list,
            JsonSerializer[list[dict[str, Any]]](),
            "json",
            folder_path
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    def add(self, id: IdValue, definition: shdomaindef.Definition):
        if not isinstance(definition, shdomaindef.Definition):
            raise ValueError(f"definition must be of type {shdomaindef.Definition.__name__}")
        def add_func(t: shdomaindef.Definition | None):
            if t is not None:
                raise ValueError("Definition already exists")
            return None, definition
        return self._item_action(add_func)(id)
    
    async def get(self, id: IdValue):
        opt_ver_with_definition = await self._file_repo_with_ver.get(id)
        if opt_ver_with_definition is None:
            return None
        _, definition = opt_ver_with_definition
        return definition

definitions_storage = DefinitionsStore()