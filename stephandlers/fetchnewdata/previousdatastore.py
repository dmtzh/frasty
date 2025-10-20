import os
from typing import Any

from shared.completedresult import CompletedResultAdapter, CompletedWith
from shared.customtypes import TaskIdValue
from shared.infrastructure.serialization.json import JsonSerializer
from shared.infrastructure.storage.filewithversionlimited import FileWithVersionLimited
from shared.infrastructure.storage.repositoryitemaction import ItemActionInAsyncRepositoryWithVersion

import config

class PreviousDataStore:
    def __init__(self):
        folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "FetchNewDataStorage")
        file_repo_with_ver = FileWithVersionLimited[TaskIdValue, CompletedWith.Data | CompletedWith.NoData, dict[str, Any]](
            "PreviousData",
            CompletedResultAdapter.to_dict,
            CompletedResultAdapter.from_dict,
            JsonSerializer[dict[str, str]](),
            "json",
            folder_path,
            5
        )
        self._file_repo_with_ver = file_repo_with_ver
        self._item_action = ItemActionInAsyncRepositoryWithVersion(file_repo_with_ver)
    
    async def get(self, id: TaskIdValue):
        opt_ver_with_value = await self._file_repo_with_ver.get(id)
        match opt_ver_with_value:
            case (_, value):
                match value:
                    case CompletedWith.Data() | CompletedWith.NoData():
                        return value
                    case _:
                        return None
            case None:
                return None
    
    def set(self, id: TaskIdValue, value: CompletedWith.Data | CompletedWith.NoData):
        match value:
            case CompletedWith.Data() | CompletedWith.NoData():
                def set_data(data: CompletedWith.Data | CompletedWith.NoData | None):
                    return None, value
                return self._item_action(set_data)(id)
            case _:
                raise ValueError(f"Invalid value {value}")

previous_data_storage = PreviousDataStore()