from collections.abc import Callable
import os

import aiofiles.os as aos
from expression import Result

from shared.infrastructure.serialization.serializer import Serializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.utils.asyncresult import async_catch_ex

class FileWithVersionLimited[TId, TItem, TItemDto](FileWithVersion[TId, TItem, TItemDto]):
    def __init__(
        self,
        items_sub_folder_name: str,
        item_to_dto: Callable[[TItem], TItemDto],
        dto_to_item: Callable[[TItemDto], TItem | Result],
        serializer: Serializer[TItemDto],
        extension: str,
        folder_path: str,
        max_number_of_stored_recent_versions: int
    ):
        if max_number_of_stored_recent_versions < 1:
            raise ValueError("max_number_of_stored_recent_versions must be between 1 and 100")
        if max_number_of_stored_recent_versions > 100:
            raise ValueError("max_number_of_stored_recent_versions must be between 1 and 100")
        self._max_number_of_stored_recent_versions = max_number_of_stored_recent_versions
        super().__init__(items_sub_folder_name, item_to_dto, dto_to_item, serializer, extension, folder_path)

    @async_catch_ex
    def _remove_old_version(self, id: TId, version_to_remove: int):
        file_name = f"{version_to_remove}.{self._extension}"
        file_path = os.path.join(self._folder_path, str(id), file_name)
        return aos.remove(file_path)
    
    async def update(self, id: TId, ver: int, item: TItem) -> bool:
        updated = await super().update(id, ver, item)
        if updated:
            version_to_remove = ver + 1 - self._max_number_of_stored_recent_versions
            if version_to_remove >= 1:
                await self._remove_old_version(id, version_to_remove)
        return updated