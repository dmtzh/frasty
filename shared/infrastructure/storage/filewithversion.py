from collections.abc import Callable
from dataclasses import dataclass
import os
import shutil

import aiofiles
import aiofiles.os as aos
from expression import Result

from shared.infrastructure.serialization.serializer import Serializer
from shared.infrastructure.storage.repository import AsyncRepositoryWithVersion
from shared.utils.result import ResultTag

@dataclass(frozen=True)
class ItemAlreadyExistsError[TId](Exception):
    """Raised when an item with the same ID already exists."""
    id: TId

class FileWithVersion[TId, TItem, TItemDto](
    AsyncRepositoryWithVersion[TId, TItem]
):
    def __init__(
        self,
        items_sub_folder_name: str,
        item_to_dto: Callable[[TItem], TItemDto],
        dto_to_item: Callable[[TItemDto], TItem | Result],
        serializer: Serializer[TItemDto],
        extension: str,
        folder_path: str
    ):
        self._item_to_dto = item_to_dto
        self._dto_to_item = dto_to_item
        self._serializer = serializer
        self._extension = extension
        self._folder_path = os.path.join(folder_path, items_sub_folder_name)
    
    async def get_all_ids(self) -> list[str]:
        if not await aos.path.isdir(self._folder_path):
            return []
        all_items = await aos.listdir(self._folder_path)
        return all_items
    
    def _get_max_version(self, all_files: list[str]) -> int:
        all_versions = [int(item.split(".")[0]) for item in all_files]
        max_ver = max(all_versions)
        return max_ver

    async def get(self, id: TId) -> tuple[int, TItem] | None:
        async def get_existing_item(ver: int) -> tuple[int, TItem] | None:
            file_name = f"{ver}.{self._extension}"
            file_path = os.path.join(self._folder_path, str(id), file_name)
            async with aiofiles.open(file_path, mode='r') as f:
                file_content = await f.read()
            if file_content == "" and ver > 1:
                return await get_existing_item(ver - 1)
            dto_item = self._serializer.deserialize(file_content)
            item_or_res = self._dto_to_item(dto_item)
            match item_or_res:
                case Result():
                    match item_or_res:
                        case Result(tag=ResultTag.OK, ok=item):
                            return ver, item
                        case Result(tag=ResultTag.ERROR, error=err):
                            raise ValueError(str(err))
                        case _:
                            raise ValueError("Item is invalid")
                case None:
                    raise ValueError("Item is None")
                case item:
                    return ver, item
        
        try:
            id_folder_path = os.path.join(self._folder_path, str(id))
            all_files = await aos.listdir(id_folder_path)
            match all_files:
                case []:
                    return None
                case _:
                    max_ver = self._get_max_version(all_files)
                    return await get_existing_item(max_ver)
        except FileNotFoundError:
            return None

    async def add(self, id: TId, item: TItem) -> None:
        id_folder_path = os.path.join(self._folder_path, str(id))
        await aos.makedirs(id_folder_path, exist_ok=True)
        ver = 1
        file_name = f"{ver}.{self._extension}"
        file_path = os.path.join(id_folder_path, file_name)
        dto_item = self._item_to_dto(item)
        try:
            async with aiofiles.open(file_path, mode='x') as f:
                await f.write(self._serializer.serialize(dto_item))
        except FileExistsError:
            raise ItemAlreadyExistsError(id)

    async def update(self, id: TId, ver: int, item: TItem) -> bool:
        ver_file_name = f"{ver}.{self._extension}"
        ver_file_path = os.path.join(self._folder_path, str(id), ver_file_name)
        if not await aos.path.isfile(ver_file_path):
            return False
        file_name = f"{ver + 1}.{self._extension}"
        file_path = os.path.join(self._folder_path, str(id), file_name)
        dto_item = self._item_to_dto(item)
        try:
            async with aiofiles.open(file_path, mode='x') as f:
                await f.write(self._serializer.serialize(dto_item))
            return True
        except FileExistsError:
            return False

    async def delete(self, id: TId) -> None:
        id_folder_path = os.path.join(self._folder_path, str(id))
        shutil.rmtree(id_folder_path, ignore_errors=True)