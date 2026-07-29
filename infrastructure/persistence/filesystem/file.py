import asyncio
from collections.abc import Callable
import os

import aiofiles
import aiofiles.os as aos
from expression import Result

from shared.infrastructure.serialization.serializer import Serializer
from shared.infrastructure.storage.repository import AlreadyExistsException, AsyncRepository, NotFoundException
from shared.utils.result import ResultTag

class File[TId, TItem, TItemDto](AsyncRepository[TId, TItem]):
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
        self._lock = asyncio.Lock()
        os.makedirs(self._folder_path, exist_ok=True)
    
    async def get(self, id: TId) -> TItem | None:
        file_name = f"{id}.{self._extension}"
        file_path = os.path.join(self._folder_path, file_name)
        try:
            async with self._lock:
                async with aiofiles.open(file_path, mode='r') as f:
                    dto_item = self._serializer.deserialize(await f.read())
                    item_or_res = self._dto_to_item(dto_item)
                    match item_or_res:
                        case Result():
                            match item_or_res:
                                case Result(tag=ResultTag.OK, ok=item):
                                    return item
                                case Result(tag=ResultTag.ERROR, error=err):
                                    raise ValueError(str(err))
                                case _:
                                    raise ValueError("Item is invalid")
                        case item:
                            return item
        except FileNotFoundError:
            return None
    
    async def add(self, id: TId, item: TItem) -> None:
        file_name = f"{id}.{self._extension}"
        file_path = os.path.join(self._folder_path, file_name)
        dto_item = self._item_to_dto(item)
        async with self._lock:
            try:
                async with aiofiles.open(file_path, mode='x') as f:
                    await f.write(self._serializer.serialize(dto_item))
            except FileExistsError:
                raise AlreadyExistsException(id)
    
    async def update(self, id: TId, item: TItem) -> None:
        file_name = f"{id}.{self._extension}"
        file_path = os.path.join(self._folder_path, file_name)
        dto_item = self._item_to_dto(item)
        async with self._lock:
            try:
                async with aiofiles.open(file_path, mode='r+') as f:
                    await f.write(self._serializer.serialize(dto_item))
                    await f.truncate()
            except FileNotFoundError:
                raise NotFoundException(id)
    
    async def delete(self, id: TId) -> None:
        file_name = f"{id}.{self._extension}"
        file_path = os.path.join(self._folder_path, file_name)
        async with self._lock:
            try:
                await aos.remove(file_path)
            except FileNotFoundError:
                return None