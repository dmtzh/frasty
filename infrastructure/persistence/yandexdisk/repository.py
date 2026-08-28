import aiohttp

from shared.infrastructure.serialization.serializer import Serializer
from shared.infrastructure.storage.repository import (
    AsyncRepository,
    NotFoundException,
    AlreadyExistsException
)

from .apiclient import (
    YandexDiskApiClient,
    YandexDiskNotFoundException,
    YandexDiskConflictException
)
from .config import YandexDiskRepositoryConfig
from .pathresolver import YandexDiskPathResolver

class YandexDiskRepository[TId, TItem](AsyncRepository[TId, TItem]):
    """
    AsyncRepository implementation backed by Yandex Disk.
    
    Key behaviors:
       - add: Strictly fails with AlreadyExistsException if the file exists.
       - update: Strictly fails with NotFoundException if the file is missing 
                 (includes double-check to prevent race conditions).
       - get: Returns None if the file is missing.
       - delete: Strictly idempotent (returns None even if the file is missing).
       - No internal retries or timeouts (SRP).
    """
    def __init__(
        self,
        config: YandexDiskRepositoryConfig,
        serializer: Serializer[TItem],
    ) -> None:
        self._oauth_token = config.oauth_token
        self._resolver = YandexDiskPathResolver(config.base_folder)
        self._serializer = serializer
        self._timeout = aiohttp.ClientTimeout(total=config.timeout_seconds)

    async def get(self, id: TId) -> TItem | None:
        path = self._resolver.resolve(id)

        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            api_client = YandexDiskApiClient(session, self._oauth_token)
            try:
                href = await api_client.get_download_link(path)
            except YandexDiskNotFoundException:
                return None

            raw_data = await api_client.download_by_link(href)

            return self._serializer.deserialize(raw_data)

    async def add(self, id: TId, item: TItem) -> None:
        path = self._resolver.resolve(id)

        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            api_client = YandexDiskApiClient(session, self._oauth_token)
            try:
                # overwrite=False ensures atomic check-and-create via API
                href = await api_client.get_upload_link(path, overwrite=False)
            except YandexDiskConflictException as cex:
                match cex.error.error:
                    case "DiskResourceAlreadyExistsError":
                        raise AlreadyExistsException(f"Item with id '{id}' already exists")
                    case _:
                        raise

            data_str = self._serializer.serialize(item)
            await api_client.upload_by_link(href, data_str)

    async def update(self, id: TId, item: TItem) -> None:
        path = self._resolver.resolve(id)

        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            api_client = YandexDiskApiClient(session, self._oauth_token)
            # 1. Pre-check existence to enforce strict NotFoundException semantics
            try:
                await api_client.get_resource_info(path)
            except YandexDiskNotFoundException:
                raise NotFoundException(f"Item with id '{id}' not found")
            
            # 2. Get upload link with overwrite=True
            href = await api_client.get_upload_link(path, overwrite=True)

            # 3. Upload data with strict race-condition protection
            data_str = self._serializer.serialize(item)
            try:
                await api_client.upload_by_link(href, data_str)
            except YandexDiskNotFoundException:
                # Race condition: file was deleted between pre-check and PUT
                raise NotFoundException(f"Item with id '{id}' vanished during update")

    async def delete(self, id: TId) -> None:
        path = self._resolver.resolve(id)

        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            api_client = YandexDiskApiClient(session, self._oauth_token)
            try:
                await api_client.delete_resource(path)
            except YandexDiskNotFoundException:
                # Strict idempotency: if it's already gone, we consider it a success
                pass
