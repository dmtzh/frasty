from abc import ABC, abstractmethod
from dataclasses import dataclass

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

@dataclass(frozen=True)
class PublicLink:
    """
    Value object representing a public read-only link to a stored resource.
    
    Attributes:
        url: Direct URL for anonymous read-only access (e.g., "https://yadi.sk/i/...").
        public_key: Internal key used to revoke the publication via unpublish().
    """
    url: str
    public_key: str

class PublishableAsyncRepository[TId](ABC):
    """
    Extension contract for storage backends that support generating
    public read-only links to stored resources.
    
    Key invariants:
       - publish() is idempotent: calling it twice returns the same PublicLink.
       - unpublish() is idempotent: revoking an already-unpublished resource succeeds.
       - get_public_link() returns None if the resource exists but is not published.
       - All operations return None for non-existent resources (consistent with AsyncRepository.get).
    """
    
    @abstractmethod
    async def publish(self, id: TId) -> PublicLink | None:
        """
        Generate a public read-only link for the resource.
        
        Returns:
            PublicLink if resource exists (new or existing link).
            None if resource does not exist.
            
        Raises:
            StorageError on unexpected API/network failures.
        """
        pass

    @abstractmethod
    async def get_public_link(self, id: TId) -> PublicLink | None:
        """
        Retrieve the existing public link without modifying state.
        
        Returns:
            PublicLink if resource exists and is published.
            None if resource does not exist OR exists but is not published.
            
        Raises:
            StorageError on unexpected API/network failures.
        """
        pass

class YandexDiskRepository[TId, TItem](AsyncRepository[TId, TItem], PublishableAsyncRepository[TId]):
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

    async def publish(self, id: TId) -> PublicLink | None:
        """
        Generate a public read-only link. Idempotent.
        Returns None if the resource does not exist (consistent with get()).
        
        The /publish endpoint only toggles the visibility flag and returns 
        a metadata href. It does NOT return the public_url or public_key.
        Therefore, this method performs a two-step sequence:
        1. Call /publish to make the resource public.
        2. Call /resources (metadata) to fetch the actual public_url and public_key.
        """
        path = self._resolver.resolve(id)

        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            api_client = YandexDiskApiClient(session, self._oauth_token)
            try:
                # --- Step 1: Trigger publication (idempotent operation) ---
                await api_client.publish_resource(path)
                # --- Step 2: Fetch metadata to extract the actual public link ---
                metadata = await api_client.get_resource_metadata(path)
            except YandexDiskNotFoundException:
                return None

        # Check if resource is actually published
        public_url = metadata.get("public_url")
        public_key = metadata.get("public_key")
        if public_url is None or public_key is None:
            return None
        return PublicLink(url=public_url, public_key=public_key)

    async def get_public_link(self, id: TId) -> PublicLink | None:
        """
        Retrieve existing public link without state modification.
        Returns None if resource doesn't exist OR is not published.
        """
        path = self._resolver.resolve(id)

        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            api_client = YandexDiskApiClient(session, self._oauth_token)
            try:
                metadata = await api_client.get_resource_metadata(path)
            except YandexDiskNotFoundException:
                return None
            
        # Check if resource is actually published
        public_url = metadata.get("public_url")
        public_key = metadata.get("public_key")
        if public_url is None or public_key is None:
            return None
        return PublicLink(url=public_url, public_key=public_key)