from dataclasses import dataclass
from typing import Any

import aiohttp

from shared.utils.parse import NonEmptyStr

@dataclass(frozen=True)
class YandexDiskApiError:
    message: str
    description: str
    error: str

class YandexDiskApiException(Exception):
    """Base exception for unexpected Yandex Disk API errors."""
    def __init__(self, status: int, error: YandexDiskApiError):
        self.status = status
        self.error = error
        super().__init__(f"Yandex Disk API error {status}: {error.message}")

class YandexDiskNotFoundException(YandexDiskApiException):
    """Raised when the API returns 404 Not Found."""
    pass

class YandexDiskConflictException(YandexDiskApiException):
    """Raised when the API returns 409 Conflict or 412 Precondition Failed."""
    pass

class YandexDiskApiClient:
    """
    Low-level async HTTP client for Yandex Disk API.
    
    Strictly follows SRP: handles authentication, HTTP requests, and basic 
    HTTP-status mapping. Does NOT contain retry, timeout, or business-logic 
    error handling.
    
    Architecture Invariant:
       Temporary `href` links returned by Yandex Disk API have a short TTL.
       This client NEVER caches these links. Every operation triggers a fresh 
       request to obtain an actual `href` immediately before data transfer.
    """
    def __init__(self, session: aiohttp.ClientSession, oauth_token: NonEmptyStr) -> None:
        self._session = session
        self._headers = {
            "Authorization": f"OAuth {oauth_token}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
        }

    async def _validate_response(self, response: aiohttp.ClientResponse) -> aiohttp.ClientResponse:
        """Map HTTP status codes to specific transport exceptions."""
        if response.status == 200 or response.status == 201 or response.status == 204:
            return response
        
        raw_err = await response.json()
        error = YandexDiskApiError(**raw_err)
        if response.status == 404:
            raise YandexDiskNotFoundException(response.status, error)
        if response.status in (409, 412):
            raise YandexDiskConflictException(response.status, error)
            
        raise YandexDiskApiException(response.status, error)

    async def get_resource_info(self, path: str) -> dict[str, Any]:
        """Calls GET /v1/disk/resources?path={path}. Raises YandexDiskNotFoundException if missing."""
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        params = {"path": path}
        async with self._session.get(url, headers=self._headers, params=params) as resp:
            await self._validate_response(resp)
            return await resp.json()

    async def get_download_link(self, path: str) -> str:
        """Calls GET /v1/disk/resources/download. Returns the temporary 'href'."""
        url = "https://cloud-api.yandex.net/v1/disk/resources/download"
        params = {"path": path}
        async with self._session.get(url, headers=self._headers, params=params) as resp:
            await self._validate_response(resp)
            data = await resp.json()
            return data["href"]

    async def get_upload_link(self, path: str, overwrite: bool) -> str:
        """
        Calls GET /v1/disk/resources/upload. 
        Raises YandexDiskConflictException if overwrite=False and file exists.
        """
        url = "https://cloud-api.yandex.net/v1/disk/resources/upload"
        params = {"path": path, "overwrite": str(overwrite).lower()}
        async with self._session.get(url, headers=self._headers, params=params) as resp:
            await self._validate_response(resp)
            data = await resp.json()
            return data["href"]

    async def upload_by_link(self, href: str, data: str) -> None:
        """
        Calls PUT {href} with the serialized string data.
        Note: The temporary 'href' does not require the Authorization header.
        aiohttp will automatically handle chunking for large strings.
        """
        async with self._session.put(href, data=data) as resp:
            # If the link expired or resource vanished, it might return 404
            await self._validate_response(resp)

    async def download_by_link(self, href: str) -> str:
        """Calls GET {href} and reads the response body as a string."""
        async with self._session.get(href) as resp:
            await self._validate_response(resp)
            return await resp.text()

    async def delete_resource(self, path: str) -> None:
        """Calls DELETE /v1/disk/resources?path={path}."""
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        params = {"path": path}
        async with self._session.delete(url, headers=self._headers, params=params) as resp:
            await self._validate_response(resp)
