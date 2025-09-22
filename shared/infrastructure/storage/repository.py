from abc import ABC, abstractmethod

from ...customtypes import Error

class Repository[TId, TItem](ABC):
    @abstractmethod
    def get(self, id: TId) -> TItem | None:
        pass

    @abstractmethod
    def add(self, id: TId, item: TItem) -> None:
        pass

    @abstractmethod
    def update(self, id: TId, item: TItem) -> None:
        pass

    @abstractmethod
    def delete(self, id: TId) -> None:
        pass

class AsyncRepository[TId, TItem](ABC):
    @abstractmethod
    async def get(self, id: TId) -> TItem | None:
        pass

    @abstractmethod
    async def add(self, id: TId, item: TItem) -> None:
        pass

    @abstractmethod
    async def update(self, id: TId, item: TItem) -> None:
        pass

    @abstractmethod
    async def delete(self, id: TId) -> None:
        pass

class RepositoryWithVersion[TId, TItem](ABC):
    @abstractmethod
    def get(self, id: TId) -> tuple[int, TItem] | None:
        pass

    @abstractmethod
    def add(self, id: TId, item: TItem) -> None:
        pass

    @abstractmethod
    def update(self, id: TId, ver: int, item: TItem) -> bool:
        pass

    @abstractmethod
    def delete(self, id: TId) -> None:
        pass

class AsyncRepositoryWithVersion[TId, TItem](ABC):
    @abstractmethod
    async def get(self, id: TId) -> tuple[int, TItem] | None:
        pass

    @abstractmethod
    async def add(self, id: TId, item: TItem) -> None:
        pass

    @abstractmethod
    async def update(self, id: TId, ver: int, item: TItem) -> bool:
        pass

    @abstractmethod
    async def delete(self, id: TId) -> None:
        pass

class StorageError(Error):
    '''Unexpected storage error'''

class NotFoundError(Error):
    '''Item not found error'''

class NotFoundException(ValueError):
    '''Item not found exception'''

class AlreadyExistsError(Error):
    '''Item already exists error'''

class AlreadyExistsException(ValueError):
    '''Item already exists exception'''