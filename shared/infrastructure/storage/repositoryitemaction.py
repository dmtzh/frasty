from collections.abc import Callable, Coroutine
from functools import wraps
import threading
from typing import Any, Concatenate, Generic, ParamSpec, TypeVar

from shared.infrastructure.storage.repository import AsyncRepositoryWithVersion, Repository

T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")
TId = TypeVar("TId")

class ItemActionInAsyncRepositoryWithVersion(Generic[TId, T]):
    def __init__(self, repository: AsyncRepositoryWithVersion[TId, T]):
        self._repository = repository
    
    def __call__(self, func: Callable[Concatenate[T | None, P], tuple[R, T]]) -> Callable[Concatenate[TId, P], Coroutine[Any, Any, R]]:
        @wraps(func)
        async def wrapper(id: TId, *args: P.args, **kwargs: P.kwargs) -> R:
            item_with_ver = await self._repository.get(id)
            exists = item_with_ver is not None
            if exists:
                ver, item = item_with_ver
                res, updated_item = func(item, *args, **kwargs)
                updated = await self._repository.update(id, ver, updated_item)
                if updated:
                    return res
                else:
                    return await wrapper(id, *args, **kwargs)
            else:
                res, new_item = func(None, *args, **kwargs)
                await self._repository.add(id, new_item)
                return res
        return wrapper

class ItemActionInRepository(Generic[TId, T]):
    def __init__(self, repository: Repository[TId, T]):
        self._repository = repository
        self._lock = threading.Lock()
    
    def __call__(self, func: Callable[Concatenate[T | None, P], tuple[R, T]]) -> Callable[Concatenate[TId, P], R]:
        @wraps(func)
        def wrapper(id: TId, *args: P.args, **kwargs: P.kwargs) -> R:
            with self._lock:
                item = self._repository.get(id)
                exists = item is not None
                if exists:
                    res, updated_item = func(item, *args, **kwargs)
                    self._repository.update(id, updated_item)
                    return res
                else:
                    res, new_item = func(None, *args, **kwargs)
                    self._repository.add(id, new_item)
                    return res
        return wrapper