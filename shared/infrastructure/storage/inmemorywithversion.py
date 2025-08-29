import threading

from shared.infrastructure.storage.repository import RepositoryWithVersion

class InMemoryWithVersion[TId, TItem](RepositoryWithVersion[TId, TItem]):
    def __init__(self):
        self._items: dict[TId, tuple[int,TItem]] = {}
        self._lock = threading.Lock()

    def get(self, id: TId):
        with self._lock:
            return self._items.get(id)
            
    def add(self, id: TId, item: TItem):
        with self._lock:
            self._items[id] = (1, item)
    
    def update(self, id: TId, ver: int, item: TItem):
        with self._lock:
            curr_ver, _ = self._items[id]
            if curr_ver == ver:
                self._items[id] = (curr_ver + 1, item)
        return curr_ver == ver
    
    def delete(self, id: TId):
        with self._lock:
            del self._items[id]