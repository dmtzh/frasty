import threading

from .repository import Repository

class InMemory[TId, TItem](Repository[TId, TItem]):
    def __init__(self):
        self._items: dict[TId, TItem] = {}
        self._lock = threading.Lock()
    
    def get(self, id: TId):
        with self._lock:
            return self._items.get(id)

    def add(self, id: TId, item: TItem):
        with self._lock:
            self._items[id] = item

    def update(self, id: TId, item: TItem):
        with self._lock:
            self._items[id] = item
    
    def delete(self, id: TId):
        with self._lock:
            del self._items[id]
    
    def get_all(self):
        with self._lock:
            return [(id, self._items[id]) for id in self._items.keys()]