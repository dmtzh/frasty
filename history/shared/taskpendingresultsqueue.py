from collections import deque
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import effect
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.customtypes import RunIdValue, TaskIdValue
from shared.taskresulthistory import DefinitionVersion
from shared.utils.parse import parse_from_dict, parse_value

@dataclass(frozen=True)
class CompletedTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedResult
    opt_definition_version: DefinitionVersion | None

@dataclass(frozen=True)
class TaskPendingResultsQueueItem:
    data: CompletedTaskData
    prev_run_id: RunIdValue | None

class TaskPendingResultsQueue:
    def __init__(self, recent_dequeued_run_ids: list[RunIdValue] = []):
        self._items = deque[TaskPendingResultsQueueItem]()
        self._recent_dequeued_run_ids = deque[RunIdValue](recent_dequeued_run_ids[:10], maxlen=10)
        self._recent_run_id = self.recent_dequeued_run_id
        self._tmp = True

    def enqueue(self, data: CompletedTaskData) -> None:
        has_item = any(True for item in self._items if item.data.run_id == data.run_id)
        has_recently_dequeued_item = data.run_id in self._recent_dequeued_run_ids
        if has_item or has_recently_dequeued_item:
            return
        new_item = TaskPendingResultsQueueItem(data, self._recent_run_id)
        self._items.append(new_item)
        self._recent_run_id = data.run_id

    def peek(self) -> TaskPendingResultsQueueItem | None:
        return next(iter(self._items), None)

    def dequeue(self) -> TaskPendingResultsQueueItem | None:
        try:
            item = self._items.popleft()
            self._recent_dequeued_run_ids.appendleft(item.data.run_id)
            return item
        except IndexError:
            return None
    
    @property
    def recent_run_id(self) -> RunIdValue | None:
        return self._recent_run_id
    
    @property
    def recent_dequeued_run_id(self) -> RunIdValue | None:
        return next(iter(self._recent_dequeued_run_ids), None)

class CompletedTaskDataAdapter:
    @staticmethod
    def to_dict(data: CompletedTaskData) -> dict[str, Any]:
        result_dict = {"result": CompletedResultAdapter.to_dict(data.result)}
        definition_version_dict = {"definition_version": data.opt_definition_version} if data.opt_definition_version is not None else {}
        data_dict = {"task_id": data.task_id, "run_id": data.run_id} | result_dict | definition_version_dict
        return data_dict
    
    @effect.result[CompletedTaskData, str]()
    @staticmethod
    def from_dict(raw_data: dict[str, Any]) -> Generator[Any, Any, CompletedTaskData]:
        data_dict = yield from parse_value(raw_data, "data", lambda raw_data: raw_data if isinstance(raw_data, dict) and raw_data else None)
        task_id = yield from parse_from_dict(data_dict, "task_id", TaskIdValue.from_value)
        run_id = yield from parse_from_dict(data_dict, "run_id", RunIdValue.from_value)
        result_dict = yield from parse_from_dict(data_dict, "result", lambda raw_result: raw_result if isinstance(raw_result, dict) and raw_result else None)
        result = yield from CompletedResultAdapter.from_dict(result_dict)
        opt_raw_definition_version = data_dict.get("definition_version")
        match opt_raw_definition_version:
            case None:
                definition_version = None
            case raw_definition_version:
                definition_version = yield from parse_value(raw_definition_version, "definition_version", DefinitionVersion.parse)
        return CompletedTaskData(task_id, run_id, result, definition_version)

class TaskPendingResultsQueueAdapter:
    @staticmethod
    def to_dict(queue: TaskPendingResultsQueue) -> dict[str, Any]:
        items_dict = {"items": list(map(CompletedTaskDataAdapter.to_dict, (item.data for item in queue._items)))}
        recent_dequeued_run_ids_dict = {"recent_dequeued_run_ids": list(queue._recent_dequeued_run_ids)}
        return items_dict | recent_dequeued_run_ids_dict
    
    @effect.result[TaskPendingResultsQueue, str]()
    @staticmethod
    def from_dict(raw_data: dict[str, Any]) -> Generator[Any, Any, TaskPendingResultsQueue]:
        def parse_run_id(raw_run_id):
            return parse_value(raw_run_id, "run_id", RunIdValue.from_value)
        data = yield from parse_value(raw_data, "data", lambda raw_data: raw_data if isinstance(raw_data, dict) and raw_data else None)
        raw_items = yield from parse_from_dict(data, "items", lambda raw_items: raw_items if isinstance(raw_items, list) else None)
        raw_recent_dequeued_run_ids = yield from parse_from_dict(data, "recent_dequeued_run_ids", lambda raw_recent_dequeued_run_ids: raw_recent_dequeued_run_ids if isinstance(raw_recent_dequeued_run_ids, list) else None)
        task_data_items = list((yield from traverse(CompletedTaskDataAdapter.from_dict, Block(raw_items))))
        recent_dequeued_run_ids = list((yield from traverse(parse_run_id, Block(raw_recent_dequeued_run_ids))))
        queue = TaskPendingResultsQueue(recent_dequeued_run_ids)
        for task_data in task_data_items:
            queue.enqueue(task_data)
        return queue