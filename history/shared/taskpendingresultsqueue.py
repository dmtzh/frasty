from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult
from shared.customtypes import RunIdValue, TaskIdValue
from shared.taskresulthistory import DefinitionVersion

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
    def enqueue(self, data: CompletedTaskData) -> None:
        raise NotImplementedError

    def peek(self) -> TaskPendingResultsQueueItem | None:
        raise NotImplementedError

    def dequeue(self) -> TaskPendingResultsQueueItem | None:
        raise NotImplementedError
    
    @property
    def recent_run_id(self) -> RunIdValue | None:
        raise NotImplementedError
    
    @property
    def recent_removed_run_id(self) -> RunIdValue | None:
        raise NotImplementedError


class TaskPendingResultsQueueAdapter:
    @staticmethod
    def to_dict(queue: TaskPendingResultsQueue) -> dict[str, Any]:
        raise NotImplementedError
    
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Result[TaskPendingResultsQueue, str]:
        raise NotImplementedError