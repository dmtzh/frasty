from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import Result, effect

from shared.completedresult import CompletedResult, CompletedResultAdapter

class DefinitionVersion(int):
    def __new__(cls, value):
        instance = super().__new__(cls, value)
        return instance

@dataclass(frozen=True)
class TaskResultHistoryItem:
    result: CompletedResult
    timestamp: int
    opt_definition_version: DefinitionVersion | None

class TaskResultHistoryItemAdapter:
    @effect.result[TaskResultHistoryItem, str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, TaskResultHistoryItem]:
        raw_data = yield from Result.Ok(data) if isinstance(data, dict) and data else Result.Error("data is invalid")
        raw_result = yield from Result.Ok(raw_data["result"]) if "result" in raw_data else Result.Error("result is missing")
        result = yield from CompletedResultAdapter.from_dict(raw_result)
        raw_timestamp = yield from Result.Ok(raw_data["timestamp"]) if "timestamp" in raw_data else Result.Error("timestamp is missing")
        timestamp = yield from Result.Ok(raw_timestamp) if isinstance(raw_timestamp, int) else Result.Error("timestamp is invalid")
        opt_raw_definition_version = raw_data.get("definition_version")
        match opt_raw_definition_version:
            case None:
                opt_definition_version = None
            case raw_definition_version:
                opt_definition_version = yield from Result.Ok(DefinitionVersion(raw_definition_version)) if isinstance(raw_definition_version, int) else Result.Error("definition_version is invalid")
        return TaskResultHistoryItem(result=result, timestamp=timestamp, opt_definition_version=opt_definition_version)
        
    
    @staticmethod
    def to_dict(item: TaskResultHistoryItem) -> dict[str, Any]:
        result_dict = {"result": CompletedResultAdapter.to_dict(item.result)}
        definition_version_dict = {"definition_version": item.opt_definition_version} if item.opt_definition_version is not None else {}
        return result_dict | {"timestamp": item.timestamp} | definition_version_dict