from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import Result, effect

from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.customtypes import RunIdValue
from shared.utils.parse import parse_value

class DefinitionVersion(int):
    def __new__(cls, value):
        instance = super().__new__(cls, value)
        return instance
    
    @staticmethod
    def parse(value):
        try:
            return DefinitionVersion(value)
        except (ValueError, TypeError):
            return None

@dataclass(frozen=True)
class TaskResultHistoryItem:
    result: CompletedResult
    timestamp: int
    definition_version: DefinitionVersion | None
    prev_run_id: RunIdValue | None

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
                definition_version = None
            case raw_definition_version:
                definition_version = yield from parse_value(raw_definition_version, "definition_version", DefinitionVersion.parse)
        opt_raw_prev_run_id = raw_data.get("prev_run_id")
        match opt_raw_prev_run_id:
            case None:
                prev_run_id = None
            case raw_prev_run_id:
                prev_run_id = yield from parse_value(raw_prev_run_id, "prev_run_id", RunIdValue.from_value_with_checksum)
        return TaskResultHistoryItem(result=result, timestamp=timestamp, definition_version=definition_version, prev_run_id=prev_run_id)
        
    
    @staticmethod
    def to_dict(item: TaskResultHistoryItem) -> dict[str, Any]:
        result_dict = {"result": CompletedResultAdapter.to_dict(item.result)}
        definition_version_dict = {"definition_version": item.definition_version} if item.definition_version is not None else {}
        prev_run_id_dict = {"prev_run_id": item.prev_run_id.to_value_with_checksum()} if item.prev_run_id is not None else {}
        return result_dict | {"timestamp": item.timestamp} | definition_version_dict | prev_run_id_dict