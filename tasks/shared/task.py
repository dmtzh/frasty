from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import Result, effect

from shared.customtypes import IdValue

class TaskName(str):
    def __new__(cls, value):
        instance = super().__new__(cls, value)
        return instance

    @staticmethod
    def parse(value: str):
        if value is None:
            return None
        match value.strip():
            case "":
                return None
            case task_name:
                return TaskName(task_name)

@dataclass(frozen=True)
class Task:
    name: TaskName
    definition_id: IdValue

class TaskAdapter:
    @effect.result[Task, str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, Task]:
        raw_data = yield from Result.Ok(data) if isinstance(data, dict) and data else Result.Error("data is invalid")
        raw_name = yield from Result.Ok(raw_data["name"]) if "name" in raw_data else Result.Error("name is missing")
        raw_definition_id = yield from Result.Ok(raw_data["definition_id"]) if "definition_id" in raw_data else Result.Error("definition_id is missing")
        
        opt_name = TaskName.parse(raw_name)
        name = yield from Result.Ok(opt_name) if opt_name is not None else Result.Error(f"invalid name {raw_name}")
        
        opt_definition_id = IdValue.from_value(raw_definition_id)
        definition_id = yield from Result.Ok(opt_definition_id) if opt_definition_id is not None else Result.Error(f"invalid definition_id {raw_definition_id}")
        
        return Task(name=name, definition_id=definition_id)

    @staticmethod
    def to_dict(task: Task) -> dict[str, Any]:
        return {
            "name": task.name,
            "definition_id": task.definition_id
        }