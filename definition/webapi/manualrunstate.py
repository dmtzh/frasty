from collections.abc import Generator
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from expression import Result, effect
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.domaindefinition import Definition
from shared.dtodefinition import DefinitionAdapter
from shared.utils.string import strip_and_lowercase

class ManualRunStates(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed out"

    @staticmethod
    def parse(state: str):
        if state is None:
            return None
        match strip_and_lowercase(state):
            case ManualRunStates.RUNNING:
                return ManualRunStates.RUNNING
            case ManualRunStates.COMPLETED:
                return ManualRunStates.COMPLETED
            case ManualRunStates.FAILED:
                return ManualRunStates.FAILED
            case ManualRunStates.TIMED_OUT:
                return ManualRunStates.TIMED_OUT
            case _:
                return None

@dataclass(frozen=True)
class ManualRunState:
    state: ManualRunStates
    definition: Definition
    result: CompletedResult | None = None
    
    @staticmethod
    def create_running(definition: Definition):
        return ManualRunState(ManualRunStates.RUNNING, definition)
    
    def complete(self, result: CompletedResult):
        match self.state:
            case ManualRunStates.RUNNING | ManualRunStates.COMPLETED:
                return ManualRunState(ManualRunStates.COMPLETED, self.definition, result)
            case _:
                raise RuntimeError(f"Cannot complete manual run definition in '{self.state}' state.")
        
    
class ManualRunStateAdapter:
    @effect.result[ManualRunState, str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, ManualRunState]:
        raw_data = yield from Result.Ok(data) if isinstance(data, dict) and data else Result.Error("data is invalid")
        raw_state = yield from Result.Ok(raw_data["state"]) if "state" in raw_data else Result.Error("state is missing")
        opt_state = ManualRunStates.parse(raw_state)
        state = yield from Result.Ok(opt_state) if opt_state is not None else Result.Error(f"invalid manual run state {raw_state}")
        raw_definition = yield from Result.Ok(raw_data["definition"]) if "definition" in raw_data else Result.Error("definition is missing")
        definition = yield from DefinitionAdapter.from_list(raw_definition).map_error(str)
        raw_result = raw_data.get("result")
        if raw_result is not None:
            result = yield from CompletedResultAdapter.from_dict(raw_result)
            return ManualRunState(state=state, definition=definition, result=result)
        else:
            return ManualRunState(state=state, definition=definition)    
    
    @staticmethod
    def to_dict(state: ManualRunState) -> dict[str, Any]:
        result_dict = {"result": CompletedResultAdapter.to_dict(state.result)} if state.result is not None else {}
        return {"state": state.state.value, "definition": DefinitionAdapter.to_list(state.definition)} | result_dict
        