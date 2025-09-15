from collections.abc import Generator
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from expression import Result, effect

from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.utils.string import strip_and_lowercase

class WebApiTaskRunStates(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"

    @staticmethod
    def parse(state: str):
        if state is None:
            return None
        match strip_and_lowercase(state):
            case WebApiTaskRunStates.RUNNING:
                return WebApiTaskRunStates.RUNNING
            case WebApiTaskRunStates.COMPLETED:
                return WebApiTaskRunStates.COMPLETED
            case _:
                return None

@dataclass(frozen=True)
class WebApiTaskRunState:
    state: WebApiTaskRunStates
    result: CompletedResult | None = None
    
    @staticmethod
    def create_running():
        return WebApiTaskRunState(WebApiTaskRunStates.RUNNING)
    
    def complete(self, result: CompletedResult):
        match self.state:
            case WebApiTaskRunStates.RUNNING | WebApiTaskRunStates.COMPLETED:
                return WebApiTaskRunState(WebApiTaskRunStates.COMPLETED, result)
            case _:
                raise RuntimeError(f"Cannot complete web api task run in '{self.state}' state.")
        
    
class WebApiTaskRunStateAdapter:
    @effect.result[WebApiTaskRunState, str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, WebApiTaskRunState]:
        raw_data = yield from Result.Ok(data) if isinstance(data, dict) and data else Result.Error("data is invalid")
        raw_state = yield from Result.Ok(raw_data["state"]) if "state" in raw_data else Result.Error("state is missing")
        opt_state = WebApiTaskRunStates.parse(raw_state)
        state = yield from Result.Ok(opt_state) if opt_state is not None else Result.Error(f"invalid web api task run state {raw_state}")
        raw_result = raw_data.get("result")
        if raw_result is not None:
            result = yield from CompletedResultAdapter.from_dict(raw_result)
            return WebApiTaskRunState(state=state, result=result)
        else:
            return WebApiTaskRunState(state=state)    
    
    @staticmethod
    def to_dict(state: WebApiTaskRunState) -> dict[str, Any]:
        result_dict = {"result": CompletedResultAdapter.to_dict(state.result)} if state.result is not None else {}
        return {"state": state.state.value} | result_dict
        