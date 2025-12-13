from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult

class ActionName(str):
    def __new__(cls, value):
        instance = super().__new__(cls, value)
        return instance

class ActionType(StrEnum):
    CORE = "core"

@dataclass(frozen=True)
class Action:
    name: ActionName
    type: ActionType

@dataclass(frozen=True)
class ActionDataDto:
    run_id: str
    step_id: str
    config: dict | None
    data: dict | list
    metadata: dict

type ActionHandler = Callable[[Result[ActionDataDto, Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]