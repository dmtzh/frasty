from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum

from shared.utils.string import strip_and_lowercase

class ActionName(str):
    def __new__(cls, value):
        instance = super().__new__(cls, value)
        return instance

class ActionType(StrEnum):
    CORE = "core"
    CUSTOM = "custom"

    @staticmethod
    def parse(action_type: str) -> ActionType | None:
        if action_type is None:
            return None
        match strip_and_lowercase(action_type):
            case ActionType.CORE:
                return ActionType.CORE
            case ActionType.CUSTOM:
                return ActionType.CUSTOM
            case _:
                return None

@dataclass(frozen=True)
class Action:
    name: ActionName
    type: ActionType

    def get_name(self):
        return f"frasty_{self.type}_{self.name}"
