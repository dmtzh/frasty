from dataclasses import dataclass
from enum import StrEnum

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

    def get_name(self):
        return f"frasty_{self.type}_{self.name}"
