from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
import re

from shared.utils.string import strip_and_lowercase

# Pre-compile the regex pattern for performance.
# Allows only lowercase Latin letters, digits, underscores, and hyphens.
_PATTERN = re.compile(r"^[a-z0-9_-]+$")

class ActionName(str):
    """
    A domain-specific string type representing the name of an action.
    Enforces the invariant that the value matches ^[a-z0-9_-]+$.
    
    Construction modes:
    1. Direct construction via ActionName(value):
       - Accepts ONLY str values.
       - Raises TypeError for non-str types.
       - Raises ValueError if the string does not match ^[a-z0-9_-]+$.
       - Does NOT apply strip() or lowercase().
       - Use this mode when the caller is certain the value is valid.
       
    2. Safe parsing via ActionName.parse(value):
       - Accepts any value, but only processes str.
       - Applies strip_and_lowercase() before validation.
       - Returns ActionName on success, or None on any failure.
       - Never raises exceptions.
       - Use this mode when input comes from untrusted sources (e.g., JSON).
    """
    
    def __new__(cls, value: str) -> ActionName:
        # 1. Reject non-str types explicitly.
        if not isinstance(value, str):
            raise TypeError(f"Expected a str, got {type(value).__name__}")
        
        # 2. Reject strings that do not match the allowed alphabet.
        if not _PATTERN.fullmatch(value):
            raise ValueError(f"Expected a string matching ^[a-z0-9_-]+$, got {value!r}")
            
        return super().__new__(cls, value)

    def __repr__(self) -> str:
        """
        Return a string representation that clearly identifies this as an ActionName.
        Useful for debugging and logging to distinguish from plain str.
        """
        return f"ActionName({str.__repr__(self)})"

    def __str__(self) -> str:
        """
        Return the plain string representation.
        Ensures that str(), f-strings, and format() return the plain value.
        """
        return str.__str__(self)

    @classmethod
    def parse(cls, value: object) -> ActionName | None:
        """
        Safely attempt to convert a value to an ActionName.
        Applies automatic normalization (strip and lowercase) for convenience.
        Returns an ActionName instance on success, or None on any failure.
        """
        # 1. Reject non-str types.
        if not isinstance(value, str):
            return None
            
        # 2. Apply normalization.
        normalized = strip_and_lowercase(value)
        
        # 3. Reject empty strings (after stripping).
        if not normalized:
            return None
            
        # 4. Validate against the allowed alphabet.
        if not _PATTERN.fullmatch(normalized):
            return None
            
        # 5. Construct and return.
        return cls(normalized)

class ActionType(StrEnum):
    CORE = "core"
    SERVICE = "service"
    CUSTOM = "custom"

    @staticmethod
    def parse(action_type: str) -> ActionType | None:
        if action_type is None:
            return None
        match strip_and_lowercase(action_type):
            case ActionType.CORE:
                return ActionType.CORE
            case ActionType.SERVICE:
                return ActionType.SERVICE
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
