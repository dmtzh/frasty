from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class ValueMissing:
    name: str

@dataclass(frozen=True)
class ValueInvalid:
    name: str
    value: Any = None  # optional, backward-compatible

type ValueError = ValueInvalid | ValueMissing

@dataclass(frozen=True)
class InvalidId:
    '''Invalid id value'''