from dataclasses import dataclass

@dataclass(frozen=True)
class ValueMissing:
    name: str

@dataclass(frozen=True)
class ValueInvalid:
    name: str

type ValueError = ValueInvalid | ValueMissing

@dataclass(frozen=True)
class InvalidId:
    '''Invalid id value'''