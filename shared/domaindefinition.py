from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Generator
from dataclasses import dataclass
from types import UnionType
from typing import Any, Generic, TypeVar, Union, get_origin

from expression import Result, effect

from .validation import ValueError as ValueErr

TCfg = TypeVar("TCfg")

class StepDefinition(ABC, Generic[TCfg]):
    def __init__(self, config: TCfg):
        self._config = config
    
    @property
    @abstractmethod
    def input_type(self) -> type:
        '''The type of the input data'''
    
    @property
    @abstractmethod
    def output_type(self) -> type:
        '''The type of the output data'''
    
    @property
    def config(self) -> TCfg:
        return self._config

@dataclass(frozen=True)
class StepNotAllowed:
    value: StepDefinition
class StepsMissing:
    pass

class Definition:
    def __init__(self, input_data: Any, steps: tuple[StepDefinition, ...]):
        if not isinstance(steps, tuple):
            raise TypeError("steps must be a tuple")
        match steps:
            case (first_step, *_):
                if first_step.validate_input(input_data).is_error():
                    raise ValueError("input_data is not valid for the first step")        
            case _:
                raise ValueError("steps must not be empty")
        self._input_data = input_data
        self._steps = steps
    
    @property
    def input_data(self):
        return self._input_data

    @property
    def steps(self):
        return self._steps
    
    @staticmethod
    @effect.result["Definition", StepNotAllowed | StepsMissing | list[ValueErr]]()
    def from_steps(input_data_dict: dict[str, Any], steps: list[StepDefinition]) -> Generator[Any, Any, Definition]:
        # Res = Result[Definition, StepNotAllowed | StepsMissing]
        def to_head_and_tail(steps: list[StepDefinition]) -> tuple[StepDefinition, list[StepDefinition]] | None:
            match steps:
                case None | []:
                    return None
                case [head, *tail]:
                    return head, tail
        def validate_next_steps(curr_step: StepDefinition, next_steps: list[StepDefinition]) -> Result[None, StepNotAllowed]:
            match next_steps:
                case []:
                    return Result.Ok(None)
                case [next_step, *tail]:
                    if curr_step.output_type == next_step.input_type:
                        return validate_next_steps(next_step, tail)
                    elif issubclass(curr_step.output_type, next_step.input_type):
                        return validate_next_steps(next_step, tail)
                    elif get_origin(next_step.input_type) in {Union, UnionType}:
                        return Result.Error(StepNotAllowed(next_step))
                    elif curr_step.output_type.__name__ == next_step.input_type.__name__:
                        return validate_next_steps(next_step, tail)
                    else:
                        return Result.Error(StepNotAllowed(next_step))
                case _:
                    raise RuntimeError("This should never happen")

        match to_head_and_tail(steps):
            case (first_step, next_steps):
                input_data = yield from first_step.validate_input(input_data_dict)
                yield from validate_next_steps(first_step, next_steps)
            case None:
                yield from Result.Error(StepsMissing())
        return Definition(input_data, tuple(steps))