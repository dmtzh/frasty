from __future__ import annotations
from collections.abc import Callable, Generator
from dataclasses import dataclass
import inspect
from typing import Any

from expression import Result, effect
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from . import domaindefinition as shdomaindef
from .infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage, get_step_definition_name
from .validation import ValueInvalid, ValueMissing, ValueError as ValueErr

@dataclass(frozen=True)
class UnsupportedStep:
    value: str
type StepValidationError = UnsupportedStep | ValueMissing | ValueInvalid

class StepDefinitionAdapter:
    @effect.result[shdomaindef.StepDefinition, list[StepValidationError]]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, shdomaindef.StepDefinition]:
        def parse_step() -> Result[str, list[ValueErr]]:
            if "step" not in data:
                return Result.Error([ValueMissing("step")])
            raw_step = str(data.get("step") or "").strip()
            return Result.Ok(raw_step.lower()) if raw_step else Result.Error([ValueInvalid("step")])
        def get_step_definition_creator(parsed_step: str) -> Result[Callable[..., Result[shdomaindef.StepDefinition, list[ValueErr]]], list[UnsupportedStep]]:
            opt_creator = step_definition_creators_storage.get(parsed_step)
            return Result.Ok(opt_creator) if opt_creator is not None else Result.Error([UnsupportedStep(data["step"])])
        def create_step(creator: Callable[..., Result[shdomaindef.StepDefinition, list[ValueErr]]]) -> Result[shdomaindef.StepDefinition, list[ValueErr]]:
            params = inspect.signature(creator).parameters
            missing_params = [p for p in params if p not in data]
            missing_params_without_default_val = [p for p in missing_params if params[p].default is inspect.Parameter.empty]
            match missing_params_without_default_val:
                case []:
                    creator_params = {k: v for k, v in data.items() if k in params}
                    return creator(**creator_params)
                case _:
                    missing_values_err = list[ValueErr](ValueMissing(missing_param) for missing_param in missing_params_without_default_val)
                    return Result.Error(missing_values_err)
        parsed_step = yield from parse_step()
        creator = yield from get_step_definition_creator(parsed_step)
        step = yield from create_step(creator)
        return step
    
    @staticmethod   
    def to_dict(step_def: shdomaindef.StepDefinition) -> dict[str, Any]:
        step = get_step_definition_name(type(step_def))
        match step_def.config:
            case None:
                config = {}
            case step_def_config:
                if callable(getattr(step_def_config, "to_dict", None)):
                    config = step_def_config.to_dict()
                else:
                    config = {k: v for k, v in vars(step_def_config).items() if v is not None}
        return {"step": step, **config}

class DefinitionAdapter:
    @effect.result[shdomaindef.Definition, shdomaindef.StepsMissing | shdomaindef.StepNotAllowed | list[StepValidationError]]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, shdomaindef.Definition]:
        raw_input_data = yield from Result.Ok(data[0]) if data else Result.Error(shdomaindef.StepsMissing())
        steps = list((yield from traverse(StepDefinitionAdapter.from_dict, Block(data))))
        # exclude_step_and_config_keys not needed for now, but possibly can be used in the future
        input_data = InputDataDict.exclude_step_and_config_keys(raw_input_data, steps[0])
        definition = yield from shdomaindef.Definition.from_steps(input_data, steps)
        return definition
    
    @staticmethod
    def to_list(definition: shdomaindef.Definition) -> list[dict[str, Any]]:
        input_data_dict = definition.input_data if isinstance(definition.input_data, dict) else vars(definition.input_data)
        steps = list(map(StepDefinitionAdapter.to_dict, definition.steps))
        first_step_dict =[{**steps[0], **input_data_dict}]
        next_steps_dict = steps[1:]
        definition_dict = first_step_dict + next_steps_dict
        return definition_dict

class InputDataDict:
    @staticmethod
    def exclude_step_and_config_keys(raw_data: dict, step_def: shdomaindef.StepDefinition):
        create_params = inspect.signature(type(step_def).create).parameters.keys()
        keys_to_exclude = ["step", *create_params]
        raw_data_filtered = {k: v for k, v in raw_data.items() if k not in keys_to_exclude}
        return raw_data_filtered