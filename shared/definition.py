from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import Result, effect
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from shared.action import Action, ActionName, ActionType
from shared.validation import ValueInvalid, ValueMissing, ValueError as ValueErr

@dataclass(frozen=True)
class ActionDefinition(Action):
    config: dict | None

@dataclass(frozen=True)
class Definition:
    steps: tuple[ActionDefinition, ...]

type ActionValidationError = ValueMissing | ValueInvalid

class ActionDefinitionAdapter:
    @effect.result[ActionDefinition, list[ActionValidationError]]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, ActionDefinition]:
        def parse_name() -> Result[ActionName, list[ValueErr]]:
            if "action" not in data:
                return Result.Error([ValueMissing("action")])
            raw_name = str(data.get("action") or "").strip()
            return Result.Ok(ActionName(raw_name.lower())) if raw_name else Result.Error([ValueInvalid("action")])
        def parse_type() -> Result[ActionType, list[ValueErr]]:
            raw_type = str(data.get("type", ActionType.CUSTOM) or "")
            opt_type = ActionType.parse(raw_type)
            return Result.Ok(opt_type) if opt_type is not None else Result.Error([ValueInvalid("type")])
        def parse_config():
            raw_config = {k: v for k, v in data.items() if k not in ["action", "type"] and v is not None}
            return raw_config if raw_config else None
        parsed_name = yield from parse_name()
        parsed_type = yield from parse_type()
        parsed_config = parse_config()
        return ActionDefinition(parsed_name, parsed_type, parsed_config)
    
    @staticmethod   
    def to_dict(action_def: ActionDefinition) -> dict[str, Any]:
        config_dict = action_def.config if action_def.config else {}
        type_dict = {"type": str(action_def.type)} if action_def.type != ActionType.CUSTOM else {}
        return config_dict | {
            "action": str(action_def.name)
        } | type_dict

@dataclass(frozen=True)
class StepsMissing:
    '''Definition has no steps'''

class DefinitionAdapter:
    @effect.result[Definition, StepsMissing | list[ActionValidationError]]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, Definition]:
        yield from Result.Ok(None) if data else Result.Error(StepsMissing())
        steps = list((yield from traverse(ActionDefinitionAdapter.from_dict, Block(data))))
        definition = Definition(tuple(steps))
        return definition
    
    @staticmethod
    def to_list(definition: Definition) -> list[dict[str, Any]]:
        steps = list(map(ActionDefinitionAdapter.to_dict, definition.steps))
        return steps