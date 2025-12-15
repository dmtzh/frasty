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
    data: dict | None

@dataclass(frozen=True)
class Definition:
    steps: tuple[ActionDefinition, ...]

class ActionDefinitionAdapter:
    @effect.result[ActionDefinition, list[ValueErr]]()
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
        def parse_data():
            raw_data = {k: v for k, v in data.items() if k not in ["action", "type"] and v is not None}
            return raw_data if raw_data else None
        parsed_name = yield from parse_name()
        parsed_type = yield from parse_type()
        parsed_data = parse_data()
        return ActionDefinition(parsed_name, parsed_type, parsed_data)
    
    @staticmethod   
    def to_dict(action_def: ActionDefinition) -> dict[str, Any]:
        data_dict = action_def.data if action_def.data else {}
        type_dict = {"type": str(action_def.type)} if action_def.type != ActionType.CUSTOM else {}
        return data_dict | {
            "action": str(action_def.name)
        } | type_dict

@dataclass(frozen=True)
class StepsMissing:
    '''Definition has no steps'''

class DefinitionAdapter:
    @effect.result[Definition, StepsMissing | list[ValueErr]]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, Definition]:
        yield from Result.Ok(None) if data else Result.Error(StepsMissing())
        steps = tuple((yield from traverse(ActionDefinitionAdapter.from_dict, Block(data))))
        definition = Definition(steps)
        return definition
    
    @staticmethod
    def to_list(definition: Definition) -> list[dict[str, Any]]:
        steps = list(map(ActionDefinitionAdapter.to_dict, definition.steps))
        return steps