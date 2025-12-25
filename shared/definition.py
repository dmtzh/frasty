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
    config: dict[str, Any] | None

@dataclass(frozen=True)
class Definition:
    input_data: dict[str, Any] | list[dict[str, Any]]
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
        def parse_config():
            config_dict = {k: v for k, v in data.items() if k not in ["action", "type", "input_data"] and v is not None}
            return config_dict if config_dict else None
        parsed_name = yield from parse_name()
        parsed_type = yield from parse_type()
        parsed_config = parse_config()
        return ActionDefinition(parsed_name, parsed_type, parsed_config)
    
    @staticmethod   
    def to_dict(action_def: ActionDefinition) -> dict[str, Any]:
        config_dict = action_def.config if action_def.config else {}
        type_dict = {"type": action_def.type.value} if action_def.type != ActionType.CUSTOM else {}
        return config_dict | {
            "action": str(action_def.name)
        } | type_dict

@dataclass(frozen=True)
class StepsMissing:
    '''Definition has no steps'''

class DefinitionAdapter:
    @effect.result[Definition, StepsMissing | list[ValueErr]]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, Definition]:
        def parse_list_data_item(item) -> Result[dict[str, Any], list[ValueErr]]:
            match item:
                case {**item_dict} if item_dict:
                    all_keys_are_strings = all(isinstance(k, str) for k in item_dict)
                    return Result.Ok(item_dict) if all_keys_are_strings else Result.Error([ValueInvalid("input_data")])
                case _:
                    return Result.Error([ValueInvalid("input_data")])
        def parse_input_data(data: dict[str, Any]) -> Result[dict[str, Any] | list[dict[str, Any]], list[ValueErr]]:
            if "input_data" in data:
                match data["input_data"]:
                    case []:
                        return Result.Error([ValueMissing("input_data")])
                    case [*list_data]:
                        return traverse(
                            parse_list_data_item,
                            Block(list_data)
                        ).map(lambda block: list(block) if len(block) > 1 else block.head())
                    case _:
                        return Result.Error([ValueInvalid("input_data")])
            else:
                data_dict = {k: v for k, v in data.items() if k not in ["action", "type", "input_data"] and v is not None}
                return Result.Ok(data_dict) if data_dict else Result.Error([ValueMissing("input_data")])
        first_step_data = yield from Result.Ok(data[0]) if data else Result.Error(StepsMissing())
        input_data = yield from parse_input_data(first_step_data)
        steps = tuple((yield from traverse(ActionDefinitionAdapter.from_dict, Block(data))))
        definition = Definition(input_data, steps)
        return definition
    
    @staticmethod
    def to_list(definition: Definition) -> list[dict[str, Any]]:
        match definition.input_data:
            case {**dict_data}:
                input_data_dict = dict_data
            case [*list_data]:
                input_data_dict = {"input_data": list_data}
        steps = list(map(ActionDefinitionAdapter.to_dict, definition.steps))
        first_step_dict = [input_data_dict | steps[0]]
        next_steps_dict = steps[1:]
        definition_dict = first_step_dict + next_steps_dict
        return definition_dict
