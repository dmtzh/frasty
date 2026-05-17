from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import effect
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from shared.action import Action, ActionName, ActionType
from shared.customtypes import DefinitionIdValue
from shared.definition import Definition, DefinitionAdapter
from shared.pipeline.actionhandler import ActionData, RunAsyncAction, run_action_adapter
from shared.utils.parse import parse_from_dict, parse_value
from shared.utils.result import apply

EXECUTE_DEFINITION_ACTION = Action(ActionName("execute_definition"), ActionType.CORE)
@dataclass(frozen=True)
class ExecuteDefinitionInput:
    definition_id: DefinitionIdValue
    definition: Definition
    
    def to_dict(self):
        return {
            "definition_id": self.definition_id.to_value_with_checksum(),
            "definition": DefinitionAdapter.to_list(self.definition)
        }
    
    @staticmethod
    def from_dict(data: dict[str, Any]):
        @effect.result[Definition, str]()
        def parse_definition() -> Generator[Any, Any, Definition]:
            list_definition = yield from parse_from_dict(data, "definition", lambda lst: lst if isinstance(lst, list) and lst else None)
            list_of_dict_definitions = yield from traverse(
                lambda raw_def: parse_value(raw_def, "definition", lambda raw_def: raw_def if isinstance(raw_def, dict) and raw_def else None),
                Block(list_definition)
            ).map(list)
            definition = yield from DefinitionAdapter.from_list(list_of_dict_definitions).map_error(str)
            return definition
        definition_id_res = parse_from_dict(data, "definition_id", DefinitionIdValue.from_value_with_checksum)
        definitions_res = parse_definition()
        return apply(ExecuteDefinitionInput, ", ".join, definition_id_res, definitions_res)

def run_execute_definition_action(run_action: RunAsyncAction, data: ActionData[None, ExecuteDefinitionInput]):
    execute_definition_dto = ActionData(data.run_id, data.step_id, data.config, data.input.to_dict(), data.metadata)
    return run_action_adapter(run_action)(EXECUTE_DEFINITION_ACTION, execute_definition_dto)

@dataclass(frozen=True)
class ExecuteGroupOfDefinitionsInput:
    items: list[ExecuteDefinitionInput]
    
    def to_list(self):
        return [item.to_dict() for item in self.items]
    
    @effect.result['ExecuteGroupOfDefinitionsInput', str]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, 'ExecuteGroupOfDefinitionsInput']:
        non_empty_list = yield from parse_value(data, "definitions", lambda lst: lst if isinstance(lst, list) and lst else None)
        list_of_dicts = yield from parse_value(non_empty_list, "definitions", lambda lst: lst if all(isinstance(item, dict) and item for item in lst) else None)
        exec_def_inputs_res = yield from traverse(ExecuteDefinitionInput.from_dict, Block(list_of_dicts)).map(list)
        return ExecuteGroupOfDefinitionsInput(exec_def_inputs_res)
