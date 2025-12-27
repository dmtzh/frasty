from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import effect

from shared.action import Action, ActionName, ActionType
from shared.customtypes import DefinitionIdValue
from shared.definition import Definition, DefinitionAdapter
from shared.pipeline.actionhandler import ActionData, ActionDataInput, RunAsyncAction, run_action_adapter
from shared.utils.parse import parse_from_dict

EXECUTE_DEFINITION_ACTION = Action(ActionName("execute_definition"), ActionType.CORE)

@dataclass(frozen=True)
class ExecuteDefinitionInput(ActionDataInput):
    opt_definition_id: DefinitionIdValue | None
    definition: Definition
    
    def to_dict(self):
        definition_id_dict = {"definition_id": self.opt_definition_id.to_value_with_checksum()} if self.opt_definition_id is not None else {}
        definition_dict = {"definition": DefinitionAdapter.to_list(self.definition)}
        return definition_id_dict | definition_dict
    
    def to_list(self):
        return [self.to_dict()]
    
    @effect.result['ExecuteDefinitionInput', str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, 'ExecuteDefinitionInput']:
        if "definition_id" in data:
            opt_definition_id = yield from parse_from_dict(data, "definition_id", DefinitionIdValue.from_value_with_checksum)
        else:
            opt_definition_id = None
        list_definition = yield from parse_from_dict(data, "definition", lambda lst: lst if isinstance(lst, list) else None)
        definition = yield from DefinitionAdapter.from_list(list_definition).map_error(str)
        return ExecuteDefinitionInput(opt_definition_id, definition)

def run_execute_definition_action(run_action: RunAsyncAction, data: ActionData[None, ExecuteDefinitionInput]):
    return run_action_adapter(run_action)(EXECUTE_DEFINITION_ACTION, data)