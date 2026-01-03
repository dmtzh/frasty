from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import effect

from shared.action import Action, ActionName, ActionType
from shared.customtypes import DefinitionIdValue
from shared.definition import Definition, DefinitionAdapter
from shared.pipeline.actionhandler import ActionData, RunAsyncAction, run_action_adapter
from shared.utils.parse import parse_from_dict

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
    
    @effect.result['ExecuteDefinitionInput', str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, 'ExecuteDefinitionInput']:
        definition_id = yield from parse_from_dict(data, "definition_id", DefinitionIdValue.from_value_with_checksum)
        list_definition = yield from parse_from_dict(data, "definition", lambda lst: lst if isinstance(lst, list) else None)
        definition = yield from DefinitionAdapter.from_list(list_definition).map_error(str)
        return ExecuteDefinitionInput(definition_id, definition)

def run_execute_definition_action(run_action: RunAsyncAction, data: ActionData[None, ExecuteDefinitionInput]):
    execute_definition_dto = ActionData(data.run_id, data.step_id, data.config, data.input.to_dict(), data.metadata)
    return run_action_adapter(run_action)(EXECUTE_DEFINITION_ACTION, execute_definition_dto)