from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import os
from typing import Any

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue
from shared.definitionsstore import DefinitionsStore
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, DataDto
from shared.runningdefinitionsstore import GroupOfRunningDefinitionsStore, RunningDefinitionsStore
from shared.utils.parse import parse_dict_field
from shared.utils.result import traverse_accumulating_with_index
from shared.validation import format_value_errors, with_prefix

run_action = config.run_action
action_handler = config.action_handler

GET_DEFINITION_ACTION = Action(ActionName("get_definition"), ActionType.SERVICE)
@dataclass(frozen=True)
class GetDefinitionInput:
    definition_id: DefinitionIdValue
    @staticmethod
    def from_dto(dto: DataDto):
        definition_id_res = parse_dict_field(dto, "definition_id", DefinitionIdValue.from_value_with_checksum)
        return definition_id_res.map(GetDefinitionInput)
def get_definition_handler(func: Callable[[ActionData[None, GetDefinitionInput]], Coroutine[Any, Any, CompletedResult | None]]):
    prefixed_validator = lambda idx, dto: GetDefinitionInput.from_dto(dto)\
        .map_error(lambda err: with_prefix(f"input_data[{idx}]", (err,)))
    # Loads definition only for first item from list. Does not support list of definitions at the moment.
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        GET_DEFINITION_ACTION,
        lambda dto_list: traverse_accumulating_with_index(dto_list[:1], prefixed_validator)\
            .map(lambda inputs: inputs[0])\
            .map_error(format_value_errors)
    )(func)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

running_definitions_storage = RunningDefinitionsStore(STORAGE_ROOT_FOLDER)
group_of_running_definitions_storage = GroupOfRunningDefinitionsStore(STORAGE_ROOT_FOLDER)
definitions_storage = DefinitionsStore(STORAGE_ROOT_FOLDER)

app = config.create_faststream_app()