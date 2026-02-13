from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import os
from typing import Any

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, DataDto
from shared.utils.parse import parse_from_dict

run_action = config.run_action
action_handler = config.action_handler

GET_DEFINITION_ACTION = Action(ActionName("get_definition"), ActionType.SERVICE)
@dataclass(frozen=True)
class GetDefinitionInput:
    definition_id: DefinitionIdValue
    @staticmethod
    def from_dto(dto: DataDto):
        definition_id_res = parse_from_dict(dto, "definition_id", DefinitionIdValue.from_value_with_checksum)
        return definition_id_res.map(GetDefinitionInput)
def get_definition_handler(func: Callable[[ActionData[None, GetDefinitionInput]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        GET_DEFINITION_ACTION,
        lambda dto_list: GetDefinitionInput.from_dto(dto_list[0])
    )(func)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

app = config.create_faststream_app()