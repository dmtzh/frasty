from collections.abc import Callable, Coroutine, Generator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Any

from expression import effect, Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.definition import Definition, DefinitionAdapter, StepsMissing
from shared.definitionsstore import DefinitionsStore
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, DataDto, run_action_adapter
from shared.utils.parse import parse_dict_field
from shared.utils.result import traverse_accumulating_with_index
from shared.validation import ValueError as ValueErr, ValueMissing, format_value_errors

run_action = config.run_action

MANUAL_RUN_ACTION = Action(ActionName("manual_run"), ActionType.SERVICE)
@dataclass(frozen=True)
class ManualRunInput:
    definition: Definition
    def to_dto(self):
        return {"definition": DefinitionAdapter.to_list(self.definition)}
    @effect.result['ManualRunInput', StepsMissing | list[ValueErr]]()
    @staticmethod
    def from_dto(dto: dict[str, Any]) -> Generator[Any, Any, 'ManualRunInput']:
        list_definition = yield from parse_dict_field(dto, "definition", lambda lst: lst if isinstance(lst, list) else None).map_error(lambda err: [err])
        definition = yield from DefinitionAdapter.from_list(list_definition)
        return ManualRunInput(definition)
async def run_manual_run_action(data: ActionData[None, ManualRunInput]):
    run_action_dto = ActionData(data.run_id, data.step_id, data.config, data.input.to_dto(), data.metadata)
    run_action_res = await run_action_adapter(config.run_action)(MANUAL_RUN_ACTION, run_action_dto)
    return run_action_res.map(lambda _: data)
def manual_run_handler(func: Callable[[ActionData[None, ManualRunInput]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        MANUAL_RUN_ACTION,
        lambda dto_list: ManualRunInput.from_dto(dto_list[0]).map_error(lambda errs: str(errs) if isinstance(errs, StepsMissing) else format_value_errors(tuple(errs)))
    )(func)

@dataclass(frozen=True)
class CompleteManualRunInput:
    result: CompletedResult
    data: DataDto
COMPLETE_MANUAL_RUN_ACTION = Action(ActionName("complete_manual_run"), ActionType.SERVICE)
def complete_manual_run_handler(func: Callable[[ActionData[None, list[CompleteManualRunInput]]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create_without_config(
        COMPLETE_MANUAL_RUN_ACTION,
        complete_manual_run_handler_input_validator
    )(func)
def complete_manual_run_handler_input_validator(dto_list: list[DataDto]) -> Result[list[CompleteManualRunInput], Any]:
    if not dto_list:
        return Result.Error(format_value_errors((ValueMissing("input_data"),)))

    def validate_item(idx: int, dto: DataDto) -> Result[CompleteManualRunInput, str]:
        return CompletedResultAdapter.from_dict(dto) \
            .map(lambda res: CompleteManualRunInput(res, dto)) \
            .map_error(lambda err: f"input_data[{idx}].{err}")
    return traverse_accumulating_with_index(dto_list, validate_item) \
        .map_error(", ".join)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

definitions_storage = DefinitionsStore(STORAGE_ROOT_FOLDER)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await config._rabbit_broker.start()
    yield
    await config._rabbit_broker.stop()

app = FastAPI(lifespan=lifespan)