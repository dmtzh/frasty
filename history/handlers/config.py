from collections.abc import Callable, Coroutine, Generator
from dataclasses import dataclass
import os
from typing import Any

from expression import effect, Result

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.customtypes import DefinitionIdValue, Metadata, TaskIdValue
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, DataDto
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, HandlerContinuation, map_handler, with_middleware
from shared.pipeline.logging import with_input_output_logging_subscriber
from shared.pipeline.types import CompletedDefinitionData
from shared.taskpendingresultsqueue import DefinitionVersion, CompletedTaskData
from shared.utils.parse import parse_from_dict, parse_value

ADD_TASK_RESULT_TO_HISTORY_ACTION = Action(ActionName("add_task_result_to_history"), ActionType.SERVICE)
@dataclass(frozen=True)
class AddTaskResultToHistoryConfig:
    task_id: TaskIdValue
    execution_id: DefinitionIdValue
    @effect.result['AddTaskResultToHistoryConfig', str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, 'AddTaskResultToHistoryConfig']:
        task_id = yield from parse_from_dict(data, "task_id", TaskIdValue.from_value_with_checksum)
        execution_id = yield from parse_from_dict(data, "execution_id", DefinitionIdValue.from_value_with_checksum)
        return AddTaskResultToHistoryConfig(task_id, execution_id)
def add_task_result_to_history_input_validator(data: list[DataDto]):
    return CompletedResultAdapter.from_dict(data[0])
def add_task_result_to_history_handler(func: Callable[[ActionData[AddTaskResultToHistoryConfig, CompletedResult]], Coroutine[Any, Any, CompletedResult | None]]):
    return ActionHandlerFactory(config.run_action, config.action_handler).create(
        ADD_TASK_RESULT_TO_HISTORY_ACTION,
        AddTaskResultToHistoryConfig.from_dict,
        add_task_result_to_history_input_validator
    )(func)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def task_completed_subscriber(func: Callable[[CompletedTaskData], Coroutine[Any, Any, Result | None]]):
    def short_circuit_if_metadata_without_task_id(decoratee: HandlerContinuation[CompletedDefinitionData]):
        async def middleware_func(data_res: Result[CompletedDefinitionData, Any]):
            opt_task_id = data_res.map(lambda data: data.metadata.get_task_id()).default_value(None)
            return await decoratee(data_res) if opt_task_id is not None else None
        return middleware_func
    def to_completed_task_data(data: CompletedDefinitionData):
        raw_definition_version = data.metadata.get("definition_version")
        opt_definition_version = DefinitionVersion.parse(raw_definition_version)
        task_id_res = parse_value(data.metadata, "task_id", Metadata.get_task_id)
        return task_id_res.map(lambda task_id: CompletedTaskData(task_id, data.run_id, data.result, opt_definition_version))
    subscriber = config.definition_completed_subscriber("pending_task_history_results", config.RequeueChance.HIGH)
    with_metadata_task_id_subscriber = with_middleware(subscriber, short_circuit_if_metadata_without_task_id)
    with_logging_subscriber = with_input_output_logging_subscriber(with_metadata_task_id_subscriber, "task_completed")
    task_completed_s = map_handler(
        with_logging_subscriber,
        lambda data_res: data_res.bind(to_completed_task_data)
    )
    return DefinitionCompletedSubscriberAdapter(task_completed_s)(func)

app = config.create_faststream_app()