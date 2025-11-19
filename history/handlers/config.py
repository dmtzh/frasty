from collections.abc import Callable, Coroutine
import os
from typing import Any

from expression import Result

from infrastructure.rabbitmq import config
from shared.customtypes import Metadata
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, HandlerContinuation, map_handler, with_middleware
from shared.pipeline.logging import with_input_output_logging_subscriber
from shared.pipeline.types import CompletedDefinitionData
from shared.taskpendingresultsqueue import DefinitionVersion, CompletedTaskData
from shared.utils.parse import parse_value

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