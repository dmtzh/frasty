from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import os
from typing import Any

from expression import Result

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, TaskIdValue
from shared.pipeline.handlers import HandlerAdapter, map_handler, with_input_output_logging

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

@dataclass(frozen=True)
class RunTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: Metadata
    
def run_task_handler(func: Callable[[RunTaskData], Coroutine[Any, Any, Result | None]]):
    handler = config.run_task_handler()
    run_task_data_handler = map_handler(handler, lambda input_data_res: input_data_res.map(lambda input_data: RunTaskData(*input_data)))
    run_task_data_handler_with_logging = with_input_output_logging(run_task_data_handler, "run_task")
    return HandlerAdapter(run_task_data_handler_with_logging)(func)

run_definition = config.run_definition

def publish_completed_definition(run_id: RunIdValue, definition_id: DefinitionIdValue, result: CompletedResult, metadata: Metadata):
    return config.publish_completed_definition(run_id, definition_id, result, metadata.to_dict())

app = config.create_faststream_app()