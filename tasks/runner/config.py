from collections.abc import Callable, Coroutine
import os
from typing import Any

from expression import Result

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue
from shared.pipeline.handlers import HandlerAdapter, with_input_output_logging
from shared.pipeline.types import RunTaskData

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def run_task_handler(func: Callable[[RunTaskData], Coroutine[Any, Any, Result | None]]):
    handler = config.run_task_handler()
    run_task_data_handler_with_logging = with_input_output_logging(handler, "run_task")
    return HandlerAdapter(run_task_data_handler_with_logging)(func)

run_definition = config.run_definition

def publish_completed_definition(run_id: RunIdValue, definition_id: DefinitionIdValue, result: CompletedResult, metadata: Metadata):
    return config.publish_completed_definition(run_id, definition_id, result, metadata.to_dict())

app = config.create_faststream_app()