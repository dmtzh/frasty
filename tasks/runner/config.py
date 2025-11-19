from collections.abc import Callable, Coroutine
import os
from typing import Any

from expression import Result

from infrastructure.rabbitmq import config
from shared.pipeline.handlers import HandlerAdapter
from shared.pipeline.logging import with_input_output_logging
from shared.pipeline.types import RunTaskData

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def run_task_handler(func: Callable[[RunTaskData], Coroutine[Any, Any, Result | None]]):
    handler = config.run_task_handler()
    handler_with_logging = with_input_output_logging(handler, "run_task")
    return HandlerAdapter(handler_with_logging)(func)

run_definition = config.run_definition

publish_completed_definition = config.publish_completed_definition

app = config.create_faststream_app()