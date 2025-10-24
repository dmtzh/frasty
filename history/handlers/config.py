from collections.abc import Callable
import os

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

class definition_completed_subscriber[T](config.definition_completed_subscriber[T]):
    def __init__(self, input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], T]):
        super().__init__(input_adapter, "pending_task_history_results", config.RequeueChance.HIGH)

app = config.create_faststream_app()