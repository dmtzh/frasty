from collections.abc import Callable
import os

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def definition_completed_subscriber[T](input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], T]):
    subscriber = config.definition_completed_subscriber(input_adapter, "pending_task_history_results", config.RequeueChance.HIGH)
    return DefinitionCompletedSubscriberAdapter(subscriber)

app = config.create_faststream_app()