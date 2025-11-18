from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any, Coroutine

from expression import Result

from infrastructure.rabbitmq import config
from shared.customtypes import Metadata, RunIdValue, TaskIdValue
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, only_from
from shared.pipeline.types import CompletedDefinitionData, RunTaskData

def run_stress_test_task(task_id: TaskIdValue, run_id: RunIdValue):
    data = RunTaskData(task_id, run_id, Metadata())
    return config.run_task(data, "stress test")

def stress_test_definition_completed_subscriber(func: Callable[[CompletedDefinitionData], Coroutine[Any, Any, Result | None]]):
    subscriber = config.definition_completed_subscriber(None, config.RequeueChance.LOW)
    stress_test_subscriber = only_from(subscriber, "stress test")
    return DefinitionCompletedSubscriberAdapter(stress_test_subscriber)(func)

@asynccontextmanager
async def _lifespan():
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

lifespan = _lifespan()