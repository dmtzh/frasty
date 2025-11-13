from collections.abc import Callable
from contextlib import asynccontextmanager

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, TaskIdValue
from shared.definitioncompleteddata import DefinitionCompletedData
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, map_handler
from shared.pipeline.types import RunTaskData
from shared.utils.parse import parse_from_dict

def run_stress_test_task(task_id: TaskIdValue, run_id: RunIdValue):
    data = RunTaskData(task_id, run_id, Metadata())
    return config.run_task(data, "stress test")

def stress_test_definition_completed_subscriber[T](input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], T]):
    subscriber = config.definition_completed_subscriber(DefinitionCompletedData, None, config.RequeueChance.LOW)
    def from_definition_completed_data(data: DefinitionCompletedData):
        from_stress_test_res = parse_from_dict(data.metadata, "from", lambda s: True if s == "stress test" else None)
        return from_stress_test_res.map(lambda _: input_adapter(data.run_id, data.definition_id, data.result, data.metadata))
    stress_test_subscriber = map_handler(subscriber, lambda data_res: data_res.bind(from_definition_completed_data))
    return DefinitionCompletedSubscriberAdapter(stress_test_subscriber)

@asynccontextmanager
async def _lifespan():
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

lifespan = _lifespan()