from contextlib import asynccontextmanager

from infrastructure.rabbitmq import config
from shared.customtypes import Metadata, RunIdValue, TaskIdValue
from shared.pipeline.types import RunTaskData

def run_stress_test_task(task_id: TaskIdValue, run_id: RunIdValue):
    metadata = Metadata()
    metadata.set_from("stress test")
    data = RunTaskData(task_id, run_id, metadata)
    return config.run_task(data)

@asynccontextmanager
async def _lifespan():
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

lifespan = _lifespan()