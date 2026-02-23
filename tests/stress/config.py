from contextlib import asynccontextmanager

from expression import Result

from infrastructure.rabbitmq import config
from shared.customtypes import RunIdValue, TaskIdValue

async def run_stress_test_task(task_id: TaskIdValue, run_id: RunIdValue):
    return Result.Ok(None)

@asynccontextmanager
async def _lifespan():
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

lifespan = _lifespan()