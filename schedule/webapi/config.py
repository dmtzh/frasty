from collections.abc import Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.changetaskscheduledefinition import ChangeTaskSchedule
from shared.commands import Command, CommandAdapter
from shared.customtypes import Metadata, RunIdValue, StepIdValue
from shared.pipeline.types import StepData

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
CHANGE_TASK_SCHEDULE_RUN_ID = RunIdValue("0" * RunIdValue._length)

def change_task_schedule(command: Command) -> Coroutine[Any, Any, Result[None, Any]]:
    command_dto = CommandAdapter.to_dict(command)
    data = StepData(CHANGE_TASK_SCHEDULE_RUN_ID, StepIdValue.new_id(), ChangeTaskSchedule(), command_dto, Metadata())
    return config.run_step(data)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)