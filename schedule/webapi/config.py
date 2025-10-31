from collections.abc import Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.commands import Command, CommandAdapter
from shared.customtypes import ScheduleIdValue, TaskIdValue

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def change_task_schedule(task_id: TaskIdValue, schedule_id: ScheduleIdValue, command: Command) -> Coroutine[Any, Any, Result[None, Any]]:
    command_dto = CommandAdapter.to_dict(command)
    return config.change_task_schedule(task_id, schedule_id, command_dto)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)