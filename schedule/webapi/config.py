from collections.abc import Coroutine
from contextlib import asynccontextmanager
import os
from typing import Any

from expression import Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.changetaskcheduleaction import CHANGE_TASK_SCHEDULE_ACTION
from shared.commands import Command, CommandAdapter
from shared.customtypes import Metadata, RunIdValue, StepIdValue
from shared.pipeline.actionhandler import ActionData, run_action_adapter

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
CHANGE_TASK_SCHEDULE_RUN_ID = RunIdValue("0" * RunIdValue._length)

def change_task_schedule(command: Command):
    command_dto = CommandAdapter.to_dict(command)
    change_task_schedule_action_dto = ActionData(CHANGE_TASK_SCHEDULE_RUN_ID, StepIdValue.new_id(), None, command_dto, Metadata())
    return run_action_adapter(config.run_action)(CHANGE_TASK_SCHEDULE_ACTION, change_task_schedule_action_dto)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)