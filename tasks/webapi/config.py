from collections.abc import Callable
from contextlib import asynccontextmanager
import os

from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
ADD_DEFINITION_URL = os.environ['ADD_DEFINITION_URL']
CHANGE_SCHEDULE_URL = os.environ['CHANGE_SCHEDULE_URL']

run_task = config.run_task

def definition_completed_subscriber[T](input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], T]):
    subscriber = config.definition_completed_subscriber(input_adapter, None, config.RequeueChance.LOW)
    return DefinitionCompletedSubscriberAdapter(subscriber)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)
