from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Any

from expression import Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import Metadata, RunIdValue, TaskIdValue
from shared.pipeline.handlers import DefinitionCompletedSubscriberAdapter, map_handler, only_from, with_input_output_logging_subscriber
from shared.pipeline.types import CompletedDefinitionData, RunTaskData
from shared.utils.parse import parse_value

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
ADD_DEFINITION_URL = os.environ['ADD_DEFINITION_URL']
CHANGE_SCHEDULE_URL = os.environ['CHANGE_SCHEDULE_URL']

def run_webapi_task(task_id: TaskIdValue, run_id: RunIdValue):
    metadata = Metadata()
    metadata.set_from("run task webapi")
    data = RunTaskData(task_id, run_id, metadata)
    return config.run_task(data)

@dataclass(frozen=True)
class CompletedTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedResult

def webapi_task_completed_subscriber(func: Callable[[CompletedTaskData], Coroutine[Any, Any, Result | None]]):
    def to_completed_task_data(data: CompletedDefinitionData):
        task_id_res = parse_value(data.metadata, "task_id", Metadata.get_task_id)
        return task_id_res.map(lambda task_id: CompletedTaskData(task_id, data.run_id, data.result))
    subscriber = config.definition_completed_subscriber(None, config.RequeueChance.LOW)
    run_task_webapi_subscriber = only_from(subscriber, "run task webapi")
    with_logging_subscriber = with_input_output_logging_subscriber(run_task_webapi_subscriber, "webapi_task_completed")
    webapi_task_completed_s = map_handler(
        with_logging_subscriber,
        lambda data_res: data_res.bind(to_completed_task_data)
    )
    return DefinitionCompletedSubscriberAdapter(webapi_task_completed_s)(func)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)
