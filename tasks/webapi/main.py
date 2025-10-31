from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import Result, effect

from shared.customtypes import TaskIdValue
from shared.definitioncompleteddata import DefinitionCompletedData
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import async_result, coroutine_result
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag

import addtaskapihandler
import cleartaskscheduleapihandler
import completerunstatehandler
import getrunstateapihandler
import runtaskapihandler
import settaskscheduleapihandler
from config import app, definition_completed_subscriber, run_task

@app.post("/tasks")
async def add_task(request: addtaskapihandler.AddTaskRequest):
    return await addtaskapihandler.handle(request)

@app.post("/tasks/{id}/run", status_code=202)
async def run(id: str):
    def run_task_handler(cmd: runtaskapihandler.RunTaskCommand):
        return run_task(cmd.task_id, cmd.run_id, "tasks_webapi", {})
    return await runtaskapihandler.handle(run_task_handler, id)

@app.get("/tasks/{id}/run/{run_id}")
async def get_run_state(id: str, run_id: str):
    return await getrunstateapihandler.handle(id, run_id)

@app.post("/tasks/{id}/schedule", status_code=202)
async def set_schedule(id: str, request: settaskscheduleapihandler.SetScheduleRequest):
    return await settaskscheduleapihandler.handle(id, request)

@app.delete("/tasks/{id}/schedule/{schedule_id}", status_code=202)
async def clear_schedule(id: str, schedule_id: str):
    return await cleartaskscheduleapihandler.handle(id, schedule_id)

@dataclass(frozen=True)
class DataValidationError:
    '''Data validation error'''
    error: str

@definition_completed_subscriber(DefinitionCompletedData)
async def complete_web_api_task_run_state_with_result(data: DefinitionCompletedData):
    @async_result
    @make_async
    @effect.result[completerunstatehandler.CompleteRunStateCommand, str]()
    def to_complete_run_state_cmd() -> Generator[Any, Any, completerunstatehandler.CompleteRunStateCommand]:
        yield from parse_from_dict(data.metadata, "from", lambda s: True if s == "tasks_webapi" else None)
        task_id = yield from parse_from_dict(data.metadata, "task_id", TaskIdValue.from_value_with_checksum)
        return completerunstatehandler.CompleteRunStateCommand(task_id, data.run_id, data.result)
    @coroutine_result[DataValidationError | NotFoundError | StorageError]()
    async def complete_run_state():
        cmd = await to_complete_run_state_cmd().map_error(DataValidationError)
        state = await async_result(completerunstatehandler.handle)(cmd)
        return state

    complete_run_state_res = await complete_run_state()
    match complete_run_state_res:
        case Result(tag=ResultTag.ERROR, error=DataValidationError()):
            return None
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            return None
        case _:
            return complete_run_state_res
