from expression import Result

from shared.completedresult import CompletedResult
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

import addtaskapihandler
import cleartaskscheduleapihandler
import getrunstateapihandler
import runtaskapihandler
import settaskscheduleapihandler
from webapitaskrunstate import WebApiTaskRunState
from webapitaskrunstore import web_api_task_run_storage
from config import CompletedTaskData, app, run_webapi_task, webapi_task_completed_subscriber

@app.post("/tasks/legacy")
async def add_legacy_task(request: addtaskapihandler.AddTaskRequest):
    return await addtaskapihandler.handle(request)

@app.post("/tasks/legacy/{id}/run", status_code=202)
async def run_legacy(id: str):
    return await runtaskapihandler.handle(lambda cmd: run_webapi_task(cmd.task_id, cmd.run_id), id)

@app.get("/tasks/legacy/{id}/run/{run_id}")
async def get_legacy_run_state(id: str, run_id: str):
    return await getrunstateapihandler.handle(id, run_id)

@app.post("/tasks/legacy/{id}/schedule", status_code=202)
async def set_legacy_schedule(id: str, request: settaskscheduleapihandler.SetScheduleRequest):
    return await settaskscheduleapihandler.handle(id, request)

@app.delete("/tasks/legacy/{id}/schedule/{schedule_id}", status_code=202)
async def clear_legacy_schedule(id: str, schedule_id: str):
    return await cleartaskscheduleapihandler.handle(id, schedule_id)

@webapi_task_completed_subscriber
async def complete_run_state_with_result(data: CompletedTaskData):
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    @web_api_task_run_storage.with_storage
    def apply_complete_run_state(state: WebApiTaskRunState | None, result: CompletedResult):
        if state is None:
            raise NotFoundException()
        new_state = state.complete(result)
        return (new_state, new_state)
    
    complete_run_state_res = await apply_complete_run_state(data.task_id, data.run_id, data.result)
    match complete_run_state_res:
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            return None
        case _:
            return complete_run_state_res
