from dataclasses import dataclass
from expression import Result
from fastapi import FastAPI
from fastapi.responses import FileResponse

from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from shared.completedresult import CompletedResult
from shared.customtypes import RunIdValue, TaskIdValue, Error
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException
from shared.utils.asyncresult import AsyncResult, async_ex_to_error_result, async_result, coroutine_result
from shared.utils.result import ResultTag

import addtaskapihandler
from config import lifespan, rabbit_client
import getrunstateapihandler
import runtaskapihandler
from webapitaskrunstate import WebApiTaskRunState
from webapitaskrunstore import web_api_task_run_storage

app = FastAPI(lifespan=lifespan)

@app.post("/tasks")
async def add_task(request: addtaskapihandler.AddTaskRequest):
    return await addtaskapihandler.handle(request)

@app.post("/tasks/{id}/run", status_code=201)
async def run_task(id: str):
    return await runtaskapihandler.handle(id)

@app.get("/tasks/{id}/run/{run_id}")
async def get_run_state(id: str, run_id: str):
    return await getrunstateapihandler.handle(id, run_id)

@app.get("/tickets")
def tickets():
    return FileResponse("../../../html_sources/get_ticket.html")

class UnexpectedError(Error):
    '''Unexpected error'''

@dataclass(frozen=True)
class WebApiTaskRunCompletedData:
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedResult

    @staticmethod
    def from_definition_completed_data(data: rabbit_definition_completed.DefinitionCompletedData) -> Result['WebApiTaskRunCompletedData', str]:
        raw_from = data.metadata.get("from")
        if raw_from != "tasks_webapi":
            return Result.Error("from is not webapi")
        raw_task_id = data.metadata.get("task_id")
        if raw_task_id is None:
            return Result.Error("task_id not found in metadata")
        if not isinstance(raw_task_id, str):
            return Result.Error("task_id is not a string")
        opt_task_id = TaskIdValue.from_value_with_checksum(raw_task_id)
        match opt_task_id:
            case None:
                return Result.Error("task_id is invalid")
            case task_id:
                res = WebApiTaskRunCompletedData(task_id, data.run_id, data.result)
                return Result.Ok(res)

@rabbit_definition_completed.subscriber(rabbit_client, rabbit_definition_completed.DefinitionCompletedData, queue_name=None)
async def complete_web_api_task_run_state_with_result(input):
    @async_result
    @async_ex_to_error_result(UnexpectedError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    @web_api_task_run_storage.with_storage
    def apply_complete_with_result(state: WebApiTaskRunState | None, result: CompletedResult):
        if state is None:
            raise NotFoundException()
        new_state = state.complete(result)
        return (None, new_state)

    @coroutine_result()
    async def complete_with_result_workflow(data: rabbit_definition_completed.DefinitionCompletedData):
        webapi_task_run_completed_data = await AsyncResult.from_result(WebApiTaskRunCompletedData.from_definition_completed_data(data)).map_error(NotFoundError)
        await apply_complete_with_result(webapi_task_run_completed_data.task_id, webapi_task_run_completed_data.run_id, webapi_task_run_completed_data.result)
        return webapi_task_run_completed_data
    
    match input:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is rabbit_definition_completed.DefinitionCompletedData:
            res = await complete_with_result_workflow(data)
            match res:
                case Result(tag=ResultTag.ERROR, error=UnexpectedError()):
                    rabbit_definition_completed.handle_processing_failure(rabbit_definition_completed.Severity.LOW)
                case Result(tag=ResultTag.ERROR, error=NotFoundError(message=message)):
                    res = message
            return res