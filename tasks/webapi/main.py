import asyncio
from dataclasses import dataclass
from typing import Any

import aiohttp
from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Error
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.tasksstore import tasks_storage
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

import addlegacytaskapihandler
import addtaskapihandler
import cleartaskscheduleapihandler
import getrunstateapihandler
import runtaskapihandler
import settaskscheduleapihandler
from webapitaskrunstate import WebApiTaskRunState
from webapitaskrunstore import web_api_task_run_storage
from config import ADD_DEFINITION_URL, CompletedTaskData, app, run_webapi_task, webapi_task_completed_subscriber

@app.post("/tasks/legacy")
async def add_legacy_task(request: addlegacytaskapihandler.AddTaskRequest):
    return await addlegacytaskapihandler.handle(request)

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

# ------------------------------------------------------------------------------------------------------------

@app.post("/tasks")
async def add_task(request: addtaskapihandler.AddTaskRequest):
    def err_to_response(error: addtaskapihandler.TaskNameMissing | DefinitionValidationError | AddDefinitionError | StorageError):
        match error:
            case addtaskapihandler.TaskNameMissing():
                errors = [{"type": "missing", "loc": ["body", "resource", "name"], "msg": "Value required"}]
                raise RequestValidationError(errors)
            case DefinitionValidationError(errors=errors):
                raise RequestValidationError(errors)
            case AddDefinitionError():
                raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
            case StorageError():
                raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
    add_task_res = await addtaskapihandler.add_task_workflow(add_definition_handler, tasks_storage.add, request.resource)
    return add_task_res.map(lambda id: {"id": id.to_value_with_checksum()}).default_with(err_to_response)

class AddDefinitionError(Error):
    '''Add definition error'''
@dataclass(frozen=True)
class DefinitionValidationError:
    errors: list
@async_ex_to_error_result(AddDefinitionError.from_exception)
async def add_definition_handler(raw_definition: list[dict[str, Any]]) -> Result[addtaskapihandler.DefinitionIdValue, DefinitionValidationError | AddDefinitionError]:
    def definition_validation_error_response_to_errors(json_response: dict):
        def update_location(error: dict):
            error["loc"].append("definition")
            return error
        detail = json_response["detail"]
        errors = list(map(update_location, detail))
        return errors
    
    add_definition_url = ADD_DEFINITION_URL
    timeout_15_seconds = aiohttp.ClientTimeout(total=15)
    json_data = {"resource": raw_definition}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(add_definition_url, json=json_data, timeout=timeout_15_seconds) as response:
                match response.status:
                    case 200:
                        json_response = await response.json()
                        definition_id_with_checksum = json_response.get("id")
                        opt_definition_id = DefinitionIdValue.from_value_with_checksum(definition_id_with_checksum)
                        match opt_definition_id:
                            case None:
                                return Result.Error(AddDefinitionError(f"Unexpected response when add definition: {json_response}"))
                            case definition_id:
                                return Result.Ok(definition_id)
                    case 422:
                        json_response = await response.json()
                        errors = definition_validation_error_response_to_errors(json_response)
                        return Result.Error(DefinitionValidationError(errors))
                    case error_status:
                        str_response = await response.text()
                        return Result.Error(AddDefinitionError(f"{error_status}, {str_response}"))
        except asyncio.TimeoutError:
            return Result.Error(AddDefinitionError(f"Request timeout {timeout_15_seconds.total} seconds when connect to {add_definition_url}"))
        except aiohttp.client_exceptions.ClientConnectorError:
            return Result.Error(AddDefinitionError(f"Cannot connect to {add_definition_url})"))

# ------------------------------------------------------------------------------------------------------------