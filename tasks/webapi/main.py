import asyncio
from dataclasses import dataclass
from typing import Any

import aiohttp
from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

import runtaskapihandler
from shared.customtypes import DefinitionIdValue, Error, TaskIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.tasksstore import tasks_storage
from shared.utils.asyncresult import async_ex_to_error_result

import addtaskapihandler
import cleartaskscheduleapihandler
import settaskscheduleapihandler
from config import ADD_DEFINITION_URL, app, execute_task

@app.post("/tasks/legacy/{id}/schedule", status_code=202)
async def set_legacy_schedule(id: str, request: settaskscheduleapihandler.SetScheduleRequest):
    return await settaskscheduleapihandler.handle(id, request)

@app.delete("/tasks/legacy/{id}/schedule/{schedule_id}", status_code=202)
async def clear_legacy_schedule(id: str, schedule_id: str):
    return await cleartaskscheduleapihandler.handle(id, schedule_id)

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

@app.post("/tasks/{id}/run", status_code=202)
async def run(id: str):
    # def to_execute_definition_input(task_id: TaskIdValue):
    #     definition_input_data = {"task_id": task_id.to_value_with_checksum()}
    #     execute_task_action = Action(ActionName("execute_task"), ActionType.SERVICE)
    #     definition_steps = (
    #         ActionDefinition(execute_task_action.name, execute_task_action.type, None),
    #         ActionDefinition(PRINT_RESULT_ACTION.name, PRINT_RESULT_ACTION.type, None),
    #     )
    #     definition = Definition(definition_input_data, definition_steps)
    #     input = ExecuteDefinitionInput(DefinitionIdValue.new_id(), definition)
    #     return input
    # async def run_task_handler(task_id: TaskIdValue):
    #     execute_def_input = to_execute_definition_input(task_id)
    #     execute_task_res = await execute_definition(execute_def_input)
    #     return execute_task_res.map(lambda action_data: action_data.run_id)
    async def run_task_handler(task_id: TaskIdValue):
        execute_task_res = await execute_task(task_id)
        return execute_task_res.map(lambda action_data: action_data.run_id)
    return await runtaskapihandler.handle(run_task_handler, id)

# ------------------------------------------------------------------------------------------------------------
