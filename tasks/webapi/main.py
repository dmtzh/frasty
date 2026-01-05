import asyncio
from dataclasses import dataclass
import functools
from typing import Any

import aiohttp
from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

from shared.action import ActionName, ActionType
from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, Error, Metadata, RunIdValue, StepIdValue, TaskIdValue
from shared.definition import ActionDefinition, Definition
from shared.executedefinitionaction import EXECUTE_DEFINITION_ACTION, ExecuteDefinitionInput, run_execute_definition_action
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.pipeline.actionhandler import ActionData
from shared.task import Task
from shared.tasksstore import tasks_storage
from shared.utils.asyncresult import async_catch_ex, async_ex_to_error_result
from shared.utils.result import lift_param

import addtaskapihandler
import cleartaskscheduleapihandler
import getrunstateapihandler
import settaskscheduleapihandler
from config import ADD_DEFINITION_URL, run_action, ExecuteTaskInput, app, execute_task_handler, run_execute_task_action

@app.get("/tasks/legacy/{id}/run/{run_id}")
async def get_legacy_run_state(id: str, run_id: str):
    return await getrunstateapihandler.handle(id, run_id)

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
    def to_execute_task_data(task_id: TaskIdValue, run_id: RunIdValue, task: Task):
        step_id = StepIdValue.new_id()
        input = ExecuteTaskInput(task_id, task.definition_id)
        metadata = Metadata()
        metadata.set_from("run task webapi")
        return ActionData(run_id, step_id, None, input, metadata)
    # def to_execute_definition_data(execute_task_data: ActionData[None, ExecuteTaskInput]):
    #     definition_input_data = execute_task_data.input.to_dto()
    #     definition_steps = (
    #         ActionDefinition(EXECUTE_TASK_ACTION.name, EXECUTE_TASK_ACTION.type, None),
    #         ActionDefinition(PRINT_RESULT_ACTION.name, PRINT_RESULT_ACTION.type, None),
    #     )
    #     definition = Definition(definition_input_data, definition_steps)
    #     input = ExecuteDefinitionInput(DefinitionIdValue.new_id(), definition)
    #     return ActionData(execute_task_data.run_id, execute_task_data.step_id, None, input, execute_task_data.metadata)
    def err_to_response(error):
        match error:
            case NotFoundError():
                raise HTTPException(status_code=404)
            case _:
                raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
    
    opt_task_id = TaskIdValue.from_value_with_checksum(id)
    if opt_task_id is None:
        raise HTTPException(status_code=404)
    opt_task_res = await async_catch_ex(tasks_storage.get)(opt_task_id)
    task_res = opt_task_res.bind(lambda opt_task: Result.Ok(opt_task) if opt_task is not None else Result.Error(NotFoundError("Task not found")))
    run_id = RunIdValue.new_id()
    execute_task_data_res = task_res.map(functools.partial(to_execute_task_data, opt_task_id, run_id))
    execute_task_res = await lift_param(run_execute_task_action)(execute_task_data_res)
    return execute_task_res.map(lambda data: {"id": data.run_id.to_value_with_checksum()}).default_with(err_to_response)
    # execute_def_data_res = execute_task_data_res.map(to_execute_definition_data)
    # execute_task_res = await lift_param(functools.partial(run_execute_definition_action, run_action))(execute_def_data_res)
    # return execute_task_res.map(lambda _: {"id": run_id.to_value_with_checksum()}).default_with(err_to_response)

# ------------------------------------------------------------------------------------------------------------

@execute_task_handler
async def handle_execute_task_action(data: ActionData[None, ExecuteTaskInput]):
    definition_input_data = {"definition_id": data.input.definition_id.to_value_with_checksum()}
    execution_id = DefinitionIdValue.new_id()
    add_task_result_to_history_config = {
        "task_id": data.input.task_id.to_value_with_checksum(),
        "execution_id": execution_id.to_value_with_checksum()
    }
    definition_steps = (
        ActionDefinition(ActionName("get_definition"), ActionType.SERVICE, None),
        ActionDefinition(EXECUTE_DEFINITION_ACTION.name, EXECUTE_DEFINITION_ACTION.type, None),
        ActionDefinition(ActionName("add_task_result_to_history"), ActionType.SERVICE, add_task_result_to_history_config),
    )
    definition = Definition(definition_input_data, definition_steps)
    input = ExecuteDefinitionInput(execution_id, definition)
    execute_definition_data = ActionData(data.run_id, data.step_id, None, input, data.metadata)
    execute_task_res = await run_execute_definition_action(run_action, execute_definition_data)
    return execute_task_res\
        .map(lambda _: None)\
        .map_error(str).map_error(CompletedWith.Error)\
        .merge()

# ------------------------------------------------------------------------------------------------------------
