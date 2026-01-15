from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

import runtaskapihandler
from shared.customtypes import TaskIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.task import Task
from shared.tasksstore import legacy_tasks_storage, tasks_storage
from shared.utils.asyncresult import async_ex_to_error_result

import addtaskapihandler
import cleartaskscheduleapihandler
import settaskscheduleapihandler
from config import DefinitionValidationError, add_definition, app, execute_task

@app.post("/tasks/legacy/{id}/schedule", status_code=202)
async def set_legacy_schedule(id: str, request: settaskscheduleapihandler.SetScheduleRequest):
    return await settaskscheduleapihandler.handle(legacy_tasks_storage, id, request)

@app.delete("/tasks/legacy/{id}/schedule/{schedule_id}", status_code=202)
async def clear_legacy_schedule(id: str, schedule_id: str):
    return await cleartaskscheduleapihandler.handle(id, schedule_id)

# ------------------------------------------------------------------------------------------------------------

@app.post("/tasks", status_code=201)
async def add_task(request: addtaskapihandler.AddTaskRequest):
    @async_ex_to_error_result(StorageError.from_exception)
    def add_task(task_id: TaskIdValue, task: Task):
        return tasks_storage.add(task_id, task)
    def err_to_response(error: addtaskapihandler.TaskNameMissing | addtaskapihandler.AddDefinitionError | addtaskapihandler.AddToStorageError):
        match error:
            case addtaskapihandler.TaskNameMissing():
                errors = [{"type": "missing", "loc": ["body", "resource", "name"], "msg": "Value required"}]
                raise RequestValidationError(errors)
            case addtaskapihandler.AddDefinitionError(error=DefinitionValidationError(errors=errors)):
                raise RequestValidationError(errors)
            case addtaskapihandler.AddDefinitionError():
                raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
            case addtaskapihandler.AddToStorageError():
                raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
    
    add_task_res = await addtaskapihandler.add_task_workflow(add_definition, add_task, request.resource)
    return add_task_res.map(lambda id: {"id": id.to_value_with_checksum()}).default_with(err_to_response)

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

@app.post("/tasks/{id}/schedule", status_code=202)
async def set_schedule(id: str, request: settaskscheduleapihandler.SetScheduleRequest):
    return await settaskscheduleapihandler.handle(tasks_storage, id, request)

# ------------------------------------------------------------------------------------------------------------
