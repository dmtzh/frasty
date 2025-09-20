from expression import Result
from fastapi import FastAPI

from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitruntask as rabbit_task
from shared.customtypes import TaskIdValue
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.repository import NotFoundError
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

import addtaskapihandler
import cleartaskscheduleapihandler
import completerunstatehandler
from config import lifespan, rabbit_client
import getrunstateapihandler
import runtaskapihandler
import settaskscheduleapihandler

app = FastAPI(lifespan=lifespan)

@app.post("/tasks")
async def add_task(request: addtaskapihandler.AddTaskRequest):
    return await addtaskapihandler.handle(request)

@app.post("/tasks/{id}/run", status_code=201)
async def run_task(id: str):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_task_handler(cmd: runtaskapihandler.RunTaskCommand):
        return rabbit_task.run(rabbit_client, cmd.task_id, cmd.run_id, "tasks_webapi", {})
    return await runtaskapihandler.handle(rabbit_run_task_handler, id)

@app.get("/tasks/{id}/run/{run_id}")
async def get_run_state(id: str, run_id: str):
    return await getrunstateapihandler.handle(id, run_id)

@app.post("/tasks/{id}/schedule", status_code=201)
async def set_schedule(id: str, request: settaskscheduleapihandler.SetScheduleRequest):
    return await settaskscheduleapihandler.handle(id, request)

@app.delete("/tasks/{id}/schedule/{schedule_id}", status_code=202)
async def clear_schedule(id: str, schedule_id: str):
    return await cleartaskscheduleapihandler.handle(id, schedule_id)

@rabbit_definition_completed.subscriber(rabbit_client, rabbit_definition_completed.DefinitionCompletedData, queue_name=None)
async def complete_web_api_task_run_state_with_result(input):
    def from_definition_completed_data(data: rabbit_definition_completed.DefinitionCompletedData) -> Result[completerunstatehandler.CompleteRunStateCommand, str]:
        raw_from = data.metadata.get("from")
        if raw_from != "tasks_webapi":
            return Result.Error("from is not tasks_webapi")
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
                cmd = completerunstatehandler.CompleteRunStateCommand(task_id, data.run_id, data.result)
                return Result.Ok(cmd)
    
    complete_run_state_cmd_res = input.bind(from_definition_completed_data)
    match complete_run_state_cmd_res:
        case Result(tag=ResultTag.OK, ok=cmd) if type(cmd) is completerunstatehandler.CompleteRunStateCommand:
            res = await completerunstatehandler.handle(cmd)
            match res:
                case Result(tag=ResultTag.ERROR, error=error) if type(error) is not NotFoundError:
                    rabbit_definition_completed.handle_processing_failure(rabbit_definition_completed.Severity.LOW)
            return res