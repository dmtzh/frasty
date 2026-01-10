import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Any

import aiohttp
from expression import Result
from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.customtypes import DefinitionIdValue, Error, Metadata, RunIdValue, StepIdValue, TaskIdValue
from shared.pipeline.actionhandler import ActionData, run_action_adapter
from shared.utils.asyncresult import async_ex_to_error_result

# async def execute_definition(input: ExecuteDefinitionInput):
#     run_id = RunIdValue.new_id()
#     step_id = StepIdValue.new_id()
#     metadata = Metadata()
#     metadata.set_from("run task webapi")
#     data = ActionData(run_id, step_id, None, input, metadata)
#     execute_definition_res = await run_execute_definition_action(config.run_action, data)
#     return execute_definition_res.map(lambda _: data)

async def execute_task(task_id: TaskIdValue):
    execute_task_action = Action(ActionName("execute_task"), ActionType.SERVICE)
    run_id = RunIdValue.new_id()
    step_id = StepIdValue.new_id()
    execute_task_input_dto = {"task_id": task_id.to_value_with_checksum()}
    metadata = Metadata()
    metadata.set_from("run task webapi")
    data = ActionData(run_id, step_id, None, execute_task_input_dto, metadata)
    run_action_res = await run_action_adapter(config.run_action)(execute_task_action, data)
    return run_action_res.map(lambda _: data)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
ADD_DEFINITION_URL = os.environ['ADD_DEFINITION_URL']
CHANGE_SCHEDULE_URL = os.environ['CHANGE_SCHEDULE_URL']

class AddDefinitionError(Error):
    '''Add definition error'''
@dataclass(frozen=True)
class DefinitionValidationError:
    errors: list
@async_ex_to_error_result(AddDefinitionError.from_exception)
async def add_definition(raw_definition: list[dict[str, Any]]) -> Result[DefinitionIdValue, DefinitionValidationError | AddDefinitionError]:
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)
