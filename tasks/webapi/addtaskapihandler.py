import asyncio
from dataclasses import dataclass

import aiohttp
from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError

from shared.customtypes import IdValue
from shared.infrastructure.storage.repository import StorageError
from shared.types import TaskIdValue
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

from addtaskapiworkflow import AddDefinitionError, AddTaskResource, DefinitionValidationError, add_task_workflow, InputValidationError, TaskNameMissing
import config

# ---------------------------
# inputs
# ---------------------------
@dataclass(frozen=True)
class AddTaskRequest():
    resource: AddTaskResource

# ==================================
# API endpoint handler
# ==================================
def definition_validation_error_response_to_errors(json_response: dict):
    def update_location(error: dict):
        error["loc"].append("definition")
        return error
    detail = json_response["detail"]
    errors = list(map(update_location, detail))
    return errors
@async_ex_to_error_result(AddDefinitionError.from_exception)
async def add_definition_handler(raw_definition: list[dict[str, str]]) -> Result[IdValue, DefinitionValidationError | AddDefinitionError]:
    add_definition_url = config.ADD_DEFINITION_URL
    timeout_15_seconds = aiohttp.ClientTimeout(total=15)
    json_data = {"resource": raw_definition}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(add_definition_url, json=json_data, timeout=timeout_15_seconds) as response:
                match response.status:
                    case 200:
                        json_response = await response.json()
                        definition_id_with_checksum = json_response.get("id")
                        opt_definition_id = IdValue.from_value_with_checksum(definition_id_with_checksum)
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

async def handle(request: AddTaskRequest):
    res = await add_task_workflow(add_definition_handler, request.resource)
    match res:
        case Result(tag=ResultTag.OK, ok=id) if type(id) is TaskIdValue:
            id_with_checksum = id.to_value_with_checksum()
            return {"id": id_with_checksum}
        case Result(tag=ResultTag.ERROR, error=error):
            match error:
                case InputValidationError(error=TaskNameMissing()):
                    errors = [{"type": "missing", "loc": ["body", "resource", "name"], "msg": "Value required", "input": request.resource.name}]
                    raise RequestValidationError(errors)
                case DefinitionValidationError(errors=errors):
                    raise RequestValidationError(errors)
                case AddDefinitionError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
                case StorageError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")