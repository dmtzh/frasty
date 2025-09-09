from dataclasses import dataclass
from typing import Any

from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel

from shared.definitionsstore import definitions_storage
from shared.customtypes import IdValue
import shared.domaindefinition as shdomaindef
import shared.dtodefinition as shdtodef
from shared.infrastructure.stepdefinitioncreatorsstore import get_step_definition_name
from shared.infrastructure.storage.repository import StorageError
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import coroutine_result, async_result, async_ex_to_error_result
from shared.utils.result import ResultTag
import shared.validation as validation

# ---------------------------
# inputs
# ---------------------------
class AddDefinitionRequest(BaseModel):
    resource: list[dict[str, Any]]

# ==================================
# Workflow implementation
# ==================================
@dataclass(frozen=True)
class InputValidationError:
    error: list[shdtodef.StepValidationError] | shdomaindef.StepsMissing | shdomaindef.StepNotAllowed
type WorkflowError = InputValidationError | StorageError

@async_result
@make_async
def request_to_definition(raw_definition: list[dict[str, Any]]):
    return shdtodef.DefinitionAdapter.from_list(raw_definition).map_error(InputValidationError)

@async_result
@async_ex_to_error_result(StorageError.from_exception)
def apply_add_definition(id: IdValue, definition: shdomaindef.Definition):
    return definitions_storage.add(id, definition)

@coroutine_result[WorkflowError]()
async def add_definition_workflow(raw_definition: list[dict[str, Any]]):
    definition = await request_to_definition(raw_definition)
    id = IdValue.new_id()
    await apply_add_definition(id, definition)
    return id

# ==================================
# API endpoint handler
# ==================================
async def handle(request: AddDefinitionRequest):
    def step_validation_error_to_string(err: shdtodef.StepValidationError) -> str:
        match err:
            case shdtodef.UnsupportedStep(step):
                return f"step '{step}' is not supported"
            case validation.ValueMissing(name):
                return f"'{name}' is missing"
            case validation.ValueInvalid(name):
                return f"'{name}' value is invalid"
    res = await add_definition_workflow(request.resource)
    match res:
        case Result(tag=ResultTag.OK, ok=id) if type(id) is IdValue:
            id_with_checksum = id.to_value_with_checksum()
            return {"id": id_with_checksum}
        case Result(tag=ResultTag.ERROR, error=error):
            match error:
                case InputValidationError(error=[*step_validation_errors]):
                    errors = [{"loc": ["body", "resource"], "type": "value_error", "msg": step_validation_error_to_string(err)} for err in step_validation_errors]
                    raise RequestValidationError(errors)
                case InputValidationError(error=shdomaindef.StepsMissing()):
                    errors = [{"loc": ["body", "resource"], "type": "missing", "msg": "steps missing"}]
                    raise RequestValidationError(errors)
                case InputValidationError(shdomaindef.StepNotAllowed(value=step)):
                    step_name = get_step_definition_name(type(step))
                    errors = [{"loc": ["body", "resource", "step"], "type": "value_error", "msg": f"step '{step_name}' not alllowed"}]
                    raise RequestValidationError(errors)
                case StorageError():
                    raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")