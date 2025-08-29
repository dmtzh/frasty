from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel

from manualrunstate import ManualRunState
from manualrunstore import manual_run_storage
from shared.customtypes import IdValue
import shared.domaindefinition as shdomaindef
from shared.domainrunning import RunningDefinitionState
import shared.dtodefinition as shdtodef
from shared.infrastructure.stepdefinitioncreatorsstore import get_step_definition_name
from shared.infrastructure.storage.repository import StorageError
from shared.runningdefinitionsstore import running_definitions_storage
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import coroutine_result, async_result, async_ex_to_error_result, async_catch_ex
from shared.utils.result import ResultTag
import shared.validation as validation

# ---------------------------
# inputs
# ---------------------------
class ManualRunRequest(BaseModel):
    resource: list[dict[str, Any]]

# ---------------------------
# workflow
# ---------------------------
@dataclass(frozen=True)
class InputValidationError:
    error: list[shdtodef.StepValidationError] | shdomaindef.StepsMissing | shdomaindef.StepNotAllowed
class ManualRunStorageError(StorageError):
    '''Unexpected manual run storage error'''
@dataclass(frozen=True)
class RunningDefinitionsStorageError(StorageError):
    '''Unexpected running definitions storage error'''
@dataclass(frozen=True)
class RunDefinitionError:
    error: Any
type WorkflowError = InputValidationError | ManualRunStorageError | RunningDefinitionsStorageError | RunDefinitionError

# ==================================
# Workflow implementation
# ==================================
@async_result
@make_async
def request_to_definition(raw_definition: list[dict[str, Any]]):
    return shdtodef.DefinitionAdapter.from_list(raw_definition).map_error(InputValidationError)

@async_result
@async_ex_to_error_result(ManualRunStorageError.from_exception)
@manual_run_storage.with_storage
def apply_add_manual_run(state: ManualRunState | None, definition: shdomaindef.Definition):
    if state is not None:
        raise ValueError("manual run state already exists")
    state = ManualRunState.create_running(definition)
    return (None, state)

@async_result
@async_ex_to_error_result(RunningDefinitionsStorageError.from_exception)
@running_definitions_storage.with_storage
def apply_run_first_step(state: RunningDefinitionState | None, definition: shdomaindef.Definition):
    if state is not None:
        raise ValueError("running definition state already exists")
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
    evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    if type(evt) is not RunningDefinitionState.Events.StepRunning:
        raise RuntimeError("expected step running event")
    return (evt, state)

@coroutine_result[WorkflowError]()
async def run_definition_workflow(run_first_step_handler: Callable[[IdValue, IdValue, RunningDefinitionState.Events.StepRunning], Coroutine[Any, Any, Result]], manual_run_id: IdValue, raw_definition: list[dict[str, Any]]):
    definition = await request_to_definition(raw_definition)
    await apply_add_manual_run(manual_run_id, definition)
    definition_id = manual_run_id
    step_running_evt = await apply_run_first_step(manual_run_id, definition_id, definition)
    await async_result(run_first_step_handler)(manual_run_id, definition_id, step_running_evt).map_error(RunDefinitionError)
    return manual_run_id

async def clean_up_failed_manual_run(manual_run_id, error: WorkflowError):
    match error:
        case RunningDefinitionsStorageError():
            await async_catch_ex(manual_run_storage.delete)(manual_run_id)
        case RunDefinitionError():
            await async_catch_ex(running_definitions_storage.delete)(manual_run_id, manual_run_id)
            await async_catch_ex(manual_run_storage.delete)(manual_run_id)

# ==================================
# API endpoint handler
# ==================================
async def handle(run_first_step_handler: Callable[[IdValue, IdValue, RunningDefinitionState.Events.StepRunning], Coroutine[Any, Any, Result]], request: ManualRunRequest):
    manual_run_id = IdValue.new_id()
    res = await run_definition_workflow(run_first_step_handler, manual_run_id, request.resource)
    if res.is_error():
        await clean_up_failed_manual_run(manual_run_id, res.error)
    def step_validation_error_to_string(err: shdtodef.StepValidationError) -> str:
        match err:
            case shdtodef.UnsupportedStep(step):
                return f"step '{step}' is not supported"
            case validation.ValueMissing(name):
                return f"'{name}' is missing"
            case validation.ValueInvalid(name):
                return f"'{name}' value is invalid"
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
                case StorageError(message=error):
                    raise HTTPException(status_code=503, detail=f"Oops... {error}")
                case RunDefinitionError(error=error):
                    raise HTTPException(status_code=503, detail=f"Oops... {error}")