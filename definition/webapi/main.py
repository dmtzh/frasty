from dataclasses import dataclass
import functools
from typing import Any

from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse
from pydantic import BaseModel

from shared.completedresult import CompletedResult, CompletedResultAdapter, CompletedWith
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, StepIdValue
from shared.definition import ActionDefinition, Definition, DefinitionAdapter, StepsMissing
from shared.definitionsstore import definitions_storage
from shared.executedefinitionaction import EXECUTE_DEFINITION_ACTION, ExecuteDefinitionInput, run_execute_definition_action
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.pipeline.actionhandler import ActionData
from shared.utils.asyncresult import async_catch_ex, async_ex_to_error_result
from shared.utils.result import ResultTag, lift_param
from shared.validation import ValueInvalid, ValueMissing, ValueError as ValueErr

from config import COMPLETE_MANUAL_RUN_ACTION, ManualRunInput, app, complete_manual_run_handler, manual_run_handler, run_action, run_manual_run_action
from manualrunstate import ManualRunStateAdapter, ManualRunState
from manualrunstore import manual_run_storage

@app.get("/tickets")
def tickets():
    return FileResponse("./html_sources/get_ticket.html")

# ------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class InputValidationError:
    error: StepsMissing | list[ValueErr]
class AddDefinitionRequest(BaseModel):
    resource: list[dict[str, Any]]
@app.post("/definitions")
async def add_definition(request: AddDefinitionRequest):
    def err_to_http(error: StorageError | InputValidationError):
        match error:
            case InputValidationError(error=StepsMissing()):
                errors = [{"loc": ["body", "resource"], "type": "missing", "msg": "steps missing"}]
                raise RequestValidationError(errors)
            case InputValidationError(error=[*step_validation_errors]):
                errors = [{"loc": ["body", "resource"], "type": "value_error", "msg": step_validation_error_to_string(err)} for err in step_validation_errors]
                raise RequestValidationError(errors)
            case other_error:
                raise HTTPException(status_code=503, detail=f"Oops... {other_error}")
    def step_validation_error_to_string(err: ValueErr) -> str:
        match err:
            case ValueMissing(name):
                return f"'{name}' is missing"
            case ValueInvalid(name):
                return f"'{name}' value is invalid"
            
    raw_definition = request.resource
    definition_res = DefinitionAdapter.from_list(raw_definition).map_error(InputValidationError)
    id = DefinitionIdValue.new_id()
    apply_add_definition = functools.partial(async_ex_to_error_result(StorageError.from_exception)(definitions_storage.add), id)
    add_definition_res = await lift_param(apply_add_definition)(definition_res)
    return add_definition_res.map(lambda _: {"id": id.to_value_with_checksum()}).default_with(err_to_http)

# ------------------------------------------------------------------------------------------------------------

@app.get("/definitions/{id}")
async def get_definition(id: str):
    def opt_definition_to_response(opt_definition_with_ver: tuple[Definition, int] | None):
        match opt_definition_with_ver:
            case None:
                raise HTTPException(status_code=404)
            case (definition, _):
                definition_dto = DefinitionAdapter.to_list(definition)
                return definition_dto
    def err_to_response(err):
        raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
    
    opt_def_id = DefinitionIdValue.from_value_with_checksum(id)
    if opt_def_id is None:
        raise HTTPException(status_code=404)
    opt_definition_with_ver_res = await async_catch_ex(definitions_storage.get_with_ver)(opt_def_id)
    return opt_definition_with_ver_res.map(opt_definition_to_response).default_with(err_to_response)

# ------------------------------------------------------------------------------------------------------------

class ManualRunRequest(BaseModel):
    resource: list[dict[str, Any]]
@app.post("/definition/manual-run", status_code=202)
async def manual_run(request: ManualRunRequest):
    def to_manual_run_data(definition: Definition):
        run_id = RunIdValue.new_id()
        step_id = StepIdValue.new_id()
        input = ManualRunInput(definition)
        metadata = Metadata()
        metadata.set_from("manual run webapi")
        return ActionData(run_id, step_id, None, input, metadata)
    def err_to_http(error):
        match error:
            case InputValidationError(error=StepsMissing()):
                errors = [{"loc": ["body", "resource"], "type": "missing", "msg": "steps missing"}]
                raise RequestValidationError(errors)
            case InputValidationError(error=[*step_validation_errors]):
                errors = [{"loc": ["body", "resource"], "type": "value_error", "msg": step_validation_error_to_string(err)} for err in step_validation_errors]
                raise RequestValidationError(errors)
            case other_error:
                raise HTTPException(status_code=503, detail=f"Oops... {other_error}")
    def step_validation_error_to_string(err: ValueErr) -> str:
        match err:
            case ValueMissing(name):
                return f"'{name}' is missing"
            case ValueInvalid(name):
                return f"'{name}' value is invalid"
    
    raw_definition = request.resource
    definition_res = DefinitionAdapter.from_list(raw_definition)
    manual_run_data_res = definition_res.map(to_manual_run_data).map_error(InputValidationError)
    manual_run_res = await lift_param(run_manual_run_action)(manual_run_data_res)
    return manual_run_res.map(lambda data: {"id": data.run_id.to_value_with_checksum()}).default_with(err_to_http)

# ------------------------------------------------------------------------------------------------------------

@app.get("/definition/manual-run/{id}")
async def get_status(id: str):
    opt_run_id = RunIdValue.from_value_with_checksum(id)
    if opt_run_id is None:
        raise HTTPException(status_code=404)
    opt_state_res = await async_catch_ex(manual_run_storage.get)(opt_run_id)
    match opt_state_res:
        case Result(ResultTag.OK, ok=None):
            raise HTTPException(status_code=404)
        case Result(ResultTag.OK, ok=state):
            state_dto = ManualRunStateAdapter.to_dict(state)
            return state_dto
        case _:
            raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")

# ------------------------------------------------------------------------------------------------------------

@manual_run_handler
async def handle_manual_run_action(data: ActionData[None, ManualRunInput]):
    @async_ex_to_error_result(StorageError.from_exception)
    @manual_run_storage.with_storage
    def apply_add_manual_run(state: ManualRunState | None, manual_run_definition_id: DefinitionIdValue, definition: Definition):
        if state is not None:
            raise ValueError("manual run state already exists")
        state = ManualRunState.create_running(manual_run_definition_id, definition)
        return (None, state)
    def to_execute_definition_data(manual_run_definition_id: DefinitionIdValue):
        definition_input_data = ExecuteDefinitionInput(DefinitionIdValue.new_id(), data.input.definition).to_dict()
        definition_steps = (
            ActionDefinition(EXECUTE_DEFINITION_ACTION.name, EXECUTE_DEFINITION_ACTION.type, None),
            ActionDefinition(COMPLETE_MANUAL_RUN_ACTION.name, COMPLETE_MANUAL_RUN_ACTION.type, None))
        definition = Definition(definition_input_data, definition_steps)
        input = ExecuteDefinitionInput(manual_run_definition_id, definition)
        run_id = data.run_id
        step_id = StepIdValue.new_id()
        metadata = Metadata()
        metadata.set_from("manual run action")
        return ActionData(run_id, step_id, None, input, metadata)
    
    manual_run_definition_id = DefinitionIdValue.new_id()
    add_manual_run_res = await apply_add_manual_run(data.run_id, manual_run_definition_id, data.input.definition)
    if add_manual_run_res.is_error():
        return CompletedWith.Error(str(add_manual_run_res.error))
    execute_definition_data = to_execute_definition_data(manual_run_definition_id)
    manual_run_res = await run_execute_definition_action(run_action, execute_definition_data)
    if manual_run_res.is_error():
        await async_catch_ex(manual_run_storage.delete)(data.run_id)
    return manual_run_res\
        .map(lambda _: None)\
        .map_error(str).map_error(CompletedWith.Error)\
        .merge()

@complete_manual_run_handler
async def handle_complete_manual_run_action(data: ActionData[None, CompletedResult]):
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    @manual_run_storage.with_storage
    def apply_complete_manual_run(state: ManualRunState | None, result: CompletedResult):
        if state is None:
            raise NotFoundException()
        new_state = state.complete(result)
        return (result, new_state)
    
    complete_manual_run_res = await apply_complete_manual_run(data.run_id, data.input)
    if complete_manual_run_res.is_error():
        return CompletedWith.Error(str(complete_manual_run_res.error))
    
    result_dict = CompletedResultAdapter.to_dict(data.input)
    return CompletedWith.Data(result_dict)

# ------------------------------------------------------------------------------------------------------------
