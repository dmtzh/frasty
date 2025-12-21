from typing import Any
from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse

from manualrunstate import ManualRunStateAdapter, ManualRunState
from manualrunstore import manual_run_storage
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, StepIdValue
from shared.definition import ActionDefinitionConfigAdapter, Definition, DefinitionAdapter, InputDataAdapter
from shared.definitionsstore import definitions_storage
from shared.dtodefinition import DefinitionAdapter as LegacyDefinitionAdapter
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.pipeline.actionhandler import ActionData, ActionDataDto, CompletedDefinitionData
from shared.pipeline.types import CompletedDefinitionData as LegacyCompletedDefinitionData
from shared.runningdefinition import RunningDefinitionState
from shared.runningdefinitionsstore import running_action_definitions_storage
from shared.utils.asyncresult import async_catch_ex, async_ex_to_error_result
from shared.utils.result import ResultTag

import adddefinitionapihandler
from config import ExecuteDefinitionData, ExecuteDefinitionInput, app, execute_definition_handler, manual_run_definition_completed_subscriber, run_action, run_execute_definition_action, run_first_step_manually
import manualrunapihandler
import runner.executedefinitionhandler as executedefinitionhandler

@app.get("/tickets")
def tickets():
    return FileResponse("./html_sources/get_ticket.html")

@app.post("/definitions")
async def add_definition(request: adddefinitionapihandler.AddDefinitionRequest):
    return await adddefinitionapihandler.handle(request)

@app.get("/definitions/legacy/{id}")
async def legacy_get_definition(id: str):
    opt_def_id = DefinitionIdValue.from_value_with_checksum(id)
    if opt_def_id is None:
        raise HTTPException(status_code=404)
    opt_definition_with_ver_res = await async_catch_ex(definitions_storage.get_with_ver)(opt_def_id)
    match opt_definition_with_ver_res:
        case Result(ResultTag.OK, ok=None):
            raise HTTPException(status_code=404)
        case Result(ResultTag.OK, ok=(definition, _)):
            definition_dto = LegacyDefinitionAdapter.to_list(definition)
            return definition_dto
        case _:
            raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")

@app.post("/definition/legacy/manual-run", status_code=201)
async def legacy_manual_run(request: manualrunapihandler.ManualRunRequest):
    return await manualrunapihandler.handle(run_first_step_manually, request)

@app.get("/definition/legacy/manual-run/{id}")
async def legacy_get_status(id: str):
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

@manual_run_definition_completed_subscriber
async def complete_manual_run_state_with_result(data: LegacyCompletedDefinitionData):
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    @manual_run_storage.with_storage
    def apply_complete_manual_run(state: ManualRunState | None, result: CompletedResult):
        if state is None:
            raise NotFoundException()
        new_state = state.complete(result)
        return (result, new_state)
    
    complete_manual_run_res = await apply_complete_manual_run(data.run_id, data.result)
    match complete_manual_run_res:
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            return None
        case _:
            return complete_manual_run_res

@app.post("/definition/manual-run", status_code=201)
async def manual_run(request: manualrunapihandler.ManualRunRequest):
    def to_execute_definition_data(definition: Definition):
        run_id = RunIdValue.new_id()
        step_id = StepIdValue.new_id()
        input = ExecuteDefinitionInput(None, definition)
        metadata = Metadata()
        metadata.set_from("action manual run webapi")
        return ExecuteDefinitionData(run_id, step_id, None, input, metadata)
    async def err_to_http(err):
        errors = [{"loc": ["body", "resource"], "type": "value_error", "msg": str(err)}]
        raise RequestValidationError(errors)
    
    raw_definition = request.resource
    run_res = DefinitionAdapter\
        .from_list(raw_definition)\
        .map(to_execute_definition_data)\
        .map(run_execute_definition_action)\
        .default_with(err_to_http)
    return await run_res

@execute_definition_handler
async def handle_execute_definition_action(data: ActionData[None, ExecuteDefinitionInput]):
    definition_id = data.input.opt_definition_id or DefinitionIdValue.new_id()
    async def run_first_step_handler(evt: RunningDefinitionState.Events.StepRunning) -> Result[None, Any]:
        data_dict = InputDataAdapter.to_dict(evt.input_data) | ActionDefinitionConfigAdapter.to_dict(evt.step_definition.config)
        metadata = Metadata()
        metadata.set_from("execute definition action")
        metadata.set_definition_id(definition_id)
        metadata.set("parent_metadata", data.metadata.to_dict())
        metadata.set_id("parent_step_id", data.step_id)
        metadata_dict = metadata.to_dict()
        action_data = ActionDataDto(data.run_id.to_value_with_checksum(), evt.step_id.to_value_with_checksum(), data_dict, metadata_dict)
        return await run_action(evt.step_definition.get_name(), action_data)
    cmd = executedefinitionhandler.ExecuteDefinitionCommand(data.run_id, definition_id, data.input.definition)
    execute_definition_res = await executedefinitionhandler.handle(
        running_action_definitions_storage.with_storage,
        run_first_step_handler,
        cmd
    )
    def ok_to_none(_):
        # Definition started and will complete eventually. Return None to properly handle ongoing execute definition action.
        return None
    def err_to_completed_result(err):
        # Definition failed to start. Return CompletedWith.Data result to complete execute definition action.
        completed_def_data = CompletedDefinitionData(definition_id, CompletedWith.Error(str(err)))
        return CompletedWith.Data(completed_def_data.to_dict())
        
    return execute_definition_res\
        .map(ok_to_none)\
        .map_error(err_to_completed_result)\
        .merge()
    