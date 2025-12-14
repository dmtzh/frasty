from expression import Result
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse

from manualrunstate import ManualRunStateAdapter, ManualRunState
from manualrunstore import manual_run_storage
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue
from shared.definition import Definition, DefinitionAdapter as DefinitionActionAdapter
from shared.definitionsstore import definitions_storage
from shared.dtodefinition import DefinitionAdapter
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.pipeline.actionhandler import ActionData
from shared.pipeline.types import CompletedDefinitionData
from shared.runningdefinition import RunningDefinitionState
from shared.runningdefinitionsstore import running_action_definitions_storage
from shared.utils.asyncresult import async_catch_ex, async_ex_to_error_result
from shared.utils.result import ResultTag

import adddefinitionapihandler
from config import app, execute_definition_handler, manual_run_definition_completed_subscriber, run_execute_definition_action, run_first_step_manually
import manualrunapihandler

@app.get("/tickets")
def tickets():
    return FileResponse("./html_sources/get_ticket.html")

@app.post("/definitions")
async def add_definition(request: adddefinitionapihandler.AddDefinitionRequest):
    return await adddefinitionapihandler.handle(request)

@app.get("/definitions/{id}")
async def get_definition(id: str):
    opt_def_id = DefinitionIdValue.from_value_with_checksum(id)
    if opt_def_id is None:
        raise HTTPException(status_code=404)
    opt_definition_with_ver_res = await async_catch_ex(definitions_storage.get_with_ver)(opt_def_id)
    match opt_definition_with_ver_res:
        case Result(ResultTag.OK, ok=None):
            raise HTTPException(status_code=404)
        case Result(ResultTag.OK, ok=(definition, _)):
            definition_dto = DefinitionAdapter.to_list(definition)
            return definition_dto
        case _:
            raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")

@app.post("/definition/manual-run", status_code=201)
async def manual_run(request: manualrunapihandler.ManualRunRequest):
    return await manualrunapihandler.handle(run_first_step_manually, request)

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

@manual_run_definition_completed_subscriber
async def complete_manual_run_state_with_result(data: CompletedDefinitionData):
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

@app.post("/definition/actiton-manual-run", status_code=201)
async def action_manual_run(request: manualrunapihandler.ManualRunRequest):
    raw_definition = request.resource
    def_res = DefinitionActionAdapter.from_list(raw_definition)
    async def err_to_http(err):
        errors = [{"loc": ["body", "resource"], "type": "value_error", "msg": str(err)}]
        raise RequestValidationError(errors)
    run_res = def_res\
        .map(run_execute_definition_action)\
        .default_with(err_to_http)
    return await run_res

@execute_definition_handler
async def execute_definition(input: ActionData[None, Definition]):
    definition_id = DefinitionIdValue.new_id()
    @async_catch_ex
    @running_action_definitions_storage.with_storage
    def apply_run_first_step(state: RunningDefinitionState | None, definition: Definition):
        if state is not None:
            raise ValueError("running definition state already exists")
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        if type(evt) is not RunningDefinitionState.Events.StepRunning:
            raise RuntimeError("expected step running event")
        return (evt, state)
    res = await apply_run_first_step(input.run_id, definition_id, input.data)
    print(res)
    return None