from dataclasses import dataclass

from expression import Result
from fastapi import HTTPException
from fastapi.responses import FileResponse

from manualrunstate import ManualRunStateAdapter, ManualRunState
from manualrunstore import manual_run_storage
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue
from shared.definitioncompleteddata import DefinitionCompletedData
from shared.definitionsstore import definitions_storage
from shared.domainrunning import RunningDefinitionState
from shared.dtodefinition import DefinitionAdapter
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import async_catch_ex, async_ex_to_error_result, async_result, coroutine_result
from shared.utils.result import ResultTag

import adddefinitionapihandler
from config import app, definition_completed_subscriber, run_step
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
    def run_first_step_handler(manual_run_id: RunIdValue, manual_definition_id: DefinitionIdValue, evt: RunningDefinitionState.Events.StepRunning):
        metadata = Metadata()
        metadata.set("from", "definition_webapi")
        metadata.set_definition_id(manual_definition_id)
        return run_step(manual_run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)
    return await manualrunapihandler.handle(run_first_step_handler, request)

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

@dataclass(frozen=True)
class CompleteManualRunCommand:
    run_id: RunIdValue
    result: CompletedResult

@dataclass(frozen=True)
class DataValidationError:
    '''Data validation error'''

@definition_completed_subscriber(DefinitionCompletedData)
async def complete_manual_run_definition_with_result(data: DefinitionCompletedData):    
    @async_result
    @make_async
    def to_complete_manual_run_cmd() -> Result[CompleteManualRunCommand, DataValidationError]:
        raw_from = data.metadata.get("from")
        match raw_from:
            case "definition_webapi":
                return Result.Ok(CompleteManualRunCommand(data.run_id, data.result))
            case _:
                return Result.Error(DataValidationError())
    @async_result
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    @manual_run_storage.with_storage
    def apply_complete(state: ManualRunState | None, result: CompletedResult):
        if state is None:
            raise NotFoundException()
        new_state = state.complete(result)
        return (result, new_state)
    @coroutine_result[DataValidationError | NotFoundError | StorageError]()
    async def complete_manual_run():
        cmd = await to_complete_manual_run_cmd()
        completed_result = await apply_complete(cmd.run_id, cmd.result)
        return completed_result
    
    complete_manual_run_res = await complete_manual_run()
    match complete_manual_run_res:
        case Result(tag=ResultTag.ERROR, error=DataValidationError()):
            return None
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            return None
        case _:
            return complete_manual_run_res
    