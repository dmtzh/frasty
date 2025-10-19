from dataclasses import dataclass
from expression import Result
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from config import lifespan, rabbit_client
from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrunstep as rabbit_step
from infrastructure.rabbitmiddlewares import RequeueChance
from manualrunstate import ManualRunStateAdapter, ManualRunState
from manualrunstore import manual_run_storage
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue
from shared.definitioncompleteddata import DefinitionCompletedData
from shared.definitionsstore import definitions_storage
from shared.domainrunning import RunningDefinitionState
from shared.dtodefinition import DefinitionAdapter
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.utils.asyncresult import async_catch_ex, async_ex_to_error_result
from shared.utils.result import ResultTag

import adddefinitionapihandler
import manualrunapihandler

app = FastAPI(lifespan=lifespan)

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
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_first_step_handler(manual_run_id: RunIdValue, manual_definition_id: DefinitionIdValue, evt: RunningDefinitionState.Events.StepRunning):
        metadata = {"from": "definition_webapi", "definition_id": manual_definition_id.to_value_with_checksum()}
        return rabbit_step.run(rabbit_client, manual_run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)
    return await manualrunapihandler.handle(rabbit_run_first_step_handler, request)

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

@rabbit_definition_completed.subscriber(rabbit_client, DefinitionCompletedData, queue_name=None, requeue_chance=RequeueChance.LOW)
async def complete_manual_run_definition_with_result(input):
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    @manual_run_storage.with_storage
    def apply_complete(state: ManualRunState | None, result: CompletedResult):
        if state is None:
            raise NotFoundException()
        new_state = state.complete(result)
        return (result, new_state)
    def from_definition_completed_data(data: DefinitionCompletedData) -> Result[CompleteManualRunCommand, str]:
        raw_from = data.metadata.get("from")
        if raw_from != "definition_webapi":
            return Result.Error("from is not definition_webapi")
        return Result.Ok(CompleteManualRunCommand(data.run_id, data.result))
    
    complete_manual_run_cmd_res = input.bind(from_definition_completed_data)
    match complete_manual_run_cmd_res:
        case Result(tag=ResultTag.OK, ok=cmd) if type(cmd) is CompleteManualRunCommand:
            res = await apply_complete(cmd.run_id, cmd.result)
            match res:
                case Result(tag=ResultTag.ERROR, error=NotFoundError()):
                    return None
                case _:
                    return res
                    