from expression import Result
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from faststream import Logger

from config import lifespan, rabbit_client
from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrunstep as rabbit_step
from manualrunstate import ManualRunStateAdapter, ManualRunState
from manualrunstore import manual_run_storage
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, IdValue
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
    return FileResponse("../../../html_sources/get_ticket.html")

@app.post("/definitions")
async def add_definition(request: adddefinitionapihandler.AddDefinitionRequest):
    return await adddefinitionapihandler.handle(request)

@app.get("/definitions/{id}")
async def get_definition(id: str):
    opt_def_id = DefinitionIdValue.from_value_with_checksum(id)
    if opt_def_id is None:
        raise HTTPException(status_code=404)
    opt_definition = await definitions_storage.get(opt_def_id)
    if opt_definition is None:
        raise HTTPException(status_code=404)
    definition_dto = DefinitionAdapter.to_list(opt_definition)
    return definition_dto

@app.post("/definition/manual-run", status_code=201)
async def manual_run(request: manualrunapihandler.ManualRunRequest):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_first_step_handler(manual_run_id: IdValue, manual_definition_id: IdValue, evt: RunningDefinitionState.Events.StepRunning):
        return rabbit_step.run(rabbit_client, None, manual_run_id, manual_definition_id, evt.step_id, evt.step_definition, evt.input_data, {})
    return await manualrunapihandler.handle(rabbit_run_first_step_handler, request)

@app.get("/definition/manual-run/{id}")
async def get_status(id: str):
    opt_state_res = await async_catch_ex(manual_run_storage.get)(IdValue(id[:-1]))
    match opt_state_res:
        case Result(ResultTag.OK, ok=None):
            raise HTTPException(status_code=404)
        case Result(ResultTag.OK, ok=state):
            state_dto = ManualRunStateAdapter.to_dict(state)
            return state_dto
        case _:
            raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")

@rabbit_definition_completed.subscriber(rabbit_client, rabbit_definition_completed.DefinitionCompletedData, queue_name=None)
async def complete_manual_run_definition_with_result(input, logger: Logger):
    logger.info(f"received input: {input}")
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
    @manual_run_storage.with_storage
    def apply_complete(state: ManualRunState | None, result: CompletedResult):
        if state is None:
            raise NotFoundException()
        new_state = state.complete(result)
        return (None, new_state)
    
    match input:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is rabbit_definition_completed.DefinitionCompletedData:
            res = await apply_complete(data.definition_id, data.result)
            match res:
                case Result(tag=ResultTag.ERROR, error=error) if type(error) is not NotFoundError:
                    rabbit_definition_completed.handle_processing_failure(rabbit_definition_completed.Severity.LOW)
            logger.info(f"manual run definition completed with result {res}")
            return res
                    