# import asyncio
from expression import Result

from shared.completedresult import CompletedWith
from shared.pipeline.types import StepData
from shared.utils.result import ResultTag
from stepdefinitions.requesturl import RequestUrl, RequestUrlInputData
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from config import app, step_handler
import fetchnewdata.handler as fetchnewdatahandler
import requesturl.handler as requesturlhandler

# ------------------------------------------------------------------------------------------------------------

@step_handler(RequestUrl, RequestUrlInputData.from_dict)
def handle_request_url_command(step_data: StepData[None, RequestUrlInputData]):
    cmd = requesturlhandler.RequestUrlCommand(step_data.data)
    return requesturlhandler.handle(cmd)

# ------------------------------------------------------------------------------------------------------------

@step_handler(FetchNewData, FetchNewDataInput.from_dict)
async def handle_fetch_new_data_command(step_data: StepData[None, FetchNewDataInput]):
    async def fetch_data_handler(fetch_id: fetchnewdatahandler.FetchIdValue) -> Result[None, fetchnewdatahandler.FetchDataError]:
        return Result.Ok(None)
    cmd = fetchnewdatahandler.FetchNewDataCommand(fetch_task_id=step_data.data.task_id, run_id=step_data.run_id, step_id=step_data.step_id)
    res = await fetchnewdatahandler.handle(fetch_data_handler, cmd)
    match res:
        case Result(tag=ResultTag.ERROR, error=error):
            return CompletedWith.Error(str(error))
        case _:
            return None # we won't complete step now, we will complete only after receive and process data

# ------------------------------------------------------------------------------------------------------------

# if __name__ == "__main__":
#     asyncio.run(app.run())