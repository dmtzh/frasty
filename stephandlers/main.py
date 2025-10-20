# import asyncio
from dataclasses import dataclass
import functools

from expression import Result

from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import TaskIdValue, RunIdValue
from shared.runstepdata import RunStepData
from shared.utils.asyncresult import make_async
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtmlConfig, GetContentFromHtml, GetLinksFromHtmlConfig, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl, RequestUrlInputData
from stepdefinitions.shared import HttpResponseData, ContentData, ListOfContentData
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from config import app, complete_step, data_fetched_handler, run_step_handler, run_task, viber_api_config
import filterhtmlresponse.handler as filterhtmlresponsehandler
import filtersuccessresponse.handler as filtersuccessresponsehandler
from fetchnewdata.fetchidvalue import FetchIdValue
import fetchnewdata.handler as fetchnewdatahandler
from getcontentfromjson.definition import GetContentFromJson, GetContentFromJsonConfig
import getcontentfromjson.handler as getcontentfromjsonhandler
import getcontentfromhtml.handler as getcontentfromhtmlhandler
import getlinksfromhtml.handler as getlinksfromhtmlhandler
import requesturl.handler as requesturlhandler
from sendtoviberchannel.definition import SendToViberChannel, SendToViberChannelConfig
import sendtoviberchannel.handler as sendtoviberchannelhandler

# ------------------------------------------------------------------------------------------------------------

class RabbitGetContentFromJsonCommand(RunStepData[GetContentFromJsonConfig, ContentData | ListOfContentData]):
    '''Input data for get content from json command'''

@run_step_handler(GetContentFromJson, GetContentFromJson.validate_input, RabbitGetContentFromJsonCommand)
@make_async
def handle_get_content_from_json_command(step_data: RunStepData[GetContentFromJsonConfig, ContentData | ListOfContentData]):
    cmd = getcontentfromjsonhandler.GetContentFromJsonCommand(step_data.config, step_data.data)
    res = getcontentfromjsonhandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

class SendToViberChannelCommand(RunStepData[SendToViberChannelConfig, list]):
    '''Input data for send to viber channel command'''

@run_step_handler(SendToViberChannel, SendToViberChannel.validate_input, SendToViberChannelCommand)
def handle_send_to_viber_channel_command(step_data: RunStepData[SendToViberChannelConfig, list]):
    cmd = sendtoviberchannelhandler.SendToViberChannelCommand(step_data.config.channel_id, step_data.config.title, step_data.data)
    return sendtoviberchannelhandler.handle(viber_api_config, cmd)

# ------------------------------------------------------------------------------------------------------------

class RabbitGetLinksFromHtmlCommand(RunStepData[GetLinksFromHtmlConfig, dict | list]):
    '''Input data for get content from html command'''

@run_step_handler(GetLinksFromHtml, GetLinksFromHtml.validate_input, RabbitGetLinksFromHtmlCommand)
@make_async
def handle_get_links_from_html_command(step_data: RunStepData[GetLinksFromHtmlConfig, dict | list]):
    cmd = getlinksfromhtmlhandler.GetLinksFromHtmlCommand(step_data.config, step_data.data)
    res = getlinksfromhtmlhandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

class RabbitGetContentFromHtmlCommand(RunStepData[GetContentFromHtmlConfig, dict | list]):
    '''Input data for get content from html command'''

@run_step_handler(GetContentFromHtml, GetContentFromHtml.validate_input, RabbitGetContentFromHtmlCommand)
@make_async
def handle_get_content_from_html_command(step_data: RunStepData[GetContentFromHtmlConfig, dict | list]):
    cmd = getcontentfromhtmlhandler.GetContentFromHtmlCommand(step_data.config, step_data.data)
    res = getcontentfromhtmlhandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

class RabbitFilterHtmlResponseCommand(RunStepData[None, HttpResponseData]):
    '''Input data for filter html response command'''

@run_step_handler(FilterHtmlResponse, HttpResponseData.from_dict, RabbitFilterHtmlResponseCommand)
@make_async
def handle_filter_html_response_command(step_data: RunStepData[None, HttpResponseData]):
    cmd = filterhtmlresponsehandler.FilterHtmlResponseCommand(step_data.data)
    res = filterhtmlresponsehandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

class RabbitFilterSuccessResponseCommand(RunStepData[None, HttpResponseData]):
    '''Input data for filter success response command'''

@run_step_handler(FilterSuccessResponse, HttpResponseData.from_dict, RabbitFilterSuccessResponseCommand)
@make_async
def handle_filter_success_response_command(step_data: RunStepData[None, HttpResponseData]):
    cmd = filtersuccessresponsehandler.FilterSuccessResponseCommand(step_data.data)
    res = filtersuccessresponsehandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

class RabbitRequestUrlCommand(RunStepData[None, RequestUrlInputData]):
    '''Input data for request url command'''

@run_step_handler(RequestUrl, RequestUrlInputData.from_dict, RabbitRequestUrlCommand)
def handle_request_url_command(step_data: RunStepData[None, RequestUrlInputData]):
    cmd = requesturlhandler.RequestUrlCommand(step_data.data)
    return requesturlhandler.handle(cmd)

# ------------------------------------------------------------------------------------------------------------

class RabbitFetchNewDataCommand(RunStepData[None, FetchNewDataInput]):
    '''Input data for fetch new data command'''

@run_step_handler(FetchNewData, FetchNewDataInput.from_dict, RabbitFetchNewDataCommand)
async def handle_fetch_new_data_command(step_data: RunStepData[None, FetchNewDataInput]):
    def run_task_handler_with_parent_metadata(parent_metadata: dict, cmd: fetchnewdatahandler.RunTaskCommand):
        metadata = {
            "fetch_id": cmd.fetch_id.to_value_with_checksum(),
            "parent_metadata": parent_metadata
        }
        return run_task(cmd.task_id, cmd.run_id, "fetch new data step", metadata)
    
    run_task_handler = functools.partial(run_task_handler_with_parent_metadata, step_data.metadata)
    cmd = fetchnewdatahandler.FetchNewDataCommand(fetch_task_id=step_data.data.task_id, run_id=step_data.run_id, step_id=step_data.step_id)
    res = await fetchnewdatahandler.handle(run_task_handler, cmd)
    match res:
        case Result(tag=ResultTag.ERROR, error=error):
            return CompletedWith.Error(str(error))
        case _:
            return None # we won't complete step now, we will complete only after receive and process new data from task

# ------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class FetchedData:
    fetch_id: FetchIdValue
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedResult
    metadata: dict

@data_fetched_handler(FetchedData)
async def handle_fetched_data(fetched_data: FetchedData):
    if isinstance(fetched_data.result, CompletedWith.Error):
        return None
    def fetch_new_data_completed_handler(fetch_cmd: fetchnewdatahandler.FetchNewDataCommand, completed_result: CompletedResult):
        return complete_step(fetch_cmd.run_id, fetch_cmd.step_id, completed_result, fetched_data.metadata)
    completed_data = fetchnewdatahandler.CompletedTaskData(fetched_data.task_id, fetched_data.run_id, fetched_data.result)
    handle_completed_task_res = await fetchnewdatahandler.handle_completed_task(fetch_new_data_completed_handler, fetched_data.fetch_id, completed_data)
    match handle_completed_task_res:
        case Result(tag=ResultTag.ERROR, error=ValueInvalid()):
            return None
        case _:
            return handle_completed_task_res

# if __name__ == "__main__":
#     asyncio.run(app.run())