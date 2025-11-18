# import asyncio
import functools

from expression import Result

from shared.completedresult import CompletedResult, CompletedWith
from shared.pipeline.types import CompleteStepData, StepInputData
from shared.utils.asyncresult import make_async
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtmlConfig, GetContentFromHtml, GetLinksFromHtmlConfig, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl, RequestUrlInputData
from stepdefinitions.shared import HttpResponseData, ContentData, ListOfContentData
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from config import FetchedData, app, complete_step, data_fetched_subscriber, fetch_data, step_handler, viber_api_config
import filterhtmlresponse.handler as filterhtmlresponsehandler
import filtersuccessresponse.handler as filtersuccessresponsehandler
import fetchnewdata.handler as fetchnewdatahandler
from getcontentfromjson.definition import GetContentFromJson, GetContentFromJsonConfig
import getcontentfromjson.handler as getcontentfromjsonhandler
import getcontentfromhtml.handler as getcontentfromhtmlhandler
import getlinksfromhtml.handler as getlinksfromhtmlhandler
import requesturl.handler as requesturlhandler
from sendtoviberchannel.definition import SendToViberChannel, SendToViberChannelConfig
import sendtoviberchannel.handler as sendtoviberchannelhandler

# ------------------------------------------------------------------------------------------------------------

@step_handler(GetContentFromJson, GetContentFromJson.validate_input)
@make_async
def handle_get_content_from_json_command(step_data: StepInputData[GetContentFromJsonConfig, ContentData | ListOfContentData]):
    cmd = getcontentfromjsonhandler.GetContentFromJsonCommand(step_data.config, step_data.data)
    res = getcontentfromjsonhandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

@step_handler(SendToViberChannel, SendToViberChannel.validate_input)
def handle_send_to_viber_channel_command(step_data: StepInputData[SendToViberChannelConfig, list]):
    cmd = sendtoviberchannelhandler.SendToViberChannelCommand(step_data.config.channel_id, step_data.config.title, step_data.data)
    return sendtoviberchannelhandler.handle(viber_api_config, cmd)

# ------------------------------------------------------------------------------------------------------------

@step_handler(GetLinksFromHtml, GetLinksFromHtml.validate_input)
@make_async
def handle_get_links_from_html_command(step_data: StepInputData[GetLinksFromHtmlConfig, dict | list]):
    cmd = getlinksfromhtmlhandler.GetLinksFromHtmlCommand(step_data.config, step_data.data)
    res = getlinksfromhtmlhandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

@step_handler(GetContentFromHtml, GetContentFromHtml.validate_input)
@make_async
def handle_get_content_from_html_command(step_data: StepInputData[GetContentFromHtmlConfig, dict | list]):
    cmd = getcontentfromhtmlhandler.GetContentFromHtmlCommand(step_data.config, step_data.data)
    res = getcontentfromhtmlhandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

@step_handler(FilterHtmlResponse, HttpResponseData.from_dict)
@make_async
def handle_filter_html_response_command(step_data: StepInputData[None, HttpResponseData]):
    cmd = filterhtmlresponsehandler.FilterHtmlResponseCommand(step_data.data)
    res = filterhtmlresponsehandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

@step_handler(FilterSuccessResponse, HttpResponseData.from_dict)
@make_async
def handle_filter_success_response_command(step_data: StepInputData[None, HttpResponseData]):
    cmd = filtersuccessresponsehandler.FilterSuccessResponseCommand(step_data.data)
    res = filtersuccessresponsehandler.handle(cmd)
    return res

# ------------------------------------------------------------------------------------------------------------

@step_handler(RequestUrl, RequestUrlInputData.from_dict)
def handle_request_url_command(step_data: StepInputData[None, RequestUrlInputData]):
    cmd = requesturlhandler.RequestUrlCommand(step_data.data)
    return requesturlhandler.handle(cmd)

# ------------------------------------------------------------------------------------------------------------

@step_handler(FetchNewData, FetchNewDataInput.from_dict)
async def handle_fetch_new_data_command(step_data: StepInputData[None, FetchNewDataInput]):
    fetch_data_handler = functools.partial(fetch_data, step_data)
    cmd = fetchnewdatahandler.FetchNewDataCommand(fetch_task_id=step_data.data.task_id, run_id=step_data.run_id, step_id=step_data.step_id)
    res = await fetchnewdatahandler.handle(fetch_data_handler, cmd)
    match res:
        case Result(tag=ResultTag.ERROR, error=error):
            return CompletedWith.Error(str(error))
        case _:
            return None # we won't complete step now, we will complete only after receive and process data

@data_fetched_subscriber
async def handle_fetched_data(fetched_data: FetchedData):
    if isinstance(fetched_data.result, CompletedWith.Error):
        return None
    def fetch_new_data_completed_handler(fetch_cmd: fetchnewdatahandler.FetchNewDataCommand, completed_result: CompletedResult):
        data = CompleteStepData(fetch_cmd.run_id, fetch_cmd.step_id, completed_result, fetched_data.metadata)
        return complete_step(data)
    completed_data = fetchnewdatahandler.CompletedTaskData(fetched_data.task_id, fetched_data.run_id, fetched_data.result)
    handle_fetched_data_res = await fetchnewdatahandler.handle_fetched_data(fetch_new_data_completed_handler, fetched_data.fetch_id, completed_data)
    match handle_fetched_data_res:
        case Result(tag=ResultTag.ERROR, error=ValueInvalid()):
            return None
        case _:
            return handle_fetched_data_res

# ------------------------------------------------------------------------------------------------------------

# if __name__ == "__main__":
#     asyncio.run(app.run())