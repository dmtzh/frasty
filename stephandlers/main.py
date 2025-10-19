# import asyncio
from collections.abc import Generator
from dataclasses import dataclass
import functools
from typing import Any

from expression import Result, effect

from infrastructure import rabbitcompletestep as rabbit_complete_step
from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitruntask as rabbit_task
from infrastructure.rabbitmiddlewares import RequeueChance
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import TaskIdValue
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.runstepdata import RunStepData
from shared.utils.asyncresult import AsyncResult, async_ex_to_error_result, async_result, coroutine_result, make_async
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtmlConfig, GetContentFromHtml, GetLinksFromHtmlConfig, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl, RequestUrlInputData
from stepdefinitions.shared import HttpResponseData, ContentData, ListOfContentData
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from config import app, rabbit_client, run_step_handler, viber_api_config
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
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_task_handler(parent_metadata: dict, cmd: fetchnewdatahandler.RunTaskCommand):
        metadata = {
            "fetch_id": cmd.fetch_id.to_value_with_checksum(),
            "parent_metadata": parent_metadata
        }
        return rabbit_task.run(rabbit_client, cmd.task_id, cmd.run_id, "fetch_new_data_handler", metadata)
    
    run_task_handler = functools.partial(rabbit_run_task_handler, step_data.metadata)
    cmd = fetchnewdatahandler.FetchNewDataCommand(fetch_task_id=step_data.data.task_id, run_id=step_data.run_id, step_id=step_data.step_id)
    res = await fetchnewdatahandler.handle(run_task_handler, cmd)
    match res:
        case Result(tag=ResultTag.ERROR, error=error):
            return CompletedWith.Error(str(error))
        case _:
            return None # we won't complete step now, we will complete only after receive and process new data from task

# ------------------------------------------------------------------------------------------------------------

@effect.result[tuple[FetchIdValue, fetchnewdatahandler.CompletedTaskData, dict], str]()
def definition_to_fetched_task(data: rabbit_definition_completed.DefinitionCompletedData) -> Generator[Any, Any, tuple[FetchIdValue, fetchnewdatahandler.CompletedTaskData, dict]]:
    yield from parse_from_dict(data.metadata, "from", lambda s: True if s == "fetch_new_data_handler" else None)
    fetch_id = yield from parse_from_dict(data.metadata, "fetch_id", FetchIdValue.from_value_with_checksum)
    task_id = yield from parse_from_dict(data.metadata, "task_id", TaskIdValue.from_value_with_checksum)
    result = yield from Result.Ok(data.result) if type(data.result) is CompletedWith.Data or type(data.result) is CompletedWith.NoData else Result.Error("result is invalid")
    completed_data = fetchnewdatahandler.CompletedTaskData(task_id, data.run_id, result)
    parent_metadata = yield from parse_from_dict(data.metadata, "parent_metadata", lambda pm: pm if type(pm) is dict else None)
    return fetch_id, completed_data, parent_metadata

@dataclass(frozen=True)
class FetchedTaskValidationError:
    error: Any

@rabbit_definition_completed.subscriber(rabbit_client, rabbit_definition_completed.DefinitionCompletedData, queue_name="fetchnewdata_completed_tasks", requeue_chance=RequeueChance.HIGH)
async def handle_fetched_task(input):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_fetch_new_data_completed_handler(metadata: dict, fetch_cmd: fetchnewdatahandler.FetchNewDataCommand, completed_result: CompletedResult):
        return rabbit_complete_step.run(rabbit_client, fetch_cmd.run_id, fetch_cmd.step_id, completed_result, metadata)
    @coroutine_result()
    async def process_fetched_task(input: Result[rabbit_definition_completed.DefinitionCompletedData, Any]):
        fetch_id, completed_data, parent_metadata = await AsyncResult.from_result(input.bind(definition_to_fetched_task))\
            .map_error(FetchedTaskValidationError)
        fetch_new_data_completed_handler = functools.partial(rabbit_fetch_new_data_completed_handler, parent_metadata)
        completed_result = await async_result(fetchnewdatahandler.handle_completed_task)(fetch_new_data_completed_handler, fetch_id, completed_data)
        return completed_result
    
    process_fetched_task_res = await process_fetched_task(input)
    match process_fetched_task_res:
        case Result(tag=ResultTag.ERROR, error=FetchedTaskValidationError()):
            return None
        case Result(tag=ResultTag.ERROR, error=ValueInvalid()):
            return None
        case _:
            return process_fetched_task_res

# if __name__ == "__main__":
#     asyncio.run(app.run())