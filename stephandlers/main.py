# import asyncio
from collections.abc import Generator
from dataclasses import dataclass
import functools
from typing import Any

from expression import Result, effect

from infrastructure import rabbitcompletestep as rabbit_complete_step
from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrunstep as rabbit_run_step
from infrastructure import rabbitruntask as rabbit_task
from infrastructure.rabbitmiddlewares import RequeueChance
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import TaskIdValue
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.utils.asyncresult import AsyncResult, async_ex_to_error_result, async_result, coroutine_result, make_async
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtmlConfig, GetContentFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl, RequestUrlInputData
from stepdefinitions.shared import HttpResponseData
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from config import app, rabbit_client
import filterhtmlresponse.handler as filterhtmlresponsehandler
import filtersuccessresponse.handler as filtersuccessresponsehandler
from fetchnewdata.fetchidvalue import FetchIdValue
import fetchnewdata.handler as fetchnewdatahandler
import getcontentfromhtml.handler as getcontentfromhtmlhandler
import requesturl.handler as requesturlhandler

class RabbitGetContentFromHtmlCommand(rabbit_run_step.RunStepData[GetContentFromHtmlConfig, dict | list]):
    '''Input data for get content from html command'''

@dataclass(frozen=True)
class GetContentFromHtmlCommandValidationError:
    error: Any

@rabbit_run_step.handler(rabbit_client, GetContentFromHtml, GetContentFromHtml.validate_input, RabbitGetContentFromHtmlCommand)
@make_async
def handle_get_content_from_html_command(input):
    @effect.result[CompletedResult, GetContentFromHtmlCommandValidationError]()
    def get_content_from_html(input: Result[RabbitGetContentFromHtmlCommand, Any]) -> Generator[Any, Any, CompletedResult]:
        step_data = yield from input.map_error(GetContentFromHtmlCommandValidationError)
        cmd = getcontentfromhtmlhandler.GetContentFromHtmlCommand(step_data.config, step_data.data)
        res = getcontentfromhtmlhandler.handle(cmd)
        return res
    
    get_content_from_html_res = get_content_from_html(input)
    return get_content_from_html_res.default_value(None)

class RabbitFilterHtmlResponseCommand(rabbit_run_step.RunStepData[None, HttpResponseData]):
    '''Input data for filter html response command'''

@dataclass(frozen=True)
class FilterHtmlResponseCommandValidationError:
    error: Any

@rabbit_run_step.handler(rabbit_client, FilterHtmlResponse, HttpResponseData.from_dict, RabbitFilterHtmlResponseCommand)
@make_async
def handle_filter_html_response_command(input):
    @effect.result[CompletedResult, FilterHtmlResponseCommandValidationError]()
    def filter_html_response(input: Result[RabbitFilterHtmlResponseCommand, Any]) -> Generator[Any, Any, CompletedResult]:
        step_data = yield from input.map_error(FilterHtmlResponseCommandValidationError)
        cmd = filterhtmlresponsehandler.FilterHtmlResponseCommand(step_data.data)
        res = filterhtmlresponsehandler.handle(cmd)
        return res
    
    filter_html_response_res = filter_html_response(input)
    return filter_html_response_res.default_value(None)

class RabbitFilterSuccessResponseCommand(rabbit_run_step.RunStepData[None, HttpResponseData]):
    '''Input data for filter success response command'''

@dataclass(frozen=True)
class FilterSuccessResponseCommandValidationError:
    error: Any

@rabbit_run_step.handler(rabbit_client, FilterSuccessResponse, HttpResponseData.from_dict, RabbitFilterSuccessResponseCommand)
@make_async
def handle_filter_success_response_command(input):
    @effect.result[CompletedResult, FilterSuccessResponseCommandValidationError]()
    def filter_success_response(input: Result[RabbitFilterSuccessResponseCommand, Any]) -> Generator[Any, Any, CompletedResult]:
        step_data = yield from input.map_error(FilterSuccessResponseCommandValidationError)
        cmd = filtersuccessresponsehandler.FilterSuccessResponseCommand(step_data.data)
        res = filtersuccessresponsehandler.handle(cmd)
        return res
    
    filter_success_response_res = filter_success_response(input)
    return filter_success_response_res.default_value(None)

class RabbitRequestUrlCommand(rabbit_run_step.RunStepData[None, RequestUrlInputData]):
    '''Input data for request url command'''

@dataclass(frozen=True)
class RequestUrlCommandValidationError:
    error: Any

@rabbit_run_step.handler(rabbit_client, RequestUrl, RequestUrlInputData.from_dict, RabbitRequestUrlCommand)
async def handle_request_url_command(input):
    @coroutine_result[RequestUrlCommandValidationError]()
    async def process_request_url(input: Result[RabbitRequestUrlCommand, Any]):
        step_data = await AsyncResult.from_result(input).map_error(RequestUrlCommandValidationError)
        cmd = requesturlhandler.RequestUrlCommand(step_data.data)
        res = await requesturlhandler.handle(cmd)
        return res
    
    process_request_url_res = await process_request_url(input)
    return process_request_url_res.default_value(None)

class RabbitFetchNewDataCommand(rabbit_run_step.RunStepData[None, FetchNewDataInput]):
    '''Input data for fetch new data command'''

@rabbit_run_step.handler(rabbit_client, FetchNewData, FetchNewDataInput.from_dict, RabbitFetchNewDataCommand)
async def handle_fetch_new_data_command(input):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_task_handler(parent_metadata: dict, cmd: fetchnewdatahandler.RunTaskCommand):
        metadata = {
            "fetch_id": cmd.fetch_id.to_value_with_checksum(),
            "parent_metadata": parent_metadata
        }
        return rabbit_task.run(rabbit_client, cmd.task_id, cmd.run_id, "fetch_new_data_handler", metadata)
    match input:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is RabbitFetchNewDataCommand:
            fetch_new_data_cmd = fetchnewdatahandler.FetchNewDataCommand(fetch_task_id=data.data.task_id, run_id=data.run_id, step_id=data.step_id)
            run_task_handler = functools.partial(rabbit_run_task_handler, data.metadata)
            fetch_new_data_res = await fetchnewdatahandler.handle(run_task_handler, fetch_new_data_cmd)
            match fetch_new_data_res:
                case Result(tag=ResultTag.ERROR, error=error):
                    return CompletedWith.Error(str(error))
                case _:
                    return None

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