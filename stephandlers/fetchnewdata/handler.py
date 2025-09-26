from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import datetime
from typing import Any

from expression import Result, effect

from shared.completedresult import CompletedWith, CompletedResult
from shared.customtypes import Error, RunIdValue, TaskIdValue, StepIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.utils.asyncresult import AsyncResult, coroutine_result, async_result, async_ex_to_error_result, async_catch_ex
from shared.utils.parse import parse_from_str
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid
from stepdefinitions.shared import DictData, ListOfDictData

from .executingtasksstore import executing_tasks_storage, ExecutingTaskData
from .fetchidvalue import FetchIdValue
from .previousdatastore import previous_data_storage

@dataclass(frozen=True)
class FetchNewDataCommand:
    fetch_task_id: TaskIdValue
    run_id: RunIdValue
    step_id: StepIdValue

@dataclass(frozen=True)
class RunTaskCommand:
    task_id: TaskIdValue
    run_id: RunIdValue
    fetch_id: FetchIdValue

@dataclass(frozen=True)
class RunTaskError:
    fetch_id: FetchIdValue
    error: Any

@dataclass(frozen=True)
class CompletedTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedWith.Data | CompletedWith.NoData

def step_id_to_fetch_id(step_id: StepIdValue):
    return parse_from_str(step_id, "fetch_id", FetchIdValue.from_value).map_error(lambda _: ValueInvalid("fetch_id"))

@dataclass(frozen=True)
class HistoryItem:
    run_id: RunIdValue | None

@async_result
async def get_recent_history_item(task_id: TaskIdValue):
    run_id = None
    return Result.Ok(HistoryItem(run_id))

@async_result
@async_ex_to_error_result(StorageError.from_exception)
def apply_add_running(cmd: RunTaskCommand):
    timestamp = int(datetime.datetime.now().timestamp())
    data = ExecutingTaskData(cmd.task_id, cmd.run_id, timestamp)
    return executing_tasks_storage.add(cmd.fetch_id, data)

@coroutine_result()
async def run_task_workflow(run_task_handler: Callable[[RunTaskCommand], Coroutine[Any, Any, Result]], fetch_cmd: FetchNewDataCommand):
    fetch_id = await AsyncResult.from_result(step_id_to_fetch_id(fetch_cmd.step_id))
    run_cmd = RunTaskCommand(fetch_cmd.fetch_task_id, fetch_cmd.run_id, fetch_id)
    await apply_add_running(run_cmd)
    await async_result(run_task_handler)(run_cmd).map_error(lambda error: RunTaskError(fetch_id, error))
    return fetch_id

async def clean_up_failed_command(error):
    match error:
        case RunTaskError(fetch_id=fetch_id, error=_):
            await async_catch_ex(executing_tasks_storage.remove)(fetch_id)

def map_command_errors(error):
    match error:
        case RunTaskError(fetch_id=_, error=run_task_err):
            return run_task_err
        case _:
            return error

async def handle(run_task_handler: Callable[[RunTaskCommand], Coroutine[Any, Any, Result]], cmd: FetchNewDataCommand):
    run_task_res = await run_task_workflow(run_task_handler, cmd)
    if run_task_res.is_error():
        await clean_up_failed_command(run_task_res.error)
    return run_task_res.map_error(map_command_errors)

class CannotValidateError(Error):
    '''Completed data cannot be validated due to unexpected error'''

@async_result
@async_ex_to_error_result(CannotValidateError.from_exception)
async def validate_completed_data(fetch_id: FetchIdValue, completed_data: CompletedTaskData) -> Result[CompletedTaskData, ValueInvalid]:
    if type(completed_data.result) is not CompletedWith.Data and type(completed_data.result) is not CompletedWith.NoData:
        return Result.Error(ValueInvalid("completed_data"))
    opt_data = await executing_tasks_storage.get(fetch_id)
    match opt_data:
        case None:
            return Result.Error(ValueInvalid("fetch_id"))
        case data:
            task_id_same = completed_data.task_id == data.task_id
            run_id_same = completed_data.run_id == data.run_id
            match task_id_same, run_id_same:
                case True, True:
                    return Result.Ok(completed_data)
                case _:
                    return Result.Error(ValueInvalid("completed_data"))

@async_result
@async_ex_to_error_result(StorageError.from_exception)
def get_prev_data(task_id: TaskIdValue):
    return previous_data_storage.get(task_id)

def filter_new_data(data: Any, data_to_exclude: Any):
    def filter_new_dict_data(data: Any, data_to_exclude: Any):
        @effect.result[dict, str]()
        def filter_new_data():
            dict_src_data = yield from DictData.from_dict(data)\
                .map(DictData.to_dict)\
                .map_error(lambda _: f"fetchnewdatahandler received invalid dict data {data}")
            err = f"fetchnewdatahandler cannot compare dict with {type(data_to_exclude).__name__}"
            dict_data_to_exclude = yield from DictData.from_dict(data_to_exclude)\
                .map(DictData.to_dict)\
                .map_error(lambda _: err)
            filtered_data = {k: v for k, v in dict_src_data.items() if k not in dict_data_to_exclude or v != dict_data_to_exclude[k]}
            return filtered_data
        if not isinstance(data, dict):
            return None
        res = filter_new_data()\
            .map(lambda filtered_dict_data: [filtered_dict_data] if filtered_dict_data else [])
        return res

    def filter_new_list_data(data: Any, data_to_exclude: Any):
        @effect.result[list, str]()
        def filter_new_data():
            list_src_data = yield from ListOfDictData.from_list(data)\
                .map(ListOfDictData.to_list)\
                .map_error(lambda _: f"fetchnewdatahandler received invalid list data {data}")
            err = f"fetchnewdatahandler cannot compare list with {type(data_to_exclude).__name__}"
            list_data_to_exclude = yield from ListOfDictData.from_list(data_to_exclude)\
                .map(ListOfDictData.to_list)\
                .map_error(lambda _: err)
            filtered_data = [item for item in list_src_data if item not in list_data_to_exclude]
            return filtered_data
        if not isinstance(data, list):
            return None
        return filter_new_data()
    
    res = filter_new_dict_data(data, data_to_exclude)\
        or filter_new_list_data(data, data_to_exclude)\
        or Result[list, str].Error(f"fetchnewdatahandler cannot compare {type(data).__name__} with {type(data_to_exclude).__name__}")
    return res

def to_new_data(data: Any):
    def to_new_dict_data(data: Any) -> Result[list, str] | None:
        if not isinstance(data, dict):
            return None
        return DictData.from_dict(data)\
            .map(DictData.to_dict)\
            .map(lambda dict_data: [dict_data] if dict_data else [])\
            .map_error(lambda _: f"fetchnewdatahandler received invalid dict data {data}")

    def to_new_list_data(data: Any):
        if not isinstance(data, list):
            return None
        return ListOfDictData.from_list(data)\
            .map(ListOfDictData.to_list)\
            .map_error(lambda _: f"fetchnewdatahandler received invalid list data {data}")
    
    return to_new_dict_data(data)\
        or to_new_list_data(data)\
        or Result[list, str].Error(f"fetchnewdatahandler received unsupported result of type {type(data).__name__}")

@coroutine_result[ValueInvalid | CannotValidateError | StorageError | str]()
async def get_new_data_workflow(fetch_id: FetchIdValue, completed_data: CompletedTaskData):
    valid_completed_data = await validate_completed_data(fetch_id, completed_data)
    opt_data_to_exclude = await get_prev_data(valid_completed_data.task_id)
    match valid_completed_data.result, opt_data_to_exclude:
        case CompletedWith.Data(data), CompletedWith.Data(None):
            new_data = await AsyncResult.from_result(to_new_data(data))
            return new_data
        case CompletedWith.Data(data), CompletedWith.Data(data_to_exclude):
            new_data = await AsyncResult.from_result(filter_new_data(data, data_to_exclude))
            return new_data
        case CompletedWith.Data(data), _:
            new_data = await AsyncResult.from_result(to_new_data(data))
            return new_data
        case _:
            return []

async def perform_fetch_new_data_completed_teardown(fetch_id: FetchIdValue, completed_data: CompletedTaskData):
    await async_catch_ex(executing_tasks_storage.remove)(fetch_id)
    match completed_data.result:
        case CompletedWith.NoData() | CompletedWith.Data():
            await async_catch_ex(previous_data_storage.set)(completed_data.task_id, completed_data.result)

async def handle_completed_task(fetch_new_data_completed_handler: Callable[[FetchNewDataCommand, CompletedResult], Coroutine[Any, Any, Result]], fetch_id: FetchIdValue, completed_data: CompletedTaskData) -> Result[CompletedResult, ValueInvalid | Error]:
    new_data_res = await get_new_data_workflow(fetch_id, completed_data)
    
    match new_data_res:
        case Result(tag=ResultTag.ERROR, error=ValueInvalid(name=value_name)):
            # Nothing to do with invalid data
            return Result.Error(ValueInvalid(value_name))
        case Result(tag=ResultTag.ERROR, error=CannotValidateError(message=error_message)):
            # completed_data cannot be validated due to unexpected error, let caller handle this
            return Result.Error(Error(error_message))
        case _:
            new_data_completed_result = new_data_res\
                .map(lambda new_data: CompletedWith.Data(new_data) if new_data else CompletedWith.NoData())\
                .map_error(lambda error: CompletedWith.Error(message=str(error)))\
                .merge()
            fetch_cmd = FetchNewDataCommand(completed_data.task_id, completed_data.run_id, fetch_id)
            fetch_completed_res = await fetch_new_data_completed_handler(fetch_cmd, new_data_completed_result)
            match fetch_completed_res:
                case Result(tag=ResultTag.OK, ok=_):
                    await perform_fetch_new_data_completed_teardown(fetch_id, completed_data)
            return fetch_completed_res.map(lambda _: new_data_completed_result)
