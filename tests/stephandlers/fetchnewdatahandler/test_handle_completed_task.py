import datetime
from expression import Result
import pytest
import pytest_asyncio

from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error, RunIdValue, TaskIdValue
from shared.validation import ValueInvalid

from stephandlers.fetchnewdata.executingtasksstore import executing_tasks_storage, ExecutingTaskData
from stephandlers.fetchnewdata.fetchidvalue import FetchIdValue
from stephandlers.fetchnewdata.handler import CompletedTaskData, RunTaskCommand, handle_completed_task, FetchNewDataCommand
from stephandlers.fetchnewdata.previousdatastore import previous_data_storage

@pytest_asyncio.fixture
async def executing_task():
    cmd = RunTaskCommand(TaskIdValue.new_id(), RunIdValue.new_id(), FetchIdValue.new_id())
    data = ExecutingTaskData(cmd.task_id, cmd.run_id, int(datetime.datetime.now().timestamp()))
    await executing_tasks_storage.add(cmd.fetch_id, data)
    return cmd

@pytest.fixture(scope="session")
def list_without_new_data():
    return [
        {"item1data1key": "item1data1value"},
        {"item2data2key": "item2data2value", "item2data3key": "item2data3value"},
    ]

@pytest.fixture(scope="session")
def new_list_data():
    return [{"item1newdata1key": "item1newdata1value", "item1newdata2key": "item1newdata2value"}, {"item2newdata3key": "item2newdata3value"}]

@pytest.fixture(scope="session")
def dict_without_new_data():
    return {"data1key": "data1value", "data2key": "data2value", "newdata1key": "data3value"}

@pytest.fixture(scope="session")
def new_dict_data():
    return {"newdata1key": "newdata1value", "newdata2key": "newdata2value"}

@pytest.fixture(scope="session")
def completed_handler():
    async def handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        return Result.Ok(None)
    return handler



async def test_when_prev_none_and_pass_list_data_then_returns_same_list(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_new_data = new_list_data
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Data
    assert new_data_res.ok.data == expected_new_data



async def test_when_prev_none_and_pass_dict_data_then_returns_list_with_same_dict(executing_task: RunTaskCommand, new_dict_data: dict, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_dict_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_new_data = [new_dict_data]
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Data
    assert new_data_res.ok.data == expected_new_data



async def test_when_prev_none_and_pass_no_data_then_returns_no_data(executing_task: RunTaskCommand, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.NoData()
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.NoData



async def test_when_prev_none_and_pass_none_data_then_completes_with_error(executing_task: RunTaskCommand, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(None)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Error



async def test_when_prev_no_data_and_pass_list_data_then_returns_same_list(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.NoData())
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_new_data = new_list_data
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Data
    assert new_data_res.ok.data == expected_new_data



async def test_when_prev_no_data_and_pass_dict_data_then_returns_list_with_same_dict(executing_task: RunTaskCommand, new_dict_data: dict, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.NoData())
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_dict_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_new_data = [new_dict_data]
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Data
    assert new_data_res.ok.data == expected_new_data



async def test_when_prev_no_data_and_pass_no_data_then_returns_no_data(executing_task: RunTaskCommand, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.NoData())
    fetch_id = executing_task.fetch_id
    completed_result = completed_result = CompletedWith.NoData()
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.NoData



async def test_when_prev_list_data_and_pass_no_data_then_returns_no_data(executing_task: RunTaskCommand, list_without_new_data: list, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(list_without_new_data))
    fetch_id = executing_task.fetch_id
    completed_result = completed_result = CompletedWith.NoData()
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.NoData



async def test_when_prev_dict_data_and_pass_no_data_then_returns_no_data(executing_task: RunTaskCommand, dict_without_new_data: dict, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(dict_without_new_data))
    fetch_id = executing_task.fetch_id
    completed_result = completed_result = CompletedWith.NoData()
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.NoData



async def test_when_prev_dict_data_and_pass_list_data_then_completes_with_error(executing_task: RunTaskCommand, dict_without_new_data: dict, new_list_data: list, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(dict_without_new_data))
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Error



async def test_when_prev_list_data_and_pass_dict_data_then_completes_with_error(executing_task: RunTaskCommand, list_without_new_data: list, new_dict_data: dict, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(list_without_new_data))
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_dict_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Error



async def test_when_prev_list_data_and_pass_list_with_new_data_then_returns_new_data_list(executing_task: RunTaskCommand, list_without_new_data: list, new_list_data: list, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(list_without_new_data))
    fetch_id = executing_task.fetch_id
    list_with_new_data = list_without_new_data + new_list_data
    completed_result = CompletedWith.Data(list_with_new_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_new_data = new_list_data
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Data
    assert new_data_res.ok.data == expected_new_data



async def test_when_prev_dict_data_and_pass_dict_with_new_data_then_returns_list_with_new_data_dict(executing_task: RunTaskCommand, dict_without_new_data: dict, new_dict_data: dict, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(dict_without_new_data))
    fetch_id = executing_task.fetch_id
    dict_with_new_data = dict_without_new_data | new_dict_data
    completed_result = CompletedWith.Data(dict_with_new_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_new_data = [new_dict_data]
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Data
    assert new_data_res.ok.data == expected_new_data



async def test_when_pass_list_without_new_data_then_returns_no_data(executing_task: RunTaskCommand, list_without_new_data: list, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(list_without_new_data))
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(list_without_new_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.NoData



async def test_when_pass_dict_without_new_data_then_returns_no_data(executing_task: RunTaskCommand, dict_without_new_data: dict, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(dict_without_new_data))
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(dict_without_new_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.NoData



async def test_when_pass_empty_list_data_then_completes_with_error(executing_task: RunTaskCommand, list_without_new_data: list, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(list_without_new_data))
    fetch_id = executing_task.fetch_id
    empty_list_data = []
    completed_result = CompletedWith.Data(empty_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Error



async def test_when_pass_empty_dict_data_then_completes_with_error(executing_task: RunTaskCommand, dict_without_new_data: dict, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(dict_without_new_data))
    fetch_id = executing_task.fetch_id
    empty_dict_data = {}
    completed_result = CompletedWith.Data(empty_dict_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Error



async def test_when_pass_none_data_then_completes_with_error(executing_task: RunTaskCommand, completed_handler):
    fetch_id = executing_task.fetch_id
    none_data = None
    completed_result = CompletedWith.Data(none_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Error



async def test_when_pass_str_data_then_completes_with_error(executing_task: RunTaskCommand, completed_handler):
    fetch_id = executing_task.fetch_id
    str_data = "string data"
    completed_result = CompletedWith.Data(str_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.ok) is CompletedWith.Error



async def test_when_pass_list_with_new_data_then_new_data_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand, new_list_data: list):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_completed_with = completed_result
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert state["actual_completed_with"] == expected_completed_with



async def test_when_pass_dict_with_new_data_then_list_with_new_data_dict_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand, new_dict_data: dict):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_dict_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_completed_with = CompletedWith.Data([new_dict_data])
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert state["actual_completed_with"] == expected_completed_with



async def test_when_pass_list_without_new_data_then_no_data_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand, list_without_new_data: list):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(list_without_new_data))
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(list_without_new_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_completed_with = CompletedWith.NoData()
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert state["actual_completed_with"] == expected_completed_with



async def test_when_pass_dict_without_new_data_then_no_data_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand, dict_without_new_data: dict):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(dict_without_new_data))
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(dict_without_new_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_completed_with = CompletedWith.NoData()
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert state["actual_completed_with"] == expected_completed_with



async def test_when_pass_no_data_then_no_data_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.NoData()
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert type(state["actual_completed_with"]) is CompletedWith.NoData



async def test_when_pass_empty_list_data_then_error_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    empty_list_data = []
    completed_result = CompletedWith.Data(empty_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert type(state["actual_completed_with"]) is CompletedWith.Error



async def test_when_pass_empty_dict_data_then_error_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    empty_dict_data = {}
    completed_result = CompletedWith.Data(empty_dict_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert type(state["actual_completed_with"]) is CompletedWith.Error



async def test_when_pass_none_data_then_error_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    none_data = None
    completed_result = CompletedWith.Data(none_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert type(state["actual_completed_with"]) is CompletedWith.Error



async def test_when_pass_str_data_then_error_is_passed_to_fetch_new_data_completed_handler(executing_task: RunTaskCommand):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    str_data = "string data"
    completed_result = CompletedWith.Data(str_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert type(state["actual_completed_with"]) is CompletedWith.Error



async def test_when_pass_wrong_fetch_id_then_returns_invalid_value_result(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = FetchIdValue.new_id()
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.error) is ValueInvalid



async def test_when_pass_wrong_completed_data_task_id_then_returns_invalid_value_result(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(TaskIdValue.new_id(), executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.error) is ValueInvalid



async def test_when_pass_wrong_completed_data_run_id_then_returns_invalid_value_result(executing_task: RunTaskCommand, new_dict_data: dict, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_dict_data)
    completed_data = CompletedTaskData(executing_task.task_id, RunIdValue.new_id(), completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.error) is ValueInvalid



async def test_when_pass_wrong_fetch_id_then_fetch_new_data_completed_handler_not_called(executing_task: RunTaskCommand, new_dict_data: dict):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = FetchIdValue.new_id()
    completed_result = CompletedWith.Data(new_dict_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert "actual_completed_with" not in state



async def test_when_pass_wrong_completed_data_task_id_then_fetch_new_data_completed_handler_not_called(executing_task: RunTaskCommand, new_dict_data: dict):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_dict_data)
    completed_data = CompletedTaskData(TaskIdValue.new_id(), executing_task.run_id, completed_result)
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert "actual_completed_with" not in state



async def test_when_pass_wrong_completed_data_run_id_then_fetch_new_data_completed_handler_not_called(executing_task: RunTaskCommand, new_list_data: list):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, RunIdValue.new_id(), completed_result)
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert "actual_completed_with" not in state



async def test_when_fetch_new_data_completed_handler_returns_error_then_returns_same_error(executing_task: RunTaskCommand, new_list_data: list):
    expected_error = "expected error"
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        return Result.Error(expected_error)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert new_data_res.error == expected_error



async def test_when_returns_success_result_then_fetch_id_is_removed_from_executing_tasks_storage(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_fetch_id_data = None
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)
    actual_fetch_id_data = await executing_tasks_storage.get(fetch_id)

    assert type(new_data_res) is Result
    assert new_data_res.is_ok()
    assert actual_fetch_id_data == expected_fetch_id_data



async def test_when_returns_failure_result_then_fetch_id_remains_in_executing_tasks_storage(executing_task: RunTaskCommand, new_list_data: list):
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        return Result.Error("expected error")
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    new_data_res = await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)
    actual_fetch_id_data = await executing_tasks_storage.get(fetch_id)

    assert type(new_data_res) is Result
    assert new_data_res.is_error()
    assert type(actual_fetch_id_data) is ExecutingTaskData



async def test_when_returns_invalid_value_result_then_fetch_id_remains_in_executing_tasks_storage(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(TaskIdValue.new_id(), RunIdValue.new_id(), completed_result)
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)
    actual_fetch_id_data = await executing_tasks_storage.get(fetch_id)

    assert type(new_data_res) is Result
    assert type(new_data_res.error) is ValueInvalid
    assert type(actual_fetch_id_data) is ExecutingTaskData



async def test_when_pass_wrong_completed_data_task_id_then_fetch_id_remains_in_executing_tasks_storage(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(TaskIdValue.new_id(), executing_task.run_id, completed_result)
    
    await handle_completed_task(completed_handler, fetch_id, completed_data)
    actual_fetch_id_data = await executing_tasks_storage.get(fetch_id)

    assert type(actual_fetch_id_data) is ExecutingTaskData



async def test_when_pass_wrong_completed_data_run_id_then_fetch_id_remains_in_executing_tasks_storage(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, RunIdValue.new_id(), completed_result)
    
    await handle_completed_task(completed_handler, fetch_id, completed_data)
    actual_fetch_id_data = await executing_tasks_storage.get(fetch_id)

    assert type(actual_fetch_id_data) is ExecutingTaskData



async def test_when_pass_wrong_data_then_fetch_id_remains_in_executing_tasks_storage(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = None
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result) # type: ignore
    
    await handle_completed_task(completed_handler, fetch_id, completed_data)
    actual_fetch_id_data = await executing_tasks_storage.get(fetch_id)

    assert type(actual_fetch_id_data) is ExecutingTaskData



async def test_when_returns_success_result_then_prev_data_is_updated_with_completed_result(executing_task: RunTaskCommand, list_without_new_data: list, new_list_data: list, completed_handler):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.Data(list_without_new_data))
    fetch_id = executing_task.fetch_id
    list_with_new_data = list_without_new_data + new_list_data
    completed_result = CompletedWith.Data(list_with_new_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_prev_data = completed_result
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)
    actual_prev_data = await previous_data_storage.get(executing_task.task_id)

    assert type(new_data_res) is Result
    assert new_data_res.is_ok()
    assert actual_prev_data == expected_prev_data



async def test_when_returns_failure_result_then_prev_data_remains_unchanged(executing_task: RunTaskCommand, new_list_data: list):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.NoData())
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        return Result.Error("expected error")
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_prev_data = CompletedWith.NoData()
    
    new_data_res = await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)
    actual_prev_data = await previous_data_storage.get(executing_task.task_id)

    assert type(new_data_res) is Result
    assert new_data_res.is_error()
    assert actual_prev_data == expected_prev_data



async def test_when_returns_invalid_value_result_then_prev_data_remains_unchanged(executing_task: RunTaskCommand, new_list_data: list):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.NoData())
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        return Result.Error("expected error")
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, RunIdValue.new_id(), completed_result)
    expected_prev_data = CompletedWith.NoData()
    
    new_data_res = await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)
    actual_prev_data = await previous_data_storage.get(executing_task.task_id)

    assert type(new_data_res) is Result
    assert type(new_data_res.error) is ValueInvalid
    assert actual_prev_data == expected_prev_data



async def test_when_call_twice_with_same_parameters_then_returns_success_result_then_returns_invalid_value_result(executing_task: RunTaskCommand, new_list_data: list, completed_handler):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    
    first_new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)
    second_new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(first_new_data_res) is Result
    assert first_new_data_res.is_ok()
    assert type(second_new_data_res) is Result
    assert type(second_new_data_res.error) is ValueInvalid



async def test_when_executing_tasks_storage_throws_exception_then_fetch_new_data_completed_handler_not_called(executing_task: RunTaskCommand, new_list_data: list, set_executing_tasks_storage_error):
    state = {}
    async def fetch_new_data_completed_handler(fetch_cmd: FetchNewDataCommand, completed_with: CompletedResult):
        state["actual_completed_with"] = completed_with
        return Result.Ok(None)
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    set_executing_tasks_storage_error(RuntimeError("Executing tasks storage error"))
    
    await handle_completed_task(fetch_new_data_completed_handler, fetch_id, completed_data)

    assert "actual_completed_with" not in state



async def test_when_executing_tasks_storage_throws_exception_then_returns_error_result(executing_task: RunTaskCommand, new_list_data: list, completed_handler, set_executing_tasks_storage_error):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    expected_error_message = "Executing tasks storage error"
    set_executing_tasks_storage_error(RuntimeError(expected_error_message))
    
    new_data_res = await handle_completed_task(completed_handler, fetch_id, completed_data)

    assert type(new_data_res) is Result
    assert type(new_data_res.error) is Error
    assert new_data_res.error.message == expected_error_message



async def test_when_executing_tasks_storage_throws_exception_then_fetch_id_remains_in_executing_tasks_storage(executing_task: RunTaskCommand, new_list_data: list, completed_handler, set_executing_tasks_storage_error, remove_executing_tasks_storage_error):
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    set_executing_tasks_storage_error(RuntimeError("Executing tasks storage error"))
    
    await handle_completed_task(completed_handler, fetch_id, completed_data)
    remove_executing_tasks_storage_error()
    actual_fetch_id_data = await executing_tasks_storage.get(fetch_id)

    assert type(actual_fetch_id_data) is ExecutingTaskData



async def test_when_executing_tasks_storage_throws_exception_then_prev_data_remains_unchanged(executing_task: RunTaskCommand, new_list_data: list, completed_handler, set_executing_tasks_storage_error):
    await previous_data_storage.set(executing_task.task_id, CompletedWith.NoData())
    fetch_id = executing_task.fetch_id
    completed_result = CompletedWith.Data(new_list_data)
    completed_data = CompletedTaskData(executing_task.task_id, executing_task.run_id, completed_result)
    set_executing_tasks_storage_error(RuntimeError("Executing tasks storage error"))
    expected_prev_data = CompletedWith.NoData()
    
    await handle_completed_task(completed_handler, fetch_id, completed_data)
    actual_prev_data = await previous_data_storage.get(executing_task.task_id)

    assert actual_prev_data == expected_prev_data

# executing_tasks_storage should have 18 items in total after all tests