from expression import Result
import pytest

from shared.customtypes import RunIdValue, StepIdValue, TaskIdValue
from stephandlers.fetchnewdata.fetchidvalue import FetchIdValue
from stephandlers.fetchnewdata.handler import FetchNewDataCommand, handle

@pytest.fixture
async def cmd1():
    return FetchNewDataCommand(TaskIdValue.new_id(), RunIdValue.new_id(), StepIdValue.new_id())

@pytest.fixture(scope="session")
def fetch_data_handler():
    async def handler(fetch_id: FetchIdValue):
        return Result.Ok(None)
    return handler



async def test_returns_fetch_id(cmd1: FetchNewDataCommand, fetch_data_handler):
    fetch_id_res = await handle(fetch_data_handler, cmd1)

    assert type(fetch_id_res) is Result
    assert type(fetch_id_res.ok) is FetchIdValue
