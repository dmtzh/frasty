from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import datetime
from typing import Any

from expression import Result

from shared.customtypes import RunIdValue, TaskIdValue, StepIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.utils.asyncresult import AsyncResult, coroutine_result, async_result, async_ex_to_error_result, async_catch_ex
from shared.utils.parse import parse_from_str
from shared.validation import ValueInvalid

from executingtasksstore import executing_tasks_storage, ExecutingTaskData
from fetchidvalue import FetchIdValue

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
