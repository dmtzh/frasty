from dataclasses import dataclass

from shared.completedresult import CompletedResult
from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.utils.asyncresult import async_ex_to_error_result

from webapitaskrunstate import WebApiTaskRunState
from webapitaskrunstore import web_api_task_run_storage

@dataclass(frozen=True)
class CompleteRunStateCommand:
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedResult

@async_ex_to_error_result(StorageError.from_exception)
@async_ex_to_error_result(NotFoundError.from_exception, NotFoundException)
@web_api_task_run_storage.with_storage
def apply_complete_with_result(state: WebApiTaskRunState | None, result: CompletedResult):
    if state is None:
        raise NotFoundException()
    new_state = state.complete(result)
    return (new_state, new_state)

def handle(cmd: CompleteRunStateCommand):
    return apply_complete_with_result(cmd.task_id, cmd.run_id, cmd.result)
