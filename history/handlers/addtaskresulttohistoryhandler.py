from dataclasses import dataclass
import datetime

from shared.completedresult import CompletedResult
from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.taskresulthistory import DefinitionVersion, TaskResultHistoryItem
from shared.taskresultshistorystore import taskresultshistory_storage
from shared.utils.asyncresult import async_ex_to_error_result

@dataclass(frozen=True)
class CompletedTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    result: CompletedResult
    opt_definition_version: DefinitionVersion | None

@async_ex_to_error_result(StorageError.from_exception)
@taskresultshistory_storage.with_storage
def apply_add_result_to_history(history_item: TaskResultHistoryItem | None, data: CompletedTaskData, timestamp: int):
    history_item = TaskResultHistoryItem(data.result, timestamp, data.opt_definition_version)
    return (history_item, history_item)

def handle(data: CompletedTaskData):
    timestamp = int(datetime.datetime.now().timestamp())
    return apply_add_result_to_history(data.task_id, data.run_id, data, timestamp)