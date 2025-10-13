import datetime

from shared.customtypes import RunIdValue
from shared.infrastructure.storage.repository import StorageError
from shared.taskpendingresultsqueue import CompletedTaskData, TaskPendingResultsQueue, TaskPendingResultsQueueItem
from shared.taskpendingresultsqueuestore import taskpendingresultsqueue_storage
from shared.taskresulthistory import TaskResultHistoryItem
from shared.taskresultshistorystore import taskresultshistory_storage
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

class AddTaskResultToHistoryHandlerStorageError(StorageError):
    '''Unexpected add task result to history handler storage error'''

@async_result
@async_ex_to_error_result(AddTaskResultToHistoryHandlerStorageError.from_exception)
@taskpendingresultsqueue_storage.with_storage
def apply_put_to_pending_results_queue(queue: TaskPendingResultsQueue | None, data: CompletedTaskData):
    queue = queue or TaskPendingResultsQueue()
    queue.enqueue(data)
    return queue.peek(), queue

@async_result
@async_ex_to_error_result(AddTaskResultToHistoryHandlerStorageError.from_exception)
@taskpendingresultsqueue_storage.with_storage
def apply_get_next_from_pending_results_queue(queue: TaskPendingResultsQueue | None, run_id: RunIdValue):
    queue = queue or TaskPendingResultsQueue()
    match queue.peek():
        case None:
            return None, queue
        case pending_result if pending_result.data.run_id == run_id:
            queue.dequeue()
            return queue.peek(), queue
        case _:
            return queue.peek(), queue

@async_result
@async_ex_to_error_result(AddTaskResultToHistoryHandlerStorageError.from_exception)
@taskresultshistory_storage.with_storage
def apply_add_result_to_history(history_item: TaskResultHistoryItem | None, queue_item: TaskPendingResultsQueueItem):
    timestamp = int(datetime.datetime.now().timestamp())
    data = queue_item.data
    queue_item.prev_run_id
    history_item = TaskResultHistoryItem(data.result, timestamp, data.opt_definition_version, queue_item.prev_run_id)
    return (history_item, history_item)

@coroutine_result()
async def handle(data: CompletedTaskData) -> list[TaskResultHistoryItem]:
    next_pending_result = await apply_put_to_pending_results_queue(data.task_id, data)
    added_history_items = []
    while next_pending_result is not None:
        history_item = await apply_add_result_to_history(next_pending_result.data.task_id, next_pending_result.data.run_id, next_pending_result)
        added_history_items.append(history_item)
        next_pending_result = await apply_get_next_from_pending_results_queue(next_pending_result.data.task_id, next_pending_result.data.run_id)
    return added_history_items