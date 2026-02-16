# import asyncio

import datetime
from shared.completedresult import CompletedResult, CompletedResultAdapter, CompletedWith
from shared.infrastructure.storage.repository import StorageError
from shared.pipeline.actionhandler import ActionData
from shared.taskresulthistory import TaskResultHistoryItem
from shared.taskresultshistorystore import taskresultshistory_storage

from config import AddTaskResultToHistoryConfig, add_task_result_to_history_handler, app
from shared.utils.asyncresult import async_ex_to_error_result

@add_task_result_to_history_handler
async def handle_add_task_result_to_history(data: ActionData[AddTaskResultToHistoryConfig, CompletedResult]):
    @async_ex_to_error_result(StorageError.from_exception)
    @taskresultshistory_storage.with_storage
    def apply_add_result_to_history(history_item: TaskResultHistoryItem | None, config: AddTaskResultToHistoryConfig, result: CompletedResult):
        timestamp = int(datetime.datetime.now().timestamp())
        history_item = TaskResultHistoryItem(result, timestamp, config.execution_id, None)
        return (None, history_item)
    def ok_to_completed_result(_):
        data_dict = CompletedResultAdapter.to_dict(data.input)
        return CompletedWith.Data(data_dict)
    def err_to_completed_result(err):
        return CompletedWith.Error(str(err))
    
    task_id = data.config.task_id
    run_id = data.run_id
    add_res = await apply_add_result_to_history(task_id, run_id, data.config, data.input)
    return add_res.map(ok_to_completed_result).default_with(err_to_completed_result)

# if __name__ == "__main__":
#     asyncio.run(app.run())