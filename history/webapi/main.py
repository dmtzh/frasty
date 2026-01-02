from expression import Result
from fastapi import HTTPException

from shared.customtypes import RunIdValue, TaskIdValue
from shared.taskresulthistory import TaskResultHistoryItemAdapter
from shared.taskresultshistorystore import legacy_taskresultshistory_storage
from shared.utils.asyncresult import async_catch_ex
from shared.utils.result import ResultTag

from config import app

@app.get("/tasks/legacy/{id}/run/history/{run_id}")
async def get_result(id: str, run_id: str):
    opt_task_id = TaskIdValue.from_value_with_checksum(id)
    if opt_task_id is None:
        raise HTTPException(status_code=404)
    opt_run_id = RunIdValue.from_value_with_checksum(run_id)
    if opt_run_id is None:
        raise HTTPException(status_code=404)
    opt_history_item_res = await async_catch_ex(legacy_taskresultshistory_storage.get)(opt_task_id, opt_run_id)
    match opt_history_item_res:
        case Result(ResultTag.OK, ok=None):
            raise HTTPException(status_code=404)
        case Result(ResultTag.OK, ok=history_item):
            history_item_dto = TaskResultHistoryItemAdapter.to_dict(history_item)
            return history_item_dto
        case _:
            raise HTTPException(status_code=503, detail="Oops... Service temporary unavailable, please try again later.")
