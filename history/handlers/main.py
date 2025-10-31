# import asyncio
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import Result, effect

from shared.customtypes import TaskIdValue
from shared.definitioncompleteddata import DefinitionCompletedData
from shared.taskpendingresultsqueue import DefinitionVersion, CompletedTaskData
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import async_result, coroutine_result
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag

import addtaskresulttohistoryhandler
from config import app, definition_completed_subscriber

@dataclass(frozen=True)
class DataValidationError:
    error: str

@definition_completed_subscriber(DefinitionCompletedData)
async def add_task_result_to_history(data: DefinitionCompletedData):
    @async_result
    @make_async
    @effect.result[CompletedTaskData, str]()
    def to_completed_task_data() -> Generator[Any, Any, CompletedTaskData]:
        task_id = yield from parse_from_dict(data.metadata, "task_id", TaskIdValue.from_value_with_checksum)
        raw_definition_version = data.metadata.get("definition_version")
        opt_definition_version = DefinitionVersion.parse(raw_definition_version)
        return CompletedTaskData(task_id, data.run_id, data.result, opt_definition_version)
    @coroutine_result()
    async def add_to_history():
        completed_task_data = await to_completed_task_data().map_error(DataValidationError)
        history_items = await async_result(addtaskresulttohistoryhandler.handle)(completed_task_data)
        return history_items
    
    add_to_history_res = await add_to_history()
    match add_to_history_res:
        case Result(tag=ResultTag.ERROR, error=DataValidationError()):
            return None
        case _:
            return add_to_history_res

# if __name__ == "__main__":
#     asyncio.run(app.run())