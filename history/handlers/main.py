# import asyncio
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from expression import Result, effect

from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure.rabbitmiddlewares import RequeueChance
from shared.customtypes import TaskIdValue
from shared.utils.asyncresult import AsyncResult, async_result, coroutine_result
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag

import addtaskresulttohistoryhandler
from config import app, rabbit_client

@dataclass(frozen=True)
class CompletedTaskDataValidationError:
    error: Any

@rabbit_definition_completed.subscriber(rabbit_client, rabbit_definition_completed.DefinitionCompletedData, queue_name="pending_task_history_results", requeue_chance=RequeueChance.HIGH)
async def add_task_result_to_history(input):
    @effect.result[addtaskresulttohistoryhandler.CompletedTaskData, str]()
    def definition_to_completed_task(data: rabbit_definition_completed.DefinitionCompletedData) -> Generator[Any, Any, addtaskresulttohistoryhandler.CompletedTaskData]:
        task_id = yield from parse_from_dict(data.metadata, "task_id", TaskIdValue.from_value_with_checksum)
        raw_definition_version = data.metadata.get("definition_version")
        opt_definition_version = addtaskresulttohistoryhandler.DefinitionVersion.parse(raw_definition_version)
        return addtaskresulttohistoryhandler.CompletedTaskData(task_id, data.run_id, data.result, opt_definition_version)
    @coroutine_result()
    async def add_to_history(input: Result[rabbit_definition_completed.DefinitionCompletedData, Any]):
        completed_task_data = await AsyncResult.from_result(input.bind(definition_to_completed_task))\
            .map_error(CompletedTaskDataValidationError)
        return await async_result(addtaskresulttohistoryhandler.handle)(completed_task_data)
    
    add_to_history_res = await add_to_history(input)
    match add_to_history_res:
        case Result(tag=ResultTag.ERROR, error=CompletedTaskDataValidationError()):
            return None
        case _:
            return add_to_history_res

# if __name__ == "__main__":
#     asyncio.run(app.run())