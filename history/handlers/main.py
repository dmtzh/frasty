# import asyncio

from expression import Result
from faststream.rabbit.annotations import Logger

from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from shared.customtypes import TaskIdValue
from shared.utils.result import ResultTag

import addtaskresulttohistoryhandler
from config import app, rabbit_client

def definition_to_completed_task(data: rabbit_definition_completed.DefinitionCompletedData) -> Result[addtaskresulttohistoryhandler.CompletedTaskData, str]:
    raw_task_id = data.metadata.get("task_id")
    if raw_task_id is None:
        return Result.Error("task_id not found in metadata")
    if not isinstance(raw_task_id, str):
        return Result.Error("task_id is not a string")
    opt_task_id = TaskIdValue.from_value_with_checksum(raw_task_id)
    raw_definition_version = data.metadata.get("definition_version")
    match raw_definition_version:
        case None:
            definition_version = None
        case int_def_version if isinstance(int_def_version, int):
            definition_version = addtaskresulttohistoryhandler.DefinitionVersion(int_def_version)
        case _:
            return Result.Error("definition_version is not an int")
    match opt_task_id:
        case None:
            return Result.Error("task_id is invalid")
        case task_id:
            res = addtaskresulttohistoryhandler.CompletedTaskData(task_id, data.run_id, data.result, definition_version)
            return Result.Ok(res)

@rabbit_definition_completed.subscriber(rabbit_client, rabbit_definition_completed.DefinitionCompletedData, queue_name="pending_task_history_results")
async def add_task_result_to_history(input, logger: Logger):
    logger.info(f"Add task result to history received input: {input}")
    completed_task_data_res = input.bind(definition_to_completed_task)
    match completed_task_data_res:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is addtaskresulttohistoryhandler.CompletedTaskData:
            res = await addtaskresulttohistoryhandler.handle(data)
            match res:
                case Result(tag=ResultTag.ERROR, error=_):
                    rabbit_definition_completed.handle_processing_failure(rabbit_definition_completed.Severity.HIGH)
            logger.info(f"Add task result to history completed with output {res}")
            return res

# if __name__ == "__main__":
#     asyncio.run(app.run())