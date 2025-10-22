# import asyncio
from dataclasses import dataclass

from expression import Result

from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, RunIdValue, TaskIdValue
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.repository import NotFoundError
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

from config import app, rabbit_client, run_definition, run_task_handler
import runtaskdefinitionhandler

@dataclass(frozen=True)
class RunTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: dict

@run_task_handler(RunTaskData)
async def handle_run_task_definition_command(data: RunTaskData):
    def run_definition_handler(definition_id: DefinitionIdValue):
        task_id_dict = {"task_id": data.task_id.to_value_with_checksum()}
        metadata = data.metadata | task_id_dict
        return run_definition(data.run_id, definition_id, metadata)
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    async def runtask_failure_handler(error):
        definition_id = DefinitionIdValue(data.run_id)
        result = CompletedWith.Error(str(error))
        res = await rabbit_definition_completed.publish(rabbit_client, data.run_id, definition_id, result, data.metadata)
        return res.map(lambda _: result)
    
    cmd = runtaskdefinitionhandler.RunTaskDefinitionCommand(data.task_id, data.run_id)
    res = await runtaskdefinitionhandler.handle(run_definition_handler, cmd)
    match res:
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            return None
        case Result(tag=ResultTag.ERROR, error=error):
            return await runtask_failure_handler(error)
        case _:
            return res

# if __name__ == "__main__":
#     asyncio.run(app.run())