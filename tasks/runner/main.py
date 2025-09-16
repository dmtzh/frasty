# import asyncio
import functools

from expression import Result
from faststream.rabbit.annotations import Logger

from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrundefinition as rabbit_definition
from infrastructure import rabbitruntask as rabbit_run_task
from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.repository import NotFoundError
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

from config import app, rabbit_client
import runtaskdefinitionhandler

@rabbit_run_task.handler(rabbit_client, rabbit_run_task.RunTaskData)
async def handle_run_task_definition_command(input, logger: Logger):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_definition_handler(data: rabbit_run_task.RunTaskData, definition_id: DefinitionIdValue):
        task_id_dict = {"task_id": data.task_id.to_value_with_checksum()}
        metadata = data.metadata | task_id_dict
        return rabbit_definition.run(rabbit_client, data.run_id, definition_id, metadata)
    
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    async def rabbit_runtask_failure_handler(data: rabbit_run_task.RunTaskData, error):
        definition_id = DefinitionIdValue(data.run_id)
        result = CompletedWith.Error(str(error))
        res = await rabbit_definition_completed.publish(rabbit_client, None, data.run_id, definition_id, result, data.metadata)
        return res.map(lambda _: result)
    
    match input:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is rabbit_run_task.RunTaskData:
            cmd = runtaskdefinitionhandler.RunTaskDefinitionCommand(data.task_id, data.run_id)
            run_definition_handler = functools.partial(rabbit_run_definition_handler, data)
            res = await runtaskdefinitionhandler.handle(run_definition_handler, cmd)
            match res:
                case Result(tag=ResultTag.ERROR, error=NotFoundError()):
                    return None
                case Result(tag=ResultTag.ERROR, error=error):
                    return await rabbit_runtask_failure_handler(cmd, error)
                case _:
                    return res
        case Result(tag=ResultTag.ERROR, error=error):
            # TODO: Handle error case
            logger.warning(f">>>> Received invalid RunTask command data: {error}")

# if __name__ == "__main__":
#     asyncio.run(app.run())