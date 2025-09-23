import asyncio
import functools
from expression import Result
from faststream.rabbit.annotations import Logger

from infrastructure import rabbitrunstep as rabbit_run_step
from infrastructure import rabbitruntask as rabbit_task
from shared.completedresult import CompletedWith
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from config import app, rabbit_client
import fetchnewdatahandler

class RabbitFetchNewDataCommand(rabbit_run_step.RunStepData[None, FetchNewDataInput]):
    '''Input data for fetch new data command'''

@rabbit_run_step.handler(rabbit_client, FetchNewData, FetchNewDataInput.from_dict, RabbitFetchNewDataCommand)
async def handle_fetch_new_data_command(input, logger: Logger):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_task_handler(parent_metadata: dict, cmd: fetchnewdatahandler.RunTaskCommand):
        # raise RuntimeError("rabbit_run_task_handler test runtime error")
        metadata = {
            "fetch_id": cmd.fetch_id.to_value_with_checksum(),
            "parent_metadata": parent_metadata
        }
        return rabbit_task.run(rabbit_client, cmd.task_id, cmd.run_id, "fetch_new_data_handler", metadata)
    match input:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is RabbitFetchNewDataCommand:
            fetch_new_data_cmd = fetchnewdatahandler.FetchNewDataCommand(fetch_task_id=data.data.task_id, run_id=data.run_id, step_id=data.step_id)
            run_task_handler = functools.partial(rabbit_run_task_handler, data.metadata)
            fetch_new_data_res = await fetchnewdatahandler.handle(run_task_handler, fetch_new_data_cmd)
            match fetch_new_data_res:
                case Result(tag=ResultTag.ERROR, error=error):
                    return CompletedWith.Error(str(error))
                case _:
                    return None
        case Result(tag=ResultTag.ERROR, error=error):
            # TODO: Handle error case
            cmd_name = FetchNewData.__name__
            logger.warning(f">>>> Received invalid {cmd_name} command data: {error}")

if __name__ == "__main__":
    asyncio.run(app.run())