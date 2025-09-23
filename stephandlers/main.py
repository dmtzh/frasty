# import asyncio
from expression import Result
from faststream.rabbit.annotations import Logger

from infrastructure import rabbitrunstep as rabbit_run_step
from shared.utils.result import ResultTag
from stepdefinitions.task import FetchNewData, FetchNewDataInput

from config import app, rabbit_client

class RabbitFetchNewDataCommand(rabbit_run_step.RunStepData[None, FetchNewDataInput]):
    '''Input data for fetch new data command'''

@rabbit_run_step.handler(rabbit_client, FetchNewData, FetchNewDataInput.from_dict, RabbitFetchNewDataCommand)
async def handle_fetch_new_data_command(input, logger: Logger):
    match input:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is RabbitFetchNewDataCommand:
            cmd_name = FetchNewData.__name__
            logger.info(f">>>> {cmd_name} received input {data}")
        case Result(tag=ResultTag.ERROR, error=error):
            # TODO: Handle error case
            cmd_name = FetchNewData.__name__
            logger.warning(f">>>> Received invalid {cmd_name} command data: {error}")

# if __name__ == "__main__":
#     asyncio.run(app.run())