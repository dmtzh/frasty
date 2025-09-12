# import asyncio

from expression import Result
from faststream.rabbit.annotations import Logger

from config import app, rabbit_client
from infrastructure import rabbitcompletestep as rabbit_complete_step
from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrundefinition as rabbit_run_definition
from infrastructure import rabbitrunstep as rabbit_step
from shared.completedresult import CompletedWith
from shared.customtypes import Error
from shared.domainrunning import RunningDefinitionState
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

import completestephandler
import rundefinitionhandler

@rabbit_run_definition.handler(rabbit_client, rundefinitionhandler.RunDefinitionCommand)
async def handle_run_definition_command(input, logger: Logger):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_first_step_handler(cmd: rundefinitionhandler.RunDefinitionCommand, evt: RunningDefinitionState.Events.StepRunning):
        return rabbit_step.run(rabbit_client, None, cmd.run_id, cmd.definition_id, evt.step_id, evt.step_definition, evt.input_data, cmd.metadata)
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_rundefinition_failure_handler(cmd: rundefinitionhandler.RunDefinitionCommand, error):
        result = CompletedWith.Error(str(error))
        return rabbit_definition_completed.publish(rabbit_client, None, cmd.run_id, cmd.definition_id, result, cmd.metadata)
    match input:
        case Result(tag=ResultTag.OK, ok=cmd) if type(cmd) is rundefinitionhandler.RunDefinitionCommand:
            res = await rundefinitionhandler.handle(rabbit_run_first_step_handler, cmd)
            match res:
                case Result(tag=ResultTag.ERROR, error=error):
                    res = await rabbit_rundefinition_failure_handler(cmd, error)
            return res
        case Result(tag=ResultTag.ERROR, error=error):
            # TODO: Handle error case
            cmd_name = rundefinitionhandler.RunDefinitionCommand.__name__
            logger.warning(f">>>> Received invalid {cmd_name} command data: {error}")

@rabbit_complete_step.handler(rabbit_client, completestephandler.CompleteStepCommand)
async def handle_complete_step_command(input):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def event_handler(cmd: completestephandler.CompleteStepCommand, evt: RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted):
        match evt:
            case RunningDefinitionState.Events.DefinitionCompleted():
                return rabbit_definition_completed.publish(rabbit_client, None, cmd.run_id, cmd.definition_id, evt.result, cmd.metadata)
            case RunningDefinitionState.Events.StepRunning():
                return rabbit_step.run(rabbit_client, None, cmd.run_id, cmd.definition_id, evt.step_id, evt.step_definition, evt.input_data, cmd.metadata)
            case _:
                async def error_res():
                    return Result.Error(Error(f"Unsupported event {evt}"))
                return error_res()
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def completestep_failure_handler(cmd: completestephandler.CompleteStepCommand, error):
        result = CompletedWith.Error(str(error))
        return rabbit_definition_completed.publish(rabbit_client, None, cmd.run_id, cmd.definition_id, result, cmd.metadata)
    match input:
        case Result(tag=ResultTag.OK, ok=cmd) if type(cmd) is completestephandler.CompleteStepCommand:
            res = await completestephandler.handle(event_handler, cmd)
            match res:
                case Result(tag=ResultTag.ERROR, error=error):
                    res = await completestep_failure_handler(cmd, error)
            return res

# if __name__ == "__main__":
#     asyncio.run(app.run())