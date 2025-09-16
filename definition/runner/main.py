# import asyncio
import functools

from expression import Result
from faststream.rabbit.annotations import Logger

from config import app, rabbit_client
from infrastructure import rabbitcompletestep as rabbit_complete_step
from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrundefinition as rabbit_run_definition
from infrastructure import rabbitrunstep as rabbit_step
from shared.completedresult import CompletedWith
from shared.customtypes import Error, DefinitionIdValue
from shared.domainrunning import RunningDefinitionState
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.repository import NotFoundError
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

import completestephandler
import rundefinitionhandler

@rabbit_run_definition.handler(rabbit_client, rabbit_run_definition.RunDefinitionData)
async def handle_run_definition_command(input, logger: Logger):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_first_step_handler(data: rabbit_run_definition.RunDefinitionData, evt: RunningDefinitionState.Events.StepRunning, definition_version: rundefinitionhandler.DefinitionVersion):
        definition_dict = {"definition_id": data.definition_id.to_value_with_checksum(), "definition_version": str(definition_version)}
        metadata = data.metadata | definition_dict
        return rabbit_step.run(rabbit_client, data.run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    async def rabbit_rundefinition_failure_handler(data: rabbit_run_definition.RunDefinitionData, error):
        result = CompletedWith.Error(str(error))
        res = await rabbit_definition_completed.publish(rabbit_client, None, data.run_id, data.definition_id, result, data.metadata)
        return res.map(lambda _: result)
    
    match input:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is rabbit_run_definition.RunDefinitionData:
            run_first_step_handler = functools.partial(rabbit_run_first_step_handler, data)
            cmd = rundefinitionhandler.RunDefinitionCommand(data.definition_id, data.run_id)
            res = await rundefinitionhandler.handle(run_first_step_handler, cmd)
            match res:
                case Result(tag=ResultTag.ERROR, error=NotFoundError()):
                    return None
                case Result(tag=ResultTag.ERROR, error=error):
                    return await rabbit_rundefinition_failure_handler(data, error)
                case _:
                    return res
        case Result(tag=ResultTag.ERROR, error=error):
            # TODO: Handle error case
            cmd_name = rundefinitionhandler.RunDefinitionCommand.__name__
            logger.warning(f">>>> Received invalid {cmd_name} command data: {error}")

@rabbit_complete_step.handler(rabbit_client, rabbit_complete_step.CompleteStepData)
async def handle_complete_step_command(input): 
    def from_complete_step_data(data: rabbit_complete_step.CompleteStepData) -> Result[tuple[completestephandler.CompleteStepCommand, dict], str]:
        opt_definition_id = DefinitionIdValue.from_value_with_checksum(data.metadata.get("definition_id", ""))
        match opt_definition_id:
            case None:
                return Result.Error("Missing definition_id in metadata")
            case definition_id:
                res = (completestephandler.CompleteStepCommand(data.run_id, definition_id, data.step_id, data.result), data.metadata)
                return Result.Ok(res)
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_event_handler(cmd: completestephandler.CompleteStepCommand, metadata: dict, evt: RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted):
        match evt:
            case RunningDefinitionState.Events.DefinitionCompleted():
                return rabbit_definition_completed.publish(rabbit_client, None, cmd.run_id, cmd.definition_id, evt.result, metadata)
            case RunningDefinitionState.Events.StepRunning():
                return rabbit_step.run(rabbit_client, cmd.run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)
            case _:
                async def error_res():
                    return Result.Error(Error(f"Unsupported event {evt}"))
                return error_res()
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    async def completestep_failure_handler(cmd: completestephandler.CompleteStepCommand, metadata: dict, error):
        result = CompletedWith.Error(str(error))
        res = await rabbit_definition_completed.publish(rabbit_client, None, cmd.run_id, cmd.definition_id, result, metadata)
        return res.map(lambda _: result)

    complete_step_cmd_with_metadata_res = input.bind(from_complete_step_data)
    match complete_step_cmd_with_metadata_res:
        case Result(tag=ResultTag.OK, ok=(cmd, metadata)) if type(cmd) is completestephandler.CompleteStepCommand and type(metadata) is dict:
            event_handler = functools.partial(rabbit_event_handler, cmd, metadata)
            res = await completestephandler.handle(event_handler, cmd)
            match res:
                case Result(tag=ResultTag.ERROR, error=NotFoundError()):
                    return None
                case Result(tag=ResultTag.ERROR, error=error):
                    return await completestep_failure_handler(cmd, metadata, error)
                case _:
                    return res

# if __name__ == "__main__":
#     asyncio.run(app.run())