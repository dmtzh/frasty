# import asyncio
from dataclasses import dataclass
import functools
from typing import Any

from expression import Result

from infrastructure import rabbitcompletestep as rabbit_complete_step
from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrundefinition as rabbit_run_definition
from infrastructure import rabbitrunstep as rabbit_step
from shared.completedresult import CompletedWith
from shared.customtypes import Error, DefinitionIdValue
from shared.domainrunning import RunningDefinitionState
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.repository import NotFoundError
from shared.utils.asyncresult import async_ex_to_error_result, AsyncResult, coroutine_result, async_result
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag

from config import app, rabbit_client
import completestephandler
import rundefinitionhandler

@dataclass(frozen=True)
class RunDefinitionCommandValidationError:
    error: Any
@dataclass(frozen=True)
class RunDefinitionHandlerError:
    data: rabbit_run_definition.RunDefinitionData
    error: Any

@rabbit_run_definition.handler(rabbit_client, rabbit_run_definition.RunDefinitionData)
async def handle_run_definition_command(input):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_first_step_handler(data: rabbit_run_definition.RunDefinitionData, evt: RunningDefinitionState.Events.StepRunning, definition_version: rundefinitionhandler.DefinitionVersion):
        definition_dict = {"definition_id": data.definition_id.to_value_with_checksum(), "definition_version": str(definition_version)}
        metadata = data.metadata | definition_dict
        return rabbit_step.run(rabbit_client, data.run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)
    @coroutine_result[RunDefinitionCommandValidationError | RunDefinitionHandlerError]()
    async def run_definition(input: Result[rabbit_run_definition.RunDefinitionData, Any]):
        data = await AsyncResult.from_result(input).map_error(RunDefinitionCommandValidationError)
        run_first_step_handler = functools.partial(rabbit_run_first_step_handler, data)
        cmd = rundefinitionhandler.RunDefinitionCommand(data.run_id, data.definition_id)
        res = await async_result(rundefinitionhandler.handle)(run_first_step_handler, cmd)\
            .map_error(lambda err: RunDefinitionHandlerError(data, err))
        return res
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    async def rabbit_rundefinition_failure_handler(data: rabbit_run_definition.RunDefinitionData, error):
        result = CompletedWith.Error(str(error))
        res = await rabbit_definition_completed.publish(rabbit_client, data.run_id, data.definition_id, result, data.metadata)
        return res.map(lambda _: result)

    run_definition_res = await run_definition(input)
    match run_definition_res:
        case Result(tag=ResultTag.ERROR, error=RunDefinitionCommandValidationError()):
            return None
        case Result(tag=ResultTag.ERROR, error=RunDefinitionHandlerError(_, NotFoundError())):
            return None
        case Result(tag=ResultTag.ERROR, error=RunDefinitionHandlerError(data, error)):
            return await rabbit_rundefinition_failure_handler(data, error)
        case _:
            return run_definition_res

@dataclass(frozen=True)
class CompleteStepCommandValidationError:
    error: Any
@dataclass(frozen=True)
class CompleteStepHandlerError:
    cmd: completestephandler.CompleteStepCommand
    metadata: dict
    error: Any

@rabbit_complete_step.handler(rabbit_client, rabbit_complete_step.CompleteStepData)
async def handle_complete_step_command(input): 
    def from_complete_step_data(data: rabbit_complete_step.CompleteStepData) -> Result[tuple[completestephandler.CompleteStepCommand, dict], str]:
        return parse_from_dict(data.metadata, "definition_id", DefinitionIdValue.from_value_with_checksum)\
            .map(lambda definition_id: (completestephandler.CompleteStepCommand(data.run_id, definition_id, data.step_id, data.result), data.metadata))
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_event_handler(cmd: completestephandler.CompleteStepCommand, metadata: dict, evt: RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted):
        match evt:
            case RunningDefinitionState.Events.DefinitionCompleted():
                return rabbit_definition_completed.publish(rabbit_client, cmd.run_id, cmd.definition_id, evt.result, metadata)
            case RunningDefinitionState.Events.StepRunning():
                return rabbit_step.run(rabbit_client, cmd.run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)
            case _:
                async def error_res():
                    return Result.Error(Error(f"Unsupported event {evt}"))
                return error_res()
    @coroutine_result[CompleteStepCommandValidationError | CompleteStepHandlerError]()
    async def complete_step(input: Result[rabbit_complete_step.CompleteStepData, Any]):
        cmd, metadata = await AsyncResult.from_result(input.bind(from_complete_step_data))\
            .map_error(CompleteStepCommandValidationError)
        event_handler = functools.partial(rabbit_event_handler, cmd, metadata)
        res = await async_result(completestephandler.handle)(event_handler, cmd)\
            .map_error(lambda err: CompleteStepHandlerError(cmd, metadata, err))
        return res
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    async def completestep_failure_handler(cmd: completestephandler.CompleteStepCommand, metadata: dict, error):
        result = CompletedWith.Error(str(error))
        res = await rabbit_definition_completed.publish(rabbit_client, cmd.run_id, cmd.definition_id, result, metadata)
        return res.map(lambda _: result)
    
    complete_step_res = await complete_step(input)
    match complete_step_res:
        case Result(tag=ResultTag.ERROR, error=CompleteStepCommandValidationError()):
            return None
        case Result(tag=ResultTag.ERROR, error=CompleteStepHandlerError(_, _, NotFoundError())):
            return None
        case Result(tag=ResultTag.ERROR, error=CompleteStepHandlerError(cmd, metadata, error)):
            return await completestep_failure_handler(cmd, metadata, error)
        case _:
            return complete_step_res

# if __name__ == "__main__":
#     asyncio.run(app.run())