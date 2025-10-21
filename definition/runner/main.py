# import asyncio
from dataclasses import dataclass
import functools
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Error, DefinitionIdValue, RunIdValue, StepIdValue
from shared.domainrunning import RunningDefinitionState
from shared.infrastructure.storage.repository import NotFoundError
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import coroutine_result, async_result
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag

from config import app, complete_step_handler, publish_completed_definition, run_definition_handler, run_step
import completestephandler
import rundefinitionhandler

# ------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class RunDefinitionData:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    metadata: dict

@run_definition_handler(RunDefinitionData)
async def handle_run_definition_command(data: RunDefinitionData):
    def run_first_step_handler(evt: RunningDefinitionState.Events.StepRunning, definition_version: rundefinitionhandler.DefinitionVersion):
        definition_dict = {"definition_id": data.definition_id.to_value_with_checksum(), "definition_version": str(definition_version)}
        metadata = data.metadata | definition_dict
        return run_step(data.run_id, evt.step_id, evt.step_definition, evt.input_data, metadata)
    
    cmd = rundefinitionhandler.RunDefinitionCommand(data.run_id, data.definition_id)
    run_definition_res = await rundefinitionhandler.handle(run_first_step_handler, cmd)
    match run_definition_res:
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            return None
        case Result(tag=ResultTag.ERROR, error=error):
            error_result = CompletedWith.Error(str(error))
            publish_completed_definition_res = await publish_completed_definition(data.run_id, data.definition_id, error_result, data.metadata)
            return publish_completed_definition_res.map(lambda _: error_result)
        case _:
            return run_definition_res

# ------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class CompleteStepCommandValidationError:
    error: Any
@dataclass(frozen=True)
class CompleteStepHandlerError:
    cmd: completestephandler.CompleteStepCommand
    error: Any

@dataclass(frozen=True)
class CompleteStepData:
    run_id: RunIdValue
    step_id: StepIdValue
    result: CompletedResult
    metadata: dict

@complete_step_handler(CompleteStepData)
async def handle_complete_step_command(data: CompleteStepData): 
    @async_result
    @make_async
    def to_complete_step_command() -> Result[completestephandler.CompleteStepCommand, str]:
        return parse_from_dict(data.metadata, "definition_id", DefinitionIdValue.from_value_with_checksum)\
            .map(lambda definition_id: completestephandler.CompleteStepCommand(data.run_id, definition_id, data.step_id, data.result))
    def event_handler_with_def_id(definition_id: DefinitionIdValue, evt: RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted):
        match evt:
            case RunningDefinitionState.Events.DefinitionCompleted():
                return publish_completed_definition(data.run_id, definition_id, evt.result, data.metadata)
            case RunningDefinitionState.Events.StepRunning():
                return run_step(data.run_id, evt.step_id, evt.step_definition, evt.input_data, data.metadata)
            case _:
                async def error_res():
                    return Result.Error(Error(f"Unsupported event {evt}"))
                return error_res()
    @coroutine_result[CompleteStepCommandValidationError | CompleteStepHandlerError]()
    async def complete_step():
        cmd = await to_complete_step_command().map_error(CompleteStepCommandValidationError)
        event_handler = functools.partial(event_handler_with_def_id, cmd.definition_id)
        res = await async_result(completestephandler.handle)(event_handler, cmd)\
            .map_error(lambda err: CompleteStepHandlerError(cmd, err))
        return res
    
    complete_step_res = await complete_step()
    match complete_step_res:
        case Result(tag=ResultTag.ERROR, error=CompleteStepCommandValidationError()):
            return None
        case Result(tag=ResultTag.ERROR, error=CompleteStepHandlerError(_, NotFoundError())):
            return None
        case Result(tag=ResultTag.ERROR, error=CompleteStepHandlerError(cmd, error)):
            error_result = CompletedWith.Error(str(error))
            publish_completed_definition_res = await publish_completed_definition(cmd.run_id, cmd.definition_id, error_result, data.metadata)
            return publish_completed_definition_res.map(lambda _: error_result)
        case _:
            return complete_step_res

# ------------------------------------------------------------------------------------------------------------

# if __name__ == "__main__":
#     asyncio.run(app.run())