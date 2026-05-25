from collections.abc import Callable, Coroutine
from typing import Any, Concatenate, NamedTuple

from expression import Result

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Error, RunIdValue, StepIdValue
from shared.definitioncustomtypes import GroupIdValue
from shared.groupofrunningdefinitions import GroupOfRunningDefinitionsState
from shared.infrastructure.storage.repository import NotFoundException, StorageError
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

type ToStorageActionConverter[**P] = Callable[[Callable[Concatenate[GroupOfRunningDefinitionsState | None, P], tuple[GroupOfRunningDefinitionsState.Events.Event | None, GroupOfRunningDefinitionsState]]], Callable[Concatenate[RunIdValue, GroupIdValue, P], Coroutine[Any, Any, GroupOfRunningDefinitionsState.Events.Event | None]]]

class CompleteGroupDefinitionCommand(NamedTuple):
    run_id: RunIdValue
    group_id: GroupIdValue
    step_id: StepIdValue
    definition_id: DefinitionIdValue
    result: CompletedResult

async def handle(
    convert_to_storage_action: ToStorageActionConverter,
    event_handler: Callable[[GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted], Coroutine[Any, Any, Result]],
    cmd: CompleteGroupDefinitionCommand
):
    @async_ex_to_error_result(StorageError.from_exception)
    @convert_to_storage_action
    def apply_fail_event_handler(state: GroupOfRunningDefinitionsState | None, error: Any):
        if state is None:
            raise NotFoundException()
        evt = state.apply_command(GroupOfRunningDefinitionsState.Commands.Fail(Error.from_error(error)))
        return (evt, state)
    def map_errors(error: CompleteGroupDefinitionStorageError | _EventHandlerError):
        match error:
            case _EventHandlerError(GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted(), error=evt_error):
                return CompletAllDefinitionsError(evt_error)
            case _:
                return error
    
    res = await _complete_group_definition_workflow(
        convert_to_storage_action,
        event_handler,
        cmd
    )
    opt_error = res.swap().default_value(None)
    match opt_error:
        case _EventHandlerError(event=GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted(), error=evt_error):
            await apply_fail_event_handler(cmd.run_id, cmd.group_id, evt_error)
    return res.map_error(map_errors)

class CompleteGroupDefinitionStorageError(StorageError):
    '''Unexpected complete group definition storage error'''

class CompletAllDefinitionsError(NamedTuple):
    error: Any

class _EventHandlerError(NamedTuple):
    event: GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted
    error: Any

@coroutine_result[CompleteGroupDefinitionStorageError | _EventHandlerError]()
async def _complete_group_definition_workflow(
    convert_to_storage_action: ToStorageActionConverter,
    event_handler: Callable[[GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted], Coroutine[Any, Any, Result]],
    cmd: CompleteGroupDefinitionCommand
):
    @async_result
    @async_ex_to_error_result(CompleteGroupDefinitionStorageError.from_exception)
    @async_ex_to_error_result(lambda _: CompleteGroupDefinitionStorageError(f"State not found for run_id {cmd.run_id} and group_id {cmd.group_id}"), NotFoundException)
    @convert_to_storage_action
    def apply_complete_definition(state: GroupOfRunningDefinitionsState | None):
        if state is None:
            raise NotFoundException()
        complete_def_cmd = GroupOfRunningDefinitionsState.Commands.CompleteDefinition(cmd.step_id, cmd.definition_id, cmd.result)
        evt = state.apply_command(complete_def_cmd)
        return (evt, state)
            
    opt_evt = await apply_complete_definition(cmd.run_id, cmd.group_id)
    match opt_evt:
        case GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted() as all_defs_completed:
            await async_result(event_handler)(all_defs_completed).map_error(lambda err: _EventHandlerError(all_defs_completed, err))
    return opt_evt