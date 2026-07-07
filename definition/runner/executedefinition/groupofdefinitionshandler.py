import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import functools
from typing import Any, Concatenate

from expression import Result

from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, Error, Metadata, RunIdValue, StepIdValue
from shared.definition import Definition
from shared.definitioncustomtypes import GroupIdValue
from shared.executedefinitionaction import ExecuteDefinitionInput, run_execute_definition_action
from shared.groupofrunningdefinitions import DefinitionIdWithValue, GroupOfRunningDefinitionsState, RunningDefinition
from shared.infrastructure.storage.repository import NotFoundException, StorageError
from shared.pipeline.actionhandler import ActionData, RunAsyncAction
from shared.utils.asyncresult import AsyncResult
from shared.utils.exceptiondecorators import async_ex_to_error_result
from shared.utils.result import apply, to_error_list, to_ok_list

from runningparentaction import RunningParentAction

from .input import ExecuteGroupOfDefinitionsInput

type ToStorageActionConverter[**P] = Callable[[Callable[Concatenate[GroupOfRunningDefinitionsState | None, P], tuple[GroupOfRunningDefinitionsState.Events.Event | None, GroupOfRunningDefinitionsState]]], Callable[Concatenate[RunIdValue, GroupIdValue, P], Coroutine[Any, Any, GroupOfRunningDefinitionsState.Events.Event | None]]]

class RunGroupOfDefinitionsStorageError(StorageError):
    '''Unexpected run group of definitions storage error'''

class CompleteFailedDefinitionStorageError(StorageError):
    '''Unexpected complete failed definition storage error'''

async def handle(
    convert_to_storage_action: ToStorageActionConverter,
    run_action: RunAsyncAction,
    data: ActionData[None, ExecuteGroupOfDefinitionsInput]
) -> Result[GroupOfRunningDefinitionsState.Events.Event | None, RunGroupOfDefinitionsStorageError | list[CompleteFailedDefinitionStorageError]]:
    group_id = GroupIdValue(data.step_id)
    def generate_group_definition_metadata(definition_id: DefinitionIdValue):
        metadata = Metadata()
        metadata.set_from("execute group definition action")
        metadata.set_definition_id(definition_id)
        metadata.set_id("group_id", group_id)
        parent_action = RunningParentAction(data.run_id, data.step_id, data.metadata)
        parent_action.add_to_metadata(metadata)
        return metadata
    def run_group_definition_handler(step_id: StepIdValue, definition_id: DefinitionIdValue, definition: Definition):
        input = ExecuteDefinitionInput(definition_id, definition)
        metadata = generate_group_definition_metadata(definition_id)
        execute_definition_data = ActionData(data.run_id, step_id, None, input, metadata)
        return run_execute_definition_action(run_action, execute_definition_data)
    @async_ex_to_error_result(StorageError.from_exception)
    @convert_to_storage_action
    def apply_failed_run(state: GroupOfRunningDefinitionsState | None):
        if state is None:
            raise NotFoundException()
        evt = state.apply_command(GroupOfRunningDefinitionsState.Commands.Fail(Error("Group of definitions failed to run and should be removed")))
        return (evt, state)
    
    definitions = tuple(DefinitionIdWithValue(item.definition_id, item.definition) for item in data.input.items)
    cmd = _RunGroupOfDefinitionsCommand(data.run_id, group_id, definitions)
    res = await _run_group_of_definitions_workflow(
        convert_to_storage_action,
        run_group_definition_handler,
        cmd
    )
    opt_err = res.swap().default_value(None)
    match opt_err:
        case list():
            await apply_failed_run(cmd.run_id, cmd.group_id)
    return res
            

@dataclass(frozen=True)
class _RunGroupOfDefinitionsCommand:
    run_id: RunIdValue
    group_id: GroupIdValue
    definitions: tuple[DefinitionIdWithValue[Definition], ...]

@dataclass(frozen=True)
class _RunDefinitionError:
    step_id: StepIdValue
    definition_id: DefinitionIdValue
    error: Any

def _run_group_of_definitions_workflow(
    convert_to_storage_action: ToStorageActionConverter,
    run_definition_handler: Callable[[StepIdValue, DefinitionIdValue, Definition], Coroutine[Any, Any, Result]],
    cmd: _RunGroupOfDefinitionsCommand
):
    @async_ex_to_error_result(RunGroupOfDefinitionsStorageError.from_exception)
    @convert_to_storage_action
    def apply_run_group_of_definitions(state: GroupOfRunningDefinitionsState | None):
        def set_definitions_and_run():
            new_state = GroupOfRunningDefinitionsState()
            new_state.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(cmd.definitions))
            evt = new_state.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
            return (evt, new_state)
        match state:
            case None:
                return set_definitions_and_run()
            case _:
                return (None, state)
    async def run_definition_handler_wrapper(running_definition: RunningDefinition):
        res = await run_definition_handler(*running_definition)
        return res.map_error(lambda err: _RunDefinitionError(running_definition.step_id, running_definition.definition_id, err))
    async def run_definitions(opt_evt: GroupOfRunningDefinitionsState.Events.Event | None):
        initial_res = Result[GroupOfRunningDefinitionsState.Events.Event | None, tuple[_RunDefinitionError, ...]].Ok(opt_evt)
        match opt_evt:
            case GroupOfRunningDefinitionsState.Events.DefinitionsRunning() as evt:
                run_definition_handlers = map(run_definition_handler_wrapper, evt.definitions)
                run_definitions_results = await asyncio.gather(*run_definition_handlers)
                def reduce_func(r1, r2):
                    return apply(lambda acc, _: acc, lambda err: err, r1, r2)
                reduce_res = functools.reduce(reduce_func, run_definitions_results, initial_res)
                return reduce_res
            case _:
                return initial_res
    
    opt_evt_res = AsyncResult(apply_run_group_of_definitions(cmd.run_id, cmd.group_id))
    run_definitions_res = opt_evt_res.bind(run_definitions)
    complete_failed_definitions = functools.partial(_complete_failed_definitions, convert_to_storage_action, cmd)
    res = run_definitions_res.or_else(complete_failed_definitions)
    return res.to_coroutine()

async def _complete_failed_definitions(
    convert_to_storage_action: ToStorageActionConverter,
    cmd: _RunGroupOfDefinitionsCommand,
    failed_definitions: tuple[_RunDefinitionError, ...] | RunGroupOfDefinitionsStorageError
) -> Result[GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted | None, RunGroupOfDefinitionsStorageError | list[CompleteFailedDefinitionStorageError]]:
    @async_ex_to_error_result(CompleteFailedDefinitionStorageError.from_exception)
    @async_ex_to_error_result(lambda _: CompleteFailedDefinitionStorageError(f"State not found for run_id {cmd.run_id} and group_id {cmd.group_id}"), NotFoundException)
    @convert_to_storage_action
    def apply_complete_definition_with_error(state: GroupOfRunningDefinitionsState | None, step_id: StepIdValue, definition_id: DefinitionIdValue, err: Any):
        if state is None:
            raise NotFoundException()
        err_result = CompletedWith.Error(str(err))
        evt = state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(step_id, definition_id, err_result))
        return (evt, state)
    
    if isinstance(failed_definitions, RunGroupOfDefinitionsStorageError):
        return Result.Error(failed_definitions)
    # immediately complete failed to run definitions
    complete_definitions_with_errors_handlers = (apply_complete_definition_with_error(cmd.run_id, cmd.group_id, err.step_id, err.definition_id, err.error) for err in failed_definitions)
    complete_definitions_with_errors_results = await asyncio.gather(*complete_definitions_with_errors_handlers)
    complete_definitions_with_errors_evts = to_ok_list(*complete_definitions_with_errors_results)
    opt_all_defs_completed = next((evt for evt in complete_definitions_with_errors_evts if type(evt) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted), None)
    if opt_all_defs_completed is not None:
        return Result.Ok(opt_all_defs_completed)
    complete_definitions_errors = to_error_list(*complete_definitions_with_errors_results)    
    if complete_definitions_errors:
        return Result.Error(complete_definitions_errors)
    return Result.Ok(None)
