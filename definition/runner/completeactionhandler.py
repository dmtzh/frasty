from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import functools
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult, CompletedResultAdapter, CompletedWith
from shared.customtypes import DefinitionIdValue, Error, RunIdValue, StepIdValue
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.pipeline.actionhandler import COMPLETE_ACTION, ActionData, ActionDataDto, ActionHandlerFactory, AsyncActionHandler, DataDtoAdapter, RunAsyncAction
from shared.runningdefinition import RunningDefinitionState
from shared.runningdefinitionsstore import running_action_definitions_storage
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

from runningparentaction import RunningParentAction

type CompleteInput = CompletedResult

def register_complete_action_handler(run_action: RunAsyncAction, action_handler: AsyncActionHandler):
    def handle_complete_action(data: ActionData[None, CompleteInput]):
        return _handle_complete_action(running_action_definitions_storage.with_storage, run_action, data)
    async def do_nothing_when_run_action(action_name: str, action_input: ActionDataDto):
        return Result.Ok(None)
    return ActionHandlerFactory(do_nothing_when_run_action, action_handler).create_without_config(
        COMPLETE_ACTION,
        lambda data: CompletedResultAdapter.from_dict(data[0])
    )(handle_complete_action)

ToStorageActionConverter = Callable[[Callable[[RunningDefinitionState | None], tuple[RunningDefinitionState.Events.Event | None, RunningDefinitionState]]], Callable[[RunIdValue, DefinitionIdValue], Coroutine[Any, Any, RunningDefinitionState.Events.Event | None]]]

async def _handle_complete_action(
        convert_to_storage_action: ToStorageActionConverter,
        run_action: RunAsyncAction,
        data: ActionData[None, CompleteInput]):
    async def event_handler(evt: RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted):
        match evt:
            case RunningDefinitionState.Events.StepRunning():
                data_dict = DataDtoAdapter.to_input_data(evt.input_data) | (evt.step_definition.config or {})
                action_input = ActionDataDto(data.run_id.to_value_with_checksum(), evt.step_id.to_value_with_checksum(), data_dict, data.metadata.to_dict())
                return await run_action(evt.step_definition.get_name(), action_input)
            case RunningDefinitionState.Events.DefinitionCompleted():
                opt_parent_action = RunningParentAction.parse(data.metadata)
                match opt_parent_action:
                    case None:
                        return Result.Ok(None)
                    case parent_action_no_def_id if parent_action_no_def_id.metadata.get_definition_id() is None:
                        return Result.Ok(None)
                    case parent_action_with_def_id:
                        return await parent_action_with_def_id.run_complete_definition(run_action, evt.result)
    definition_id = data.metadata.get_definition_id()
    if definition_id is None:
        # definition id is required for complete action
        return None
    cmd = CompleteActionCommand(data.run_id, definition_id, data.step_id, data.input)
    complete_action_res = await handle(
        convert_to_storage_action,
        event_handler,
        cmd
    )
    
    opt_error = complete_action_res.swap().default_value(None)
    if opt_error is not None:
        opt_parent_action = RunningParentAction.parse(data.metadata)
        if opt_parent_action is not None:
            error_result = CompletedWith.Error(str(opt_error))
            await opt_parent_action.run_complete_definition(run_action, error_result)
    
    return None

@dataclass(frozen=True)
class CompleteActionCommand:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    step_id: StepIdValue
    result: CompletedResult

def _run_next_step(completed_step_id: StepIdValue, completed_step_result: CompletedResult, state: RunningDefinitionState | None):
    if state is None:
        raise NotFoundException()
    def complete_current_step_and_run_next():
        is_current_step_running = state.running_step_id() == completed_step_id
        if not is_current_step_running:
            return None
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(completed_step_result))
        evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        return (evt, state)
    def run_next_step_from_current():
        is_current_step_recent_completed = state.recent_completed_step_id() == completed_step_id
        if not is_current_step_recent_completed:
            return None
        opt_evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        match opt_evt:
            case None:
                return None
            case evt:
                return (evt, state)
    def rerun_next_step_from_current():
        # Possible retry because of previous failure
        # Target step is already running, we need to cancel it and run the step again
        is_current_step_recent_completed = state.recent_completed_step_id() == completed_step_id
        if not is_current_step_recent_completed:
            return None
        state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
        opt_evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        match opt_evt:
            case None:
                return None
            case evt:
                return (evt, state)
    return complete_current_step_and_run_next() or run_next_step_from_current() or rerun_next_step_from_current() or (None, state)

def _fail_running_step(running_step_id: StepIdValue, error: Any, state: RunningDefinitionState | None):
    if state is None:
        raise NotFoundException()
    def fail_current_running_step():
        is_current_step_running = state.running_step_id() == running_step_id
        if not is_current_step_running:
            return None
        evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error.from_error(error)))
        return (evt, state)
    return fail_current_running_step() or (None, state)

def _fail(error: Any, state: RunningDefinitionState | None):
    if state is None:
        raise NotFoundException()
    evt = state.apply_command(RunningDefinitionState.Commands.Fail(Error.from_error(error)))
    return (evt, state)

@dataclass(frozen=True)
class EventHandlerError:
    event: RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted
    error: Any

@dataclass(frozen=True)
class RunNextStepError:
    step_id: StepIdValue
    error: Any

@dataclass(frozen=True)
class CompleteDefinitionError:
    error: Any

async def handle(
        convert_to_storage_action: ToStorageActionConverter,
        event_handler: Callable[[RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted], Coroutine[Any, Any, Result]],
        cmd: CompleteActionCommand):
    @coroutine_result[NotFoundError | StorageError | EventHandlerError]()
    async def complete_action_workflow():
        run_next_step = functools.partial(_run_next_step, cmd.step_id, cmd.result)
        not_found_error = NotFoundError(f"State not found for run_id {cmd.run_id} and definition_id {cmd.definition_id}")
        apply_run_next_step = async_result(async_ex_to_error_result(StorageError.from_exception)(async_ex_to_error_result(lambda _: not_found_error, NotFoundException)(convert_to_storage_action(run_next_step))))
        opt_evt = await apply_run_next_step(cmd.run_id, cmd.definition_id)
        match opt_evt:
            case RunningDefinitionState.Events.StepRunning() | RunningDefinitionState.Events.DefinitionCompleted():
                await async_result(event_handler)(opt_evt).map_error(lambda err: EventHandlerError(opt_evt, err))
        return opt_evt
    async def clean_up_failed_complete(error: NotFoundError | StorageError | EventHandlerError):
        match error:
            case EventHandlerError(event=evt, error=evt_error) if type(evt) is RunningDefinitionState.Events.StepRunning:
                fail_running_step = functools.partial(_fail_running_step, evt.step_id, evt_error)
                apply_fail_running_step = async_ex_to_error_result(StorageError.from_exception)(convert_to_storage_action(fail_running_step))
                await apply_fail_running_step(cmd.run_id, cmd.definition_id)
            case EventHandlerError(event=RunningDefinitionState.Events.DefinitionCompleted(), error=evt_error):
                fail = functools.partial(_fail, evt_error)
                apply_fail = async_ex_to_error_result(StorageError.from_exception)(convert_to_storage_action(fail))
                await apply_fail(cmd.run_id, cmd.definition_id)
    def map_errors(error: NotFoundError | StorageError | EventHandlerError):
        match error:
            case EventHandlerError(event=evt, error=evt_error):
                match evt:
                    case RunningDefinitionState.Events.StepRunning(step_id=step_id):
                        return RunNextStepError(step_id, evt_error)
                    case RunningDefinitionState.Events.DefinitionCompleted():
                        return CompleteDefinitionError(evt_error)
            case _:
                return error
    
    res = await complete_action_workflow()
    opt_error = res.swap().default_value(None)
    if opt_error is not None:
        await clean_up_failed_complete(opt_error)
    return res.map_error(map_errors)
