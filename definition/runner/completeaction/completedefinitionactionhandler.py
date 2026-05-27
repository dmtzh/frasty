from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Concatenate, NamedTuple

from expression import Result

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Error, RunIdValue, StepIdValue
from shared.infrastructure.storage.repository import NotFoundError, NotFoundException, StorageError
from shared.runningdefinition import RunningDefinitionState
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

type ToStorageActionConverter[**P] = Callable[[Callable[Concatenate[RunningDefinitionState | None, P], tuple[RunningDefinitionState.Events.Event | None, RunningDefinitionState]]], Callable[Concatenate[RunIdValue, DefinitionIdValue, P], Coroutine[Any, Any, RunningDefinitionState.Events.Event | None]]]
type CompleteInput = CompletedResult

@dataclass(frozen=True)
class CompleteActionCommand:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    step_id: StepIdValue
    result: CompletedResult

async def handle(
    convert_to_storage_action: ToStorageActionConverter,
    event_handler: Callable[[RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted], Coroutine[Any, Any, Result]],
    cmd: CompleteActionCommand
):
    def map_errors(error: NotFoundError | StorageError | _EventHandlerError):
        match error:
            case _EventHandlerError(event=evt, error=evt_error):
                match evt:
                    case RunningDefinitionState.Events.StepRunning(step_id=step_id):
                        return RunNextStepError(step_id, evt_error)
                    case RunningDefinitionState.Events.DefinitionCompleted():
                        return CompleteDefinitionError(evt_error)
            case _:
                return error
    
    res = await _complete_action_workflow(convert_to_storage_action, event_handler, cmd)
    opt_error = res.swap().default_value(None)
    if opt_error is not None:
        await _clean_up_failed_complete(convert_to_storage_action, cmd, opt_error)
    return res.map_error(map_errors)

class _EventHandlerError(NamedTuple):
    event: RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted
    error: Any

@dataclass(frozen=True)
class RunNextStepError:
    step_id: StepIdValue
    error: Any

@dataclass(frozen=True)
class CompleteDefinitionError:
    error: Any

@coroutine_result[NotFoundError | StorageError | _EventHandlerError]()
async def _complete_action_workflow(
    convert_to_storage_action: ToStorageActionConverter,
    event_handler: Callable[[RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted], Coroutine[Any, Any, Result]],
    cmd: CompleteActionCommand
):
    @async_result
    @async_ex_to_error_result(StorageError.from_exception)
    @async_ex_to_error_result(lambda _: NotFoundError(f"State not found for run_id {cmd.run_id} and definition_id {cmd.definition_id}"), NotFoundException)
    @convert_to_storage_action
    def apply_run_next_step(state: RunningDefinitionState | None):
        if state is None:
            raise NotFoundException()
        def complete_current_step_and_run_next():
            opt_step_completed_evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(cmd.step_id, cmd.result))
            if opt_step_completed_evt is None:
                return None
            evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
            return (evt, state)
        def run_next_step_from_current():
            is_current_step_recent_completed = state.recent_completed_step_id() == cmd.step_id
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
            is_current_step_recent_completed = state.recent_completed_step_id() == cmd.step_id
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
    opt_evt = await apply_run_next_step(cmd.run_id, cmd.definition_id)
    match opt_evt:
        case RunningDefinitionState.Events.StepRunning() | RunningDefinitionState.Events.DefinitionCompleted():
            await async_result(event_handler)(opt_evt).map_error(lambda err: _EventHandlerError(opt_evt, err))
    return opt_evt

async def _clean_up_failed_complete(
    convert_to_storage_action: ToStorageActionConverter,
    cmd: CompleteActionCommand,
    error: NotFoundError | StorageError | _EventHandlerError
):
    @async_ex_to_error_result(StorageError.from_exception)
    @convert_to_storage_action
    def apply_fail_running_step(state: RunningDefinitionState | None, running_step_id: StepIdValue, error: Any):
        if state is None:
            raise NotFoundException()
        def fail_current_running_step():
            is_current_step_running = state.running_step_id() == running_step_id
            if not is_current_step_running:
                return None
            evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error.from_error(error)))
            return (evt, state)
        return fail_current_running_step() or (None, state)
    @async_ex_to_error_result(StorageError.from_exception)
    @convert_to_storage_action
    def apply_fail(state: RunningDefinitionState | None, error: Any):
        if state is None:
            raise NotFoundException()
        evt = state.apply_command(RunningDefinitionState.Commands.Fail(Error.from_error(error)))
        return (evt, state)
    
    match error:
        case _EventHandlerError(event=evt, error=evt_error) if type(evt) is RunningDefinitionState.Events.StepRunning:
            await apply_fail_running_step(cmd.run_id, cmd.definition_id, evt.step_id, evt_error)
        case _EventHandlerError(event=RunningDefinitionState.Events.DefinitionCompleted(), error=evt_error):
            await apply_fail(cmd.run_id, cmd.definition_id, evt_error)