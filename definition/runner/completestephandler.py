from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Error, IdValue, RunIdValue, StepIdValue
from shared.infrastructure.storage.repository import StorageError, NotFoundException, NotFoundError
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result
from shared.utils.result import ResultTag

from shared.domainrunning import RunningDefinitionState
from shared.runningdefinitionsstore import running_definitions_storage

@dataclass(frozen=True)
class CompleteStepCommand:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    step_id: StepIdValue
    result: CompletedResult
    metadata: dict

def state_not_found_ex_to_err(ex: NotFoundException, run_id: IdValue, definition_id: IdValue, *args) -> NotFoundError:
    return NotFoundError(f"step1_apply_run_next_step state not found for run_id {run_id} and definition_id {definition_id}")

@async_result
@async_ex_to_error_result(StorageError.from_exception)
@async_ex_to_error_result(state_not_found_ex_to_err, NotFoundException)
@running_definitions_storage.with_storage
def step1_apply_run_next_step(state: RunningDefinitionState | None, completed_step_id: StepIdValue, completed_step_result: CompletedResult):
    if state is None:
        raise NotFoundException()
    def complete_current_step_and_run_next():
        is_current_step_running = state.running_step_id() == completed_step_id
        if is_current_step_running:
            state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(completed_step_result))
            evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
            return (evt, state)
        else:
            return None
    def run_next_step_from_current():
        is_current_step_recent_completed = state.recent_completed_step_id() == completed_step_id
        no_running_step = state.running_step_id() is None
        if is_current_step_recent_completed and no_running_step:
            evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
            return (evt, state)
        else:
            return None
    def rerun_next_step_from_current():
        # Possible retry because of previous failure
        # Target step is already running, we need to cancel it and run the step again
        is_current_step_recent_completed = state.recent_completed_step_id() == completed_step_id
        has_running_step = state.running_step_id() is not None
        if is_current_step_recent_completed and has_running_step:
            state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
            evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
            return (evt, state)
        else:
            return None
    return complete_current_step_and_run_next() or run_next_step_from_current() or rerun_next_step_from_current() or (None, state)

@async_ex_to_error_result(StorageError.from_exception)
@running_definitions_storage.with_storage
def step3_apply_fail(state: RunningDefinitionState | None, error: Any):
    if state is None:
        raise RuntimeError(f"fail received None for {id}")
    evt = state.apply_command(RunningDefinitionState.Commands.Fail(Error.from_error(error)))
    return (evt, state)

@async_ex_to_error_result(StorageError.from_exception)
@running_definitions_storage.with_storage
def step3_apply_fail_running_step(state: RunningDefinitionState | None, running_step_id: IdValue, error: Any):
    if state is None:
        raise RuntimeError(f"fail_running_step received None for {id}")
    def fail_current_running_step():
        is_current_step_running = state.running_step_id() == running_step_id
        if not is_current_step_running:
            return None
        evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error.from_error(error)))
        return (evt, state)
    return fail_current_running_step() or (None, state)

@coroutine_result()
async def complete_step_workflow(event_handler: Callable[[CompleteStepCommand, RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted], Coroutine[Any, Any, Result]], cmd: CompleteStepCommand):
    @async_result
    async def handle_definition_completed(evt: RunningDefinitionState.Events.DefinitionCompleted):
        handle_res = await event_handler(cmd, evt)
        match handle_res:
            case Result(tag=ResultTag.ERROR, error=error):
                await step3_apply_fail(cmd.run_id, cmd.definition_id, error)
        return handle_res
    @async_result
    async def handle_step_running(evt: RunningDefinitionState.Events.StepRunning):
        handle_res = await event_handler(cmd, evt)
        match handle_res:
            case Result(tag=ResultTag.ERROR, error=error):
                await step3_apply_fail_running_step(cmd.run_id, cmd.definition_id, evt.step_id, error)
        return handle_res
    evt = await step1_apply_run_next_step(cmd.run_id, cmd.definition_id, cmd.step_id, cmd.result)
    match evt:
        case RunningDefinitionState.Events.DefinitionCompleted():
            await handle_definition_completed(evt)
        case RunningDefinitionState.Events.StepRunning():
            await handle_step_running(evt)
    return evt

async def handle(event_handler: Callable[[CompleteStepCommand, RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted], Coroutine[Any, Any, Result]], cmd: CompleteStepCommand) -> None | Result[RunningDefinitionState.Events.Event | None, Any]:
    complete_step_res = await complete_step_workflow(event_handler, cmd)
    match complete_step_res:
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            return None
        case _:
            return complete_step_res