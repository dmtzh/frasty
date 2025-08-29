from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.definitionsstore import definitions_storage
from shared.domainrunning import RunningDefinitionState
from shared.runningdefinitionsstore import running_definitions_storage
from shared.customtypes import IdValue, Error
from shared.domaindefinition import Definition
from shared.infrastructure.storage.repository import StorageError
from shared.utils.asyncresult import async_catch_ex, async_result, async_ex_to_error_result, coroutine_result
from shared.utils.result import ResultTag

@dataclass(frozen=True)
class RunDefinitionCommand:
    task_id: IdValue
    run_id: IdValue
    definition_id: IdValue
    metadata: dict

@async_result
@async_ex_to_error_result(StorageError.from_exception)
@running_definitions_storage.with_storage
def apply_run_first_step(state: RunningDefinitionState | None, definition: Definition):
    def set_definition_and_run_first_step():
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
        evt = new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        return (evt, new_state)
    def run_first_step(state: RunningDefinitionState):
        no_recent_completed_step = state.recent_completed_step_id() is None
        no_running_step = state.running_step_id() is None
        if no_recent_completed_step and no_running_step:
            evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
            return (evt, state)
        else:
            return None
    def rerun_first_step(state: RunningDefinitionState):
        no_recent_completed_step = state.recent_completed_step_id() is None
        has_running_step = state.running_step_id() is not None
        if no_recent_completed_step and has_running_step:
            # Possible retry because of previous failure
            # First step is already running, we need to cancel it and run the first step again
            state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
            retry_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
            return (retry_evt, state)
        else:
            return None
    match state:
        case None:
            return set_definition_and_run_first_step()
        case _:
            return run_first_step(state) or rerun_first_step(state) or (None, state)

@async_ex_to_error_result(StorageError.from_exception)
@running_definitions_storage.with_storage
def apply_fail_run_first_step(state: RunningDefinitionState | None, running_step_id: IdValue, error: Any):
    if state is None:
        raise RuntimeError("apply_fail_run_first_step received None")
    is_current_step_running = state.running_step_id() == running_step_id
    if is_current_step_running:
        evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error.from_error(error)))
        return (evt, state)
    else:
        return (None, state)

@dataclass(frozen=True)
class RunFirstStepError:
    step_id: IdValue
    error: Any

@coroutine_result()
async def run_definition_workflow(run_first_step_handler: Callable[[RunDefinitionCommand, RunningDefinitionState.Events.StepRunning], Coroutine[Any, Any, Result]], cmd: RunDefinitionCommand, definition: Definition):
    evt = await apply_run_first_step(cmd.run_id, cmd.definition_id, definition)
    if type(evt) is RunningDefinitionState.Events.StepRunning:
        await async_result(run_first_step_handler)(cmd, evt).map_error(lambda err: RunFirstStepError(evt.step_id, err))
    return evt

async def clean_up_failed_run(cmd: RunDefinitionCommand, error: Any):
    match error:
        case RunFirstStepError(step_id=step_id, error=error):
            await apply_fail_run_first_step(cmd.run_id, cmd.definition_id, step_id, error)

async def handle(run_first_step_handler: Callable[[RunDefinitionCommand, RunningDefinitionState.Events.StepRunning], Coroutine[Any, Any, Result]], cmd: RunDefinitionCommand) -> None | Result[RunningDefinitionState.Events.Event | None, Any]:
    opt_definition_res = await async_catch_ex(definitions_storage.get)(cmd.definition_id)
    match opt_definition_res:
        case Result(tag=ResultTag.OK, ok=None):
            return None
        case Result(tag=ResultTag.OK, ok=definition):
            res = await run_definition_workflow(run_first_step_handler, cmd, definition)
            if res.is_error():
                await clean_up_failed_run(cmd, res.error)
            return res
        case Result(tag=ResultTag.ERROR, error=error):
            return Result.Error(error)
        