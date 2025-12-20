from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import functools
from typing import Any
# from typing import Any, Concatenate, ParamSpec, TypeVar

from expression import Result

from shared.customtypes import DefinitionIdValue, Error, RunIdValue, StepIdValue
from shared.definition import Definition
from shared.infrastructure.storage.repository import StorageError
from shared.runningdefinition import RunningDefinitionState
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result

# P = ParamSpec("P")
# R = TypeVar("R")
# TCfg = TypeVar("TCfg")
# D = TypeVar("D")

ToStorageActionConverter = Callable[[Callable[[RunningDefinitionState | None], tuple[RunningDefinitionState.Events.Event | None, RunningDefinitionState]]], Callable[[RunIdValue, DefinitionIdValue], Coroutine[Any, Any, RunningDefinitionState.Events.Event | None]]]
# ToStorageActionConverter = Callable[[Callable[Concatenate[RunningDefinitionState | None, P], tuple[R, RunningDefinitionState]]], Callable[Concatenate[RunIdValue, DefinitionIdValue, P], Coroutine[Any, Any, R]]]

@dataclass(frozen=True)
class ExecuteDefinitionCommand:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    definition: Definition

def _run_first_step(definition: Definition, state: RunningDefinitionState | None):
    def set_definition_and_run_first_step():
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
        evt = new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        return (evt, new_state)
    def run_first_step(state: RunningDefinitionState):
        opt_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        match opt_evt:
            case None:
                return None
            case evt:
                return (evt, state)
    def rerun_first_step(state: RunningDefinitionState):
        first_step_already_completed = state.recent_completed_step_id() is not None
        if first_step_already_completed:
            return None
        # Possible retry because of previous failure
        # First step is already running, we need to cancel it and run the first step again
        state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
        opt_retry_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        match opt_retry_evt:
            case None:
                return None
            case retry_evt:
                return (retry_evt, state)
    match state:
        case None:
            return set_definition_and_run_first_step()
        case _:
            return run_first_step(state) or rerun_first_step(state) or (None, state)

def _fail_run_first_step(running_step_id: StepIdValue, error: Any, state: RunningDefinitionState | None):
    if state is None:
        raise RuntimeError("_fail_run_first_step received None")
    is_current_step_running = state.running_step_id() == running_step_id
    if is_current_step_running:
        evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error.from_error(error)))
        return (evt, state)
    else:
        return (None, state)

@dataclass(frozen=True)
class RunFirstStepError:
    step_id: StepIdValue
    error: Any

async def handle(
        convert_to_storage_action: ToStorageActionConverter,
        run_first_step_handler: Callable[[RunningDefinitionState.Events.StepRunning], Coroutine[Any, Any, Result]],
        cmd: ExecuteDefinitionCommand):
    @coroutine_result[StorageError | RunFirstStepError]()
    async def execute_definition_workflow():
        run_first_step = functools.partial(_run_first_step, cmd.definition)
        apply_run_first_step = async_result(async_ex_to_error_result(StorageError.from_exception)(convert_to_storage_action(run_first_step)))
        opt_evt = await apply_run_first_step(cmd.run_id, cmd.definition_id)
        if type(opt_evt) is RunningDefinitionState.Events.StepRunning:
            await async_result(run_first_step_handler)(opt_evt).map_error(lambda err: RunFirstStepError(opt_evt.step_id, err))
        return opt_evt
    async def clean_up_failed_execute(error: StorageError | RunFirstStepError):
        match error:
            case RunFirstStepError(step_id=step_id, error=error):
                fail_run_first_step = functools.partial(_fail_run_first_step, step_id, error)
                apply_fail_run_first_step = async_result(async_ex_to_error_result(StorageError.from_exception)(convert_to_storage_action(fail_run_first_step)))
                await apply_fail_run_first_step(cmd.run_id, cmd.definition_id)
    
    res = await execute_definition_workflow()
    opt_error = res.swap().default_value(None)
    if opt_error is not None:
        await clean_up_failed_execute(opt_error)
    return res