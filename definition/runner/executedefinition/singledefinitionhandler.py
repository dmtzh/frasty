from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.customtypes import DefinitionIdValue, Error, Metadata, RunIdValue, StepIdValue
from shared.definition import Definition
from shared.executedefinitionaction import ExecuteDefinitionInput
from shared.infrastructure.storage.repository import StorageError
from shared.pipeline.actionhandler import ActionData, ActionInput, DataDtoAdapter, RunAsyncAction
from shared.runningdefinition import RunningDefinitionState
from shared.utils.exceptiondecorators import async_ex_to_error_result

from runningparentaction import RunningParentAction

type ToStorageActionConverter = Callable[[Callable[[RunningDefinitionState | None], tuple[RunningDefinitionState.Events.Event | None, RunningDefinitionState]]], Callable[[RunIdValue, DefinitionIdValue], Coroutine[Any, Any, RunningDefinitionState.Events.Event | None]]]

async def handle(
        convert_to_storage_action: ToStorageActionConverter,
        run_action: RunAsyncAction,
        data: ActionData[None, ExecuteDefinitionInput]):
    definition_id = data.input.definition_id
    def run_first_step_handler(evt: RunningDefinitionState.Events.StepRunning):
        metadata = Metadata()
        metadata.set_from("execute definition action")
        metadata.set_definition_id(definition_id)
        parent_action = RunningParentAction(data.run_id, data.step_id, data.metadata)
        parent_action.add_to_metadata(metadata)
        data_dict = DataDtoAdapter.to_input_data(evt.input_data) | (evt.step_definition.config or {})
        metadata_dict = metadata.to_dict()
        action_input = ActionInput(data.run_id.to_value_with_checksum(), evt.step_id.to_value_with_checksum(), data_dict, metadata_dict)
        return run_action(evt.step_definition.get_name(), action_input)
    
    cmd = _ExecuteDefinitionCommand(data.run_id, definition_id, data.input.definition)
    execute_single_definition_res = await _execute_single_definition_workflow(
        convert_to_storage_action,
        run_first_step_handler,
        cmd
    )
    opt_error = execute_single_definition_res.swap().default_value(None)
    if opt_error is not None:
        await _clean_up_failed_execute(convert_to_storage_action, cmd, opt_error)
    return execute_single_definition_res

@dataclass(frozen=True)
class _ExecuteDefinitionCommand:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    definition: Definition

@dataclass(frozen=True)
class RunFirstStepError:
    step_id: StepIdValue
    error: Any

async def _execute_single_definition_workflow(
    convert_to_storage_action: ToStorageActionConverter,
    run_first_step_handler: Callable[[RunningDefinitionState.Events.StepRunning], Coroutine[Any, Any, Result]],
    cmd: _ExecuteDefinitionCommand
) -> Result[RunningDefinitionState.Events.Event | None, RunFirstStepError | StorageError]:
    @async_ex_to_error_result(StorageError.from_exception)
    @convert_to_storage_action
    def apply_run_first_step(state: RunningDefinitionState | None):
        def set_definition_and_run_first_step():
            new_state = RunningDefinitionState()
            new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(cmd.definition))
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
    
    opt_evt_res = await apply_run_first_step(cmd.run_id, cmd.definition_id)
    opt_evt = opt_evt_res.default_value(None)
    match opt_evt:
        case RunningDefinitionState.Events.StepRunning():
            run_first_step_res = await run_first_step_handler(opt_evt)
            return run_first_step_res\
                .map(lambda _: opt_evt)\
                .map_error(lambda err: RunFirstStepError(opt_evt.step_id, err))
        case _:
            return opt_evt_res

async def _clean_up_failed_execute(convert_to_storage_action: ToStorageActionConverter, cmd: _ExecuteDefinitionCommand, error: StorageError | RunFirstStepError):
    def apply_run_first_step_error(err: RunFirstStepError):
        @async_ex_to_error_result(StorageError.from_exception)
        @convert_to_storage_action
        def fail_run_first_step(state: RunningDefinitionState | None):
            if state is None:
                raise RuntimeError("fail_run_first_step received None")
            is_current_step_running = state.running_step_id() == err.step_id
            if is_current_step_running:
                evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error.from_error(err.error)))
                return (evt, state)
            else:
                return (None, state)
        return fail_run_first_step(cmd.run_id, cmd.definition_id)
    
    match error:
        case RunFirstStepError():
            await apply_run_first_step_error(error)
    
    