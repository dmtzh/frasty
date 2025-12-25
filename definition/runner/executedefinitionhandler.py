from collections.abc import Callable, Coroutine, Generator
from dataclasses import dataclass
import functools
from typing import Any

from expression import effect, Result

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResultAdapter, CompletedWith
from shared.customtypes import DefinitionIdValue, Error, Metadata, RunIdValue, StepIdValue
from shared.definition import Definition, DefinitionAdapter
from shared.infrastructure.storage.repository import StorageError
from shared.pipeline.actionhandler import ActionData, ActionDataDto, ActionDataInput, ActionHandlerFactory, AsyncActionHandler, InputDataAdapter, RunAsyncAction, run_action_adapter
from shared.runningdefinition import RunningDefinitionState
from shared.runningdefinitionsstore import running_action_definitions_storage
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.parse import parse_from_dict

from .runningparentaction import RunningParentAction

EXECUTE_DEFINITION_ACTION = Action(ActionName("execute_definition"), ActionType.CORE)

@dataclass(frozen=True)
class ExecuteDefinitionInput:
    opt_definition_id: DefinitionIdValue | None
    definition: Definition
    def to_dict(self):
        definition_id_dict = {"definition_id": self.opt_definition_id.to_value_with_checksum()} if self.opt_definition_id is not None else {}
        definition_dict = {"definition": DefinitionAdapter.to_list(self.definition)}
        return definition_id_dict | definition_dict

def run_execute_definition_action(run_action: RunAsyncAction, data: ActionData[None, ExecuteDefinitionInput]):
    input = _ExecuteDefinitionInputAdapter(data.input)
    execute_definition_data = ActionData(data.run_id, data.step_id, data.config, input, data.metadata)
    return run_action_adapter(run_action)(EXECUTE_DEFINITION_ACTION, execute_definition_data)

def register_execute_definition_action_handler(run_action: RunAsyncAction, action_handler: AsyncActionHandler):
    def handle_execute_definition_action(data: ActionData[None, ExecuteDefinitionInput]):
        return _handle_execute_definition_action(running_action_definitions_storage.with_storage, run_action, data)
    return ActionHandlerFactory(run_action, action_handler).create_without_config(
        EXECUTE_DEFINITION_ACTION,
        _ExecuteDefinitionInputAdapter.from_list
    )(handle_execute_definition_action)

type ToStorageActionConverter = Callable[[Callable[[RunningDefinitionState | None], tuple[RunningDefinitionState.Events.Event | None, RunningDefinitionState]]], Callable[[RunIdValue, DefinitionIdValue], Coroutine[Any, Any, RunningDefinitionState.Events.Event | None]]]

@dataclass(frozen=True)
class _ExecuteDefinitionInputAdapter(ActionDataInput, ExecuteDefinitionInput):
    def __init__(self, input: ExecuteDefinitionInput):
        super().__init__(input.opt_definition_id, input.definition)
    def serialize(self):
        return [
            ExecuteDefinitionInput.to_dict(self)
        ]
    @effect.result['ExecuteDefinitionInput', str]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, 'ExecuteDefinitionInput']:
        data_dict = data[0]
        if "definition_id" in data_dict:
            opt_definition_id = yield from parse_from_dict(data_dict, "definition_id", DefinitionIdValue.from_value_with_checksum)
        else:
            opt_definition_id = None
        list_definition = yield from parse_from_dict(data_dict, "definition", lambda lst: lst if isinstance(lst, list) else None)
        definition = yield from DefinitionAdapter.from_list(list_definition).map_error(str)
        return ExecuteDefinitionInput(opt_definition_id, definition)

async def _handle_execute_definition_action(
        convert_to_storage_action: ToStorageActionConverter,
        run_action: RunAsyncAction,
        data: ActionData[None, ExecuteDefinitionInput]):
    definition_id = data.input.opt_definition_id or DefinitionIdValue.new_id()
    def run_first_step_handler(evt: RunningDefinitionState.Events.StepRunning):
        data_dict = InputDataAdapter.to_dict(evt.input_data) | (evt.step_definition.config or {})
        metadata = Metadata()
        metadata.set_from("execute definition action")
        metadata.set_definition_id(definition_id)
        parent_action = RunningParentAction(data.run_id, data.step_id, data.metadata)
        parent_action.add_to_metadata(metadata)
        metadata_dict = metadata.to_dict()
        action_data = ActionDataDto(data.run_id.to_value_with_checksum(), evt.step_id.to_value_with_checksum(), data_dict, metadata_dict)
        return run_action(evt.step_definition.get_name(), action_data)
    cmd = ExecuteDefinitionCommand(data.run_id, definition_id, data.input.definition)
    execute_definition_res = await _handle(
        convert_to_storage_action,
        run_first_step_handler,
        cmd
    )
    def ok_to_none(_):
        # Definition started and will complete eventually. Return None to properly handle ongoing execute definition action.
        return None
    def err_to_completed_result(err):
        # Definition failed to start. Return CompletedWith.Data result to complete execute definition action.
        error_result_dict = CompletedResultAdapter.to_dict(CompletedWith.Error(str(err)))
        return CompletedWith.Data(error_result_dict)
        
    return execute_definition_res\
        .map(ok_to_none)\
        .map_error(err_to_completed_result)\
        .merge()

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

async def _handle(
        convert_to_storage_action: ToStorageActionConverter,
        run_first_step_handler: Callable[[RunningDefinitionState.Events.StepRunning], Coroutine[Any, Any, Result]],
        cmd: ExecuteDefinitionCommand):
    async def execute_definition_workflow():
        run_first_step = functools.partial(_run_first_step, cmd.definition)
        apply_run_first_step = async_ex_to_error_result(StorageError.from_exception)(convert_to_storage_action(run_first_step))
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
    async def clean_up_failed_execute(error: StorageError | RunFirstStepError):
        match error:
            case RunFirstStepError(step_id=step_id, error=error):
                fail_run_first_step = functools.partial(_fail_run_first_step, step_id, error)
                apply_fail_run_first_step = async_ex_to_error_result(StorageError.from_exception)(convert_to_storage_action(fail_run_first_step))
                await apply_fail_run_first_step(cmd.run_id, cmd.definition_id)
    
    res = await execute_definition_workflow()
    opt_error = res.swap().default_value(None)
    if opt_error is not None:
        await clean_up_failed_execute(opt_error)
    return res
