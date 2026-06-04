from collections.abc import Callable
from typing import Concatenate, ParamSpec, TypeVar
from expression import Result
import pytest
from runner.executedefinition import groupofdefinitionshandler
from runner.executedefinition.input import ExecuteGroupOfDefinitionsInput
from runner.runningparentaction import RunningParentAction
from shared.action import ActionName, ActionType
from shared.customtypes import DefinitionIdValue, Error, Metadata, RunIdValue, StepIdValue
from shared.definition import ActionDefinition, Definition
from shared.definitioncustomtypes import GroupIdValue
from shared.executedefinitionaction import ExecuteDefinitionInput
from shared.groupofrunningdefinitions import GroupOfRunningDefinitionsState, DefinitionIdWithValue
from shared.pipeline.actionhandler import ActionData, ActionInput
from config import group_of_running_definitions_storage

P = ParamSpec("P")
R = TypeVar("R")

@pytest.fixture
def convert_to_storage_action():
    # Returns the storage wrapper decorator for group-level state transactions
    return group_of_running_definitions_storage.with_storage

@pytest.fixture
def run_definition_action():
    # Default async mock that successfully completes any definition execution
    async def run_action(action_name: str, action_input: ActionInput):
        return Result.Ok(None)
    return run_action

@pytest.fixture
def action_data():
    run_id = RunIdValue.new_id()
    # step_id is used as group_id inside the handler
    step_id = StepIdValue.new_id()
    config = None
    
    def_id_1 = DefinitionIdValue.new_id()
    def_id_2 = DefinitionIdValue.new_id()
    step_def_1 = ActionDefinition(ActionName("request_url"), ActionType.CUSTOM, None)
    step_def_2 = ActionDefinition(ActionName("parse_response"), ActionType.CUSTOM, None)
    
    definition_1 = Definition({"payload": {"key": "value_1"}}, (step_def_1,))
    definition_2 = Definition({"payload": {"key": "value_2"}}, (step_def_2,))
    
    exec_input_1 = ExecuteDefinitionInput(def_id_1, definition_1)
    exec_input_2 = ExecuteDefinitionInput(def_id_2, definition_2)
    
    input_data = ExecuteGroupOfDefinitionsInput((exec_input_1, exec_input_2))
    metadata = Metadata()
    metadata.set_definition_id(DefinitionIdValue.new_id())
    return ActionData(run_id, step_id, config, input_data, metadata)



async def test_handle_returns_definitions_running_event(convert_to_storage_action, run_definition_action, action_data):
    handle_res = await groupofdefinitionshandler.handle(convert_to_storage_action, run_definition_action, action_data)
    
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is GroupOfRunningDefinitionsState.Events.DefinitionsRunning
    # Verify that all definitions were transitioned to running state
    assert len(evt.definitions) == len(action_data.input.items)



async def test_handle_returns_all_completed_when_partial_failure(convert_to_storage_action, action_data):
    expected_error = Error("downstream service timeout")
    
    # Mock that fails for all definitions to trigger compensation logic
    async def run_action_fail(action_name: str, action_input: ActionInput):
        return Result.Error(expected_error)
    
    handle_res = await groupofdefinitionshandler.handle(convert_to_storage_action, run_action_fail, action_data)
    
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    # Compensation should finalize the group with an aggregated completion event
    assert type(evt) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted



async def test_handle_returns_no_event_when_group_already_running(convert_to_storage_action, run_definition_action, action_data):
    # Pre-seed state to simulate an already running group
    def seed_group_running(state: GroupOfRunningDefinitionsState | None):
        if state is not None:
            raise RuntimeError("State already exists")
        new_state = GroupOfRunningDefinitionsState()
        defs = tuple(DefinitionIdWithValue(item.definition_id, item.definition) for item in action_data.input.items)
        new_state.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        evt = new_state.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
        return (evt, new_state)
    
    await convert_to_storage_action(seed_group_running)(action_data.run_id, action_data.step_id)
    handle_res = await groupofdefinitionshandler.handle(convert_to_storage_action, run_definition_action, action_data)
    
    assert handle_res.is_ok()
    # Handler should return None when state is already initialized and running
    assert handle_res.ok is None



async def test_handle_passes_correct_running_parent_action_to_run_action(convert_to_storage_action, action_data):
    passed_metadata = {}
    
    async def run_action_capture(action_name: str, action_input: ActionInput):
        opt_running_parent_action = RunningParentAction.parse(Metadata(action_input.metadata))
        if opt_running_parent_action is not None:
            passed_metadata["parent_run_id"] = opt_running_parent_action.run_id
        return Result.Ok(None)
    
    await groupofdefinitionshandler.handle(convert_to_storage_action, run_action_capture, action_data)
    
    # Verify parent context propagation through RunningParentAction
    assert passed_metadata["parent_run_id"] == action_data.run_id



async def test_handle_returns_error_when_storage_exception(run_definition_action, action_data):
    # Local fixture override to simulate storage failure
    def convert_to_storage_action_err(func: Callable[Concatenate[GroupOfRunningDefinitionsState | None, P], tuple[R, GroupOfRunningDefinitionsState]]):
        async def wrapper(run_id: RunIdValue, group_id: GroupIdValue, *args: P.args, **kwargs: P.kwargs) -> R:
            raise RuntimeError("Storage write failed")
        return wrapper
    
    handle_res = await groupofdefinitionshandler.handle(convert_to_storage_action_err, run_definition_action, action_data)
    
    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert type(handle_res.error) is groupofdefinitionshandler.RunGroupOfDefinitionsStorageError



async def test_handle_does_not_invoke_run_action_when_already_completed(convert_to_storage_action, action_data):
    run_action_calls = []
    
    async def run_action_track(action_name: str, action_input: ActionInput):
        run_action_calls.append(action_input)
        return Result.Ok(None)
    
    # Pre-seed to a completed state
    def seed_group_completed(state: GroupOfRunningDefinitionsState | None):
        if state is not None:
            raise RuntimeError()
        new_state = GroupOfRunningDefinitionsState()
        defs = tuple(DefinitionIdWithValue(item.definition_id, item.definition) for item in action_data.input.items)
        new_state.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        # Transition to completed without executing run handlers
        evt = new_state.apply_command(GroupOfRunningDefinitionsState.Commands.Fail(Error("group completed externally")))
        return (evt, new_state)
    
    await convert_to_storage_action(seed_group_completed)(action_data.run_id, action_data.step_id)
    await groupofdefinitionshandler.handle(convert_to_storage_action, run_action_track, action_data)
    
    # No run_action should be triggered for already terminal states
    assert len(run_action_calls) == 0



async def test_handle_two_different_groups_with_same_run_id(convert_to_storage_action, run_definition_action, action_data):
    # Second group with different step_id (group_id) but same run_id
    action_data_2 = ActionData(
        action_data.run_id,
        StepIdValue.new_id(),
        action_data.config,
        ExecuteGroupOfDefinitionsInput((
            ExecuteDefinitionInput(DefinitionIdValue.new_id(), Definition({"x": 1}, (ActionDefinition(ActionName("a1"), ActionType.CUSTOM, None),))),
            ExecuteDefinitionInput(DefinitionIdValue.new_id(), Definition({"x": 2}, (ActionDefinition(ActionName("a1"), ActionType.CUSTOM, None),)))
        )),
        Metadata()
    )
    
    handle1_res = await groupofdefinitionshandler.handle(convert_to_storage_action, run_definition_action, action_data)
    handle2_res = await groupofdefinitionshandler.handle(convert_to_storage_action, run_definition_action, action_data_2)
    
    assert type(handle1_res) is Result and type(handle2_res) is Result
    assert handle1_res.is_ok() and handle2_res.is_ok()
    # Events should be isolated per group_id, no cross-contamination
    assert type(handle1_res.ok) is GroupOfRunningDefinitionsState.Events.DefinitionsRunning
    assert type(handle2_res.ok) is GroupOfRunningDefinitionsState.Events.DefinitionsRunning
    assert handle1_res.ok.definitions != handle2_res.ok.definitions