from collections.abc import Callable
from typing import Concatenate, ParamSpec, TypeVar
from expression import Result
import pytest

from shared.action import ActionName, ActionType
from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, Error, RunIdValue
from shared.definition import ActionDefinition, Definition
from shared.runningdefinition import RunningDefinitionState
from shared.runningdefinitionsstore import running_action_definitions_storage
from webapi import executedefinitionhandler

@pytest.fixture
def convert_to_storage_action():
    return running_action_definitions_storage.with_storage

@pytest.fixture
def run_first_step_handler():
    async def handler(evt):
        return Result.Ok(None)
    return handler

FIRST_STEP_DEFINITION = ActionDefinition(ActionName("requesturl"), ActionType.CUSTOM, {"url": "http://localhost", "http_method": "GET"})

@pytest.fixture
def run_id():
    return RunIdValue.new_id()

@pytest.fixture
def cmd1(run_id):
    definition_id = DefinitionIdValue.new_id()
    second_step_definition = ActionDefinition(ActionName("filtersuccessresponse"), ActionType.CUSTOM, None)
    definition = Definition((FIRST_STEP_DEFINITION, second_step_definition))
    cmd = executedefinitionhandler.ExecuteDefinitionCommand(run_id, definition_id, definition)
    return cmd

@pytest.fixture
def cmd2(run_id):
    definition_id = DefinitionIdValue.new_id()
    first_step_definition = ActionDefinition(ActionName("filtersuccessresponse"), ActionType.CUSTOM, { "response": "test response" })
    second_step_definition = ActionDefinition(ActionName("filterhtmlresponse"), ActionType.CUSTOM, None)
    definition = Definition((first_step_definition, second_step_definition))
    cmd = executedefinitionhandler.ExecuteDefinitionCommand(run_id, definition_id, definition)
    return cmd



async def test_handle_returns_first_step_running_event(convert_to_storage_action, run_first_step_handler, cmd1):
    expected_step_definition = FIRST_STEP_DEFINITION

    handle_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)

    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition == expected_step_definition



async def test_handle_returns_new_first_step_running_event_when_first_step_already_running(convert_to_storage_action, run_first_step_handler, cmd1):
    def run_first_step(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(cmd1.definition))
        evt = new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        return (evt, new_state)
    running_evt = await convert_to_storage_action(run_first_step)(cmd1.run_id, cmd1.definition_id)

    handle_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)

    assert type(running_evt) is RunningDefinitionState.Events.StepRunning
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_id != running_evt.step_id



async def test_handle_returns_first_step_running_event_when_first_step_canceled(convert_to_storage_action, run_first_step_handler, cmd1):
    def run_first_step_then_cancel(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(cmd1.definition))
        new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        evt = new_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
        return (evt, new_state)
    canceled_evt = await convert_to_storage_action(run_first_step_then_cancel)(cmd1.run_id, cmd1.definition_id)

    handle_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)

    assert type(canceled_evt) is RunningDefinitionState.Events.StepCanceled
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_id != canceled_evt.step_id



async def test_handle_returns_first_step_running_event_when_first_step_failed(convert_to_storage_action, run_first_step_handler, cmd1):
    def run_first_step_then_fail(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(cmd1.definition))
        new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        evt = new_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error("some error")))
        return (evt, new_state)
    failed_evt = await convert_to_storage_action(run_first_step_then_fail)(cmd1.run_id, cmd1.definition_id)

    handle_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)

    assert type(failed_evt) is RunningDefinitionState.Events.StepFailed
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_id != failed_evt.step_id



async def test_handle_returns_no_event_when_first_step_already_completed(convert_to_storage_action, run_first_step_handler, cmd1):
    def run_first_step_then_complete(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(cmd1.definition))
        new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        evt = new_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(CompletedWith.Data("completed result data")))
        return (evt, new_state)
    completed_evt = await convert_to_storage_action(run_first_step_then_complete)(cmd1.run_id, cmd1.definition_id)

    handle_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)

    assert type(completed_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert evt is None



async def test_handle_passes_correct_data_to_run_first_step_handler(convert_to_storage_action, cmd1):
    expected_step_definition = FIRST_STEP_DEFINITION
    passed_data = {}
    async def run_first_step_handler(evt: RunningDefinitionState.Events.StepRunning):
        passed_data["input_data"] = evt.input_data
        passed_data["step_definition"] = evt.step_definition
        return Result.Ok(None)
    
    await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)

    assert "input_data" in passed_data
    assert passed_data["input_data"] is None
    assert "step_definition" in passed_data
    actual_step_definition = passed_data["step_definition"]
    assert actual_step_definition.name == expected_step_definition.name
    assert actual_step_definition.type == expected_step_definition.type
    assert actual_step_definition.data == expected_step_definition.data



async def test_handle_returns_error_when_run_first_step_handler_error(convert_to_storage_action, cmd1):
    expected_error = Error("expected error")
    async def run_first_step_handler_with_err(evt):
        return Result.Error(expected_error)
    
    handle_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler_with_err, cmd1)

    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert handle_res.error.error == expected_error



async def test_handle_raises_exception_when_run_first_step_handler_exception(convert_to_storage_action, cmd1):
    expected_ex = RuntimeError("expected exception")
    async def run_first_step_handler_with_ex(evt):
        raise expected_ex
    
    try:
        await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler_with_ex, cmd1)
        assert False
    except Exception as e:
        actual_ex = e

    assert actual_ex == expected_ex



async def test_handle_does_not_invoke_run_first_step_handler_when_first_step_already_completed(convert_to_storage_action, cmd1):
    run_first_step_handler_calls = []
    async def run_first_step_handler(evt):
        run_first_step_handler_calls.append(evt)
        return Result.Ok(None)
    def run_then_complete_first_step(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(cmd1.definition))
        new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        completed_step_result = CompletedWith.Data("completed result data")
        evt = new_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(completed_step_result))
        return (evt, new_state)
    
    await convert_to_storage_action(run_then_complete_first_step)(cmd1.run_id, cmd1.definition_id)
    await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)

    assert len(run_first_step_handler_calls) == 0



P = ParamSpec("P")
R = TypeVar("R")
async def test_handle_returns_error_when_storage_action_exception(run_first_step_handler, cmd1):
    def convert_to_storage_action(func: Callable[Concatenate[RunningDefinitionState | None, P], tuple[R, RunningDefinitionState]]):
        async def wrapper(run_id: RunIdValue, definition_id: DefinitionIdValue, *args: P.args, **kwargs: P.kwargs) -> R:
            raise RuntimeError("Storage action error")
        return wrapper

    handle_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)

    assert type(handle_res) is Result
    assert handle_res.is_error()



async def test_handle_returns_two_different_first_step_running_events_when_invoked_with_same_run_id_but_different_definition_id(convert_to_storage_action, run_first_step_handler, cmd1, cmd2):
    handle1_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd1)
    handle2_res = await executedefinitionhandler.handle(convert_to_storage_action, run_first_step_handler, cmd2)

    assert cmd1.run_id == cmd2.run_id
    assert type(handle1_res) is Result
    assert type(handle2_res) is Result
    assert handle1_res.is_ok()
    assert handle2_res.is_ok()
    evt1 = handle1_res.ok
    evt2 = handle2_res.ok
    assert type(evt1) is RunningDefinitionState.Events.StepRunning
    assert type(evt2) is RunningDefinitionState.Events.StepRunning
    assert evt1.step_definition == cmd1.definition.steps[0]
    assert evt2.step_definition == cmd2.definition.steps[0]
    assert evt1.step_id != evt2.step_id