from collections.abc import Callable
from typing import Tuple
from expression import Result
import pytest

from runner import completestephandler
from shared.domainrunning import RunningDefinitionState
from shared.runningdefinitionsstore import running_definitions_storage
from shared.completedresult import CompletedWith
from shared.customtypes import Error, IdValue
from shared.domaindefinition import Definition
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.shared import HttpResponseData

@pytest.fixture(scope="session")
def step_running_event_handler():
    async def handler(cmd, evt):
        assert type(evt) is RunningDefinitionState.Events.StepRunning
        return Result.Ok(None)
    return handler

@pytest.fixture(scope="session")
def definition_completed_event_handler():
    async def handler(cmd, evt):
        assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
        return Result.Ok(None)
    return handler

@pytest.fixture(scope="session")
def runtime_error_event_handler():
    async def handler(cmd, evt):
        raise RuntimeError()
    return handler

@pytest.fixture(scope="session")
def two_step_definition():
    input_data = {"url": "http://localhost", "http_method": "GET"}
    first_step_def = RequestUrl()
    second_step_def = FilterSuccessResponse()
    two_steps_def = [first_step_def, second_step_def]
    definition = Definition.from_steps(input_data, two_steps_def).ok
    return definition

@pytest.fixture(scope="session")
def html_response_result():
    html_data = HttpResponseData(200, "text/html", "<html><body>test html content</body></html>")
    html_response_data = CompletedWith.Data(HttpResponseData.to_dict(html_data))
    return html_response_data

async def create_complete_step_cmd(storage_setup_func: Callable[[RunningDefinitionState | None, dict], Tuple[RunningDefinitionState.Events.Event | None, RunningDefinitionState]]):
    run_id = IdValue.new_id()
    definition_id = IdValue.new_id()
    html_data = HttpResponseData(200, "text/html", "<html><body>test html content</body></html>")
    html_response_data = CompletedWith.Data(HttpResponseData.to_dict(html_data))
    cmd_dict = {
        "opt_task_id": definition_id,
        "run_id": run_id,
        "definition_id": definition_id,
        "result": html_response_data,
        "metadata": {"command": "cmd1"}
    }
    evt = await running_definitions_storage.with_storage(storage_setup_func)(run_id, definition_id, cmd_dict)
    cmd = completestephandler.CompleteStepCommand(cmd_dict["opt_task_id"], cmd_dict["run_id"], cmd_dict["definition_id"], cmd_dict["step_id"], cmd_dict["result"], cmd_dict["metadata"])
    return (evt, cmd)



async def test_handle_returns_next_step_running_event_when_step_running_and_has_more_steps(step_running_event_handler, two_step_definition):
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    first_step_running_evt, cmd = await create_complete_step_cmd(apply_set_first_step_running)
    
    handle_res = await completestephandler.handle(step_running_event_handler, cmd)

    assert type(first_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert first_step_running_evt.step_id != second_step_running_evt.step_id



async def test_handle_returns_next_step_running_event_when_step_completed_and_has_more_steps(step_running_event_handler, two_step_definition, html_response_result):
    def apply_set_first_step_completed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        return (evt, state)
    first_step_completed_evt, cmd = await create_complete_step_cmd(apply_set_first_step_completed)

    handle_res = await completestephandler.handle(step_running_event_handler, cmd)

    assert type(first_step_completed_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert first_step_completed_evt.step_id != second_step_running_evt.step_id



async def test_handle_returns_next_step_running_event_when_next_step_running(step_running_event_handler, two_step_definition, html_response_result):
    def apply_set_second_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        return (evt, state)
    second_step_running_evt1, cmd = await create_complete_step_cmd(apply_set_second_step_running)

    handle_res = await completestephandler.handle(step_running_event_handler, cmd)

    assert type(second_step_running_evt1) is RunningDefinitionState.Events.StepRunning
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt2 = handle_res.ok
    assert type(second_step_running_evt2) is RunningDefinitionState.Events.StepRunning
    assert second_step_running_evt1.step_id != second_step_running_evt2.step_id



async def test_handle_returns_next_step_running_event_when_next_step_canceled(step_running_event_handler, two_step_definition, html_response_result):
    def apply_set_second_step_canceled(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        evt = state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
        return (evt, state)
    second_step_canceled_evt, cmd = await create_complete_step_cmd(apply_set_second_step_canceled)

    handle_res = await completestephandler.handle(step_running_event_handler, cmd)

    assert type(second_step_canceled_evt) is RunningDefinitionState.Events.StepCanceled
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert second_step_canceled_evt.step_id != second_step_running_evt.step_id



async def test_handle_returns_next_step_running_event_when_next_step_running_and_then_failure(step_running_event_handler, two_step_definition, html_response_result):
    second_step_error = Error("Second step failure")
    def apply_set_second_step_failed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        evt = state.apply_command(RunningDefinitionState.Commands.Fail(second_step_error))
        return (evt, state)
    second_step_failed_evt, cmd = await create_complete_step_cmd(apply_set_second_step_failed)

    handle_res = await completestephandler.handle(step_running_event_handler, cmd)

    assert type(second_step_failed_evt) is RunningDefinitionState.Events.Failed
    assert second_step_failed_evt.error == second_step_error
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning



async def test_handle_returns_next_step_running_event_when_next_step_failed(step_running_event_handler, two_step_definition, html_response_result):
    second_step_error = Error("Second step failure")
    def apply_set_second_step_failed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(second_step_error))
        return (evt, state)
    second_step_failed_evt, cmd = await create_complete_step_cmd(apply_set_second_step_failed)

    handle_res = await completestephandler.handle(step_running_event_handler, cmd)

    assert type(second_step_failed_evt) is RunningDefinitionState.Events.StepFailed
    assert second_step_failed_evt.error == second_step_error
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert second_step_failed_evt.step_id != second_step_running_evt.step_id



async def test_handle_returns_definition_completed_event_when_step_running_and_no_more_steps(definition_completed_event_handler, two_step_definition, html_response_result):
    def apply_set_second_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = CompletedWith.Data("test html content")
        return (evt, state)
    second_step_running_evt, cmd = await create_complete_step_cmd(apply_set_second_step_running)

    handle_res = await completestephandler.handle(definition_completed_event_handler, cmd)

    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    definition_completed_evt = handle_res.ok
    assert type(definition_completed_evt) is RunningDefinitionState.Events.DefinitionCompleted



async def test_handle_returns_definition_completed_event_when_step_completed_and_no_more_steps(definition_completed_event_handler, two_step_definition, html_response_result):
    def apply_set_second_step_completed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = CompletedWith.Data("test html content")
        evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(cmd_dict["result"]))
        return (evt, state)
    second_step_completed_evt, cmd = await create_complete_step_cmd(apply_set_second_step_completed)

    handle_res = await completestephandler.handle(definition_completed_event_handler, cmd)

    assert type(second_step_completed_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    definition_completed_evt = handle_res.ok
    assert type(definition_completed_evt) is RunningDefinitionState.Events.DefinitionCompleted



async def test_handle_returns_definition_completed_event_when_definition_completed(definition_completed_event_handler, two_step_definition, html_response_result):
    def apply_set_definition_completed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = CompletedWith.Data("test html content")
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(cmd_dict["result"]))
        evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        return (evt, state)
    definition_completed_evt1, cmd = await create_complete_step_cmd(apply_set_definition_completed)

    handle_res = await completestephandler.handle(definition_completed_event_handler, cmd)

    assert type(definition_completed_evt1) is RunningDefinitionState.Events.DefinitionCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    definition_completed_evt2 = handle_res.ok
    assert type(definition_completed_evt2) is RunningDefinitionState.Events.DefinitionCompleted
    assert definition_completed_evt1.result == definition_completed_evt2.result



async def test_handle_returns_no_event_when_step_canceled(runtime_error_event_handler, two_step_definition, html_response_result):
    def apply_set_second_step_canceled(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = CompletedWith.Data("test html content")
        evt = state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
        return (evt, state)
    second_step_canceled_evt, cmd = await create_complete_step_cmd(apply_set_second_step_canceled)

    handle_res = await completestephandler.handle(runtime_error_event_handler, cmd)

    assert type(second_step_canceled_evt) is RunningDefinitionState.Events.StepCanceled
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    no_evt = handle_res.ok
    assert no_evt is None



async def test_handle_returns_no_event_when_step_failed(runtime_error_event_handler, two_step_definition, html_response_result):
    def apply_set_first_step_failed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error("First step failure")))
        return (evt, state)
    first_step_failed_evt, cmd = await create_complete_step_cmd(apply_set_first_step_failed)

    handle_res = await completestephandler.handle(runtime_error_event_handler, cmd)

    assert type(first_step_failed_evt) is RunningDefinitionState.Events.StepFailed
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    no_evt = handle_res.ok
    assert no_evt is None



async def test_handle_returns_no_event_when_step_running_but_step_id_different(runtime_error_event_handler, two_step_definition, html_response_result):
    def apply_set_second_step_completed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        cmd_dict["step_id"] = IdValue.new_id()
        cmd_dict["result"] = CompletedWith.Data("test html content")
        evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(cmd_dict["result"]))
        return (evt, state)
    second_step_completed_evt, cmd = await create_complete_step_cmd(apply_set_second_step_completed)

    handle_res = await completestephandler.handle(runtime_error_event_handler, cmd)

    assert type(second_step_completed_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    no_evt = handle_res.ok
    assert no_evt is None



async def test_handle_returns_None_when_state_not_found(runtime_error_event_handler, two_step_definition):
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)
    wrong_definition_id = IdValue.new_id()
    cmd_with_wrong_definition_id = completestephandler.CompleteStepCommand(cmd.opt_task_id, cmd.run_id, wrong_definition_id, cmd.step_id, cmd.result, cmd.metadata)

    handle_res = await completestephandler.handle(runtime_error_event_handler, cmd_with_wrong_definition_id)

    assert handle_res is None



async def test_handle_passes_correct_data_to_next_step_running_event_handler(two_step_definition):
    passed_data = {}
    async def step_running_event_handler(cmd: completestephandler.CompleteStepCommand, evt):
        assert type(evt) is RunningDefinitionState.Events.StepRunning
        passed_data["cmd"] = cmd
        passed_data["input_data"] = evt.input_data
        return Result.Ok(None)
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)

    await completestephandler.handle(step_running_event_handler, cmd)

    assert cmd == passed_data["cmd"]
    assert type(cmd.result) is CompletedWith.Data
    assert cmd.result.data == passed_data["input_data"]



async def test_handle_passes_correct_data_to_definition_completed_event_handler(two_step_definition, html_response_result):
    passed_data = {}
    async def definition_completed_event_handler(cmd: completestephandler.CompleteStepCommand, evt):
        assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
        passed_data["cmd"] = cmd
        passed_data["result"] = evt.result
        return Result.Ok(None)
    def apply_set_second_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = CompletedWith.Data("test html content")
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_second_step_running)

    await completestephandler.handle(definition_completed_event_handler, cmd)

    assert cmd == passed_data["cmd"]
    assert cmd.result == passed_data["result"]



async def test_handle_does_not_invoke_event_handler_when_no_event(two_step_definition):
    event_handler_calls = []
    async def run_first_step_handler(cmd, evt):
        event_handler_calls.append(evt)
        return Result.Ok(None)
    def apply_set_first_step_failed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error("First step failure")))
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_failed)

    await completestephandler.handle(run_first_step_handler, cmd)

    assert len(event_handler_calls) == 0



async def test_handle_returns_error_when_running_definitions_storage_exception(step_running_event_handler, two_step_definition, set_running_definitions_storage_error):
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)
    set_running_definitions_storage_error(RuntimeError("Running definitions storage error"))

    handle_res = await completestephandler.handle(step_running_event_handler, cmd)

    assert type(handle_res) is Result
    assert handle_res.is_error()



async def test_handle_returns_error_when_event_handler_error(two_step_definition):
    event_handler_error = Error("Event handler error")
    async def error_event_handler(cmd, evt):
        return Result.Error(event_handler_error)
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)

    handle_res = await completestephandler.handle(error_event_handler, cmd)

    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert handle_res.error == event_handler_error



async def test_handle_raises_exception_when_event_handler_exception(two_step_definition):
    expected_ex = RuntimeError("expected exception")
    async def event_handler_with_ex(cmd, evt):
        raise expected_ex
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)

    try:
        await completestephandler.handle(event_handler_with_ex, cmd)
        assert False
    except Exception as e:
        actual_ex = e

    assert actual_ex == expected_ex