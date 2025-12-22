from collections.abc import Callable
import functools
from typing import Concatenate, ParamSpec, TypeVar
from expression import Result
import pytest

from runner import completeactionhandler
from shared.action import ActionName, ActionType
from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, Error, RunIdValue, StepIdValue
from shared.definition import ActionDefinition, Definition
from shared.infrastructure.storage.repository import NotFoundError
from shared.runningdefinition import RunningDefinitionState
from shared.runningdefinitionsstore import running_action_definitions_storage

async def step_running_event_handler(evt):
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    return Result.Ok(None)

async def definition_completed_event_handler(evt):
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
    return Result.Ok(None)

async def runtime_error_event_handler(evt):
    raise RuntimeError()

@pytest.fixture
def convert_to_storage_action():
    return running_action_definitions_storage.with_storage

@pytest.fixture
def create_complete_step_cmd(convert_to_storage_action):
    async def wrapper(storage_setup_func: Callable[[RunningDefinitionState | None, dict], tuple[RunningDefinitionState.Events.Event | None, RunningDefinitionState]]):
        run_id = RunIdValue.new_id()
        definition_id = DefinitionIdValue.new_id()
        html_data = {
            "status_code": 200,
            "content_type": "text/html",
            "content": "<html><body>test html content</body></html>"
        }
        html_response_data = CompletedWith.Data(html_data)
        cmd_dict = {
            "run_id": run_id,
            "definition_id": definition_id,
            "result": html_response_data,
            "metadata": {"command": "cmd1"}
        }
        evt = await convert_to_storage_action(storage_setup_func)(run_id, definition_id, cmd_dict)
        cmd = completeactionhandler.CompleteActionCommand(cmd_dict["run_id"], cmd_dict["definition_id"], cmd_dict["step_id"], cmd_dict["result"])
        return (evt, cmd)
    return wrapper

@pytest.fixture
def handle(convert_to_storage_action):
    return functools.partial(completeactionhandler.handle, convert_to_storage_action)

@pytest.fixture(scope="session")
def two_step_definition():
    input_data = {"url": "http://localhost", "http_method": "GET"}
    first_step_definition = ActionDefinition(ActionName("requesturl"), ActionType.CUSTOM, None)
    second_step_definition = ActionDefinition(ActionName("filtersuccessresponse"), ActionType.CUSTOM, None)
    definition = Definition(input_data, (first_step_definition, second_step_definition))
    return definition

@pytest.fixture(scope="session")
def html_response_result():
    html_data = {
        "status_code": 200,
        "content_type": "text/html",
        "content": "<html><body>test html content</body></html>"
    }
    html_response_data = CompletedWith.Data(html_data)
    return html_response_data



async def test_handle_returns_next_step_running_event_when_step_running_and_has_more_steps(create_complete_step_cmd, handle, two_step_definition):
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    first_step_running_evt, cmd = await create_complete_step_cmd(apply_set_first_step_running)
    
    handle_res = await handle(step_running_event_handler, cmd)

    assert type(first_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert first_step_running_evt.step_id != second_step_running_evt.step_id



async def test_handle_returns_next_step_running_event_when_step_completed_and_has_more_steps(create_complete_step_cmd, handle, two_step_definition, html_response_result):
    def apply_set_first_step_completed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        return (evt, state)
    first_step_completed_evt, cmd = await create_complete_step_cmd(apply_set_first_step_completed)

    handle_res = await handle(step_running_event_handler, cmd)

    assert type(first_step_completed_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert first_step_completed_evt.step_id != second_step_running_evt.step_id



async def test_handle_returns_next_step_running_event_when_next_step_running(create_complete_step_cmd, handle, two_step_definition, html_response_result):
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

    handle_res = await handle(step_running_event_handler, cmd)

    assert type(second_step_running_evt1) is RunningDefinitionState.Events.StepRunning
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt2 = handle_res.ok
    assert type(second_step_running_evt2) is RunningDefinitionState.Events.StepRunning
    assert second_step_running_evt1.step_id != second_step_running_evt2.step_id



async def test_handle_returns_next_step_running_event_when_next_step_canceled(create_complete_step_cmd, handle, two_step_definition, html_response_result):
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

    handle_res = await handle(step_running_event_handler, cmd)

    assert type(second_step_canceled_evt) is RunningDefinitionState.Events.StepCanceled
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert second_step_canceled_evt.step_id != second_step_running_evt.step_id



async def test_handle_returns_next_step_running_event_when_next_step_running_and_then_failure(create_complete_step_cmd, handle, two_step_definition, html_response_result):
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

    handle_res = await handle(step_running_event_handler, cmd)

    assert type(second_step_failed_evt) is RunningDefinitionState.Events.Failed
    assert second_step_failed_evt.error == second_step_error
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning



async def test_handle_returns_next_step_running_event_when_next_step_failed(create_complete_step_cmd, handle, two_step_definition, html_response_result):
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

    handle_res = await handle(step_running_event_handler, cmd)

    assert type(second_step_failed_evt) is RunningDefinitionState.Events.StepFailed
    assert second_step_failed_evt.error == second_step_error
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    second_step_running_evt = handle_res.ok
    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert second_step_failed_evt.step_id != second_step_running_evt.step_id



async def test_handle_returns_definition_completed_event_when_step_running_and_no_more_steps(create_complete_step_cmd, handle, two_step_definition, html_response_result):
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

    handle_res = await handle(definition_completed_event_handler, cmd)

    assert type(second_step_running_evt) is RunningDefinitionState.Events.StepRunning
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    definition_completed_evt = handle_res.ok
    assert type(definition_completed_evt) is RunningDefinitionState.Events.DefinitionCompleted



async def test_handle_returns_definition_completed_event_when_step_completed_and_no_more_steps(create_complete_step_cmd, handle, two_step_definition, html_response_result):
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

    handle_res = await handle(definition_completed_event_handler, cmd)

    assert type(second_step_completed_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    definition_completed_evt = handle_res.ok
    assert type(definition_completed_evt) is RunningDefinitionState.Events.DefinitionCompleted



async def test_handle_returns_definition_completed_event_when_definition_completed(create_complete_step_cmd, handle, two_step_definition, html_response_result):
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

    handle_res = await handle(definition_completed_event_handler, cmd)

    assert type(definition_completed_evt1) is RunningDefinitionState.Events.DefinitionCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    definition_completed_evt2 = handle_res.ok
    assert type(definition_completed_evt2) is RunningDefinitionState.Events.DefinitionCompleted
    assert definition_completed_evt1.result == definition_completed_evt2.result



async def test_handle_returns_no_event_when_step_canceled(create_complete_step_cmd, handle, two_step_definition, html_response_result):
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

    handle_res = await handle(runtime_error_event_handler, cmd)

    assert type(second_step_canceled_evt) is RunningDefinitionState.Events.StepCanceled
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    no_evt = handle_res.ok
    assert no_evt is None



async def test_handle_returns_no_event_when_step_failed(create_complete_step_cmd, handle, two_step_definition, html_response_result):
    def apply_set_first_step_failed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        cmd_dict["result"] = html_response_result
        evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error("First step failure")))
        return (evt, state)
    first_step_failed_evt, cmd = await create_complete_step_cmd(apply_set_first_step_failed)

    handle_res = await handle(runtime_error_event_handler, cmd)

    assert type(first_step_failed_evt) is RunningDefinitionState.Events.StepFailed
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    no_evt = handle_res.ok
    assert no_evt is None



async def test_handle_returns_no_event_when_step_running_but_step_id_different(create_complete_step_cmd, handle, two_step_definition, html_response_result):
    def apply_set_second_step_completed(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(html_response_result))
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())
        cmd_dict["step_id"] = StepIdValue.new_id()
        cmd_dict["result"] = CompletedWith.Data("test html content")
        evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(cmd_dict["result"]))
        return (evt, state)
    second_step_completed_evt, cmd = await create_complete_step_cmd(apply_set_second_step_completed)

    handle_res = await handle(runtime_error_event_handler, cmd)

    assert type(second_step_completed_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    no_evt = handle_res.ok
    assert no_evt is None



async def test_handle_returns_NotFoundError_when_state_not_found(create_complete_step_cmd, handle, two_step_definition):
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)
    wrong_definition_id = DefinitionIdValue.new_id()
    cmd_with_wrong_definition_id = completeactionhandler.CompleteActionCommand(cmd.run_id, wrong_definition_id, cmd.step_id, cmd.result)

    handle_res = await handle(runtime_error_event_handler, cmd_with_wrong_definition_id)

    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert type(handle_res.error) is NotFoundError



async def test_handle_passes_correct_data_to_next_step_running_event_handler(create_complete_step_cmd, handle, two_step_definition):
    passed_data = {}
    async def step_running_event_handler(evt):
        assert type(evt) is RunningDefinitionState.Events.StepRunning
        passed_data["input_data"] = evt.input_data
        return Result.Ok(None)
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)

    await handle(step_running_event_handler, cmd)

    assert type(cmd.result) is CompletedWith.Data
    assert cmd.result.data == passed_data["input_data"]



async def test_handle_passes_correct_data_to_definition_completed_event_handler(create_complete_step_cmd, handle, two_step_definition, html_response_result):
    passed_data = {}
    async def definition_completed_event_handler(evt):
        assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
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

    await handle(definition_completed_event_handler, cmd)

    assert cmd.result == passed_data["result"]



async def test_handle_does_not_invoke_event_handler_when_no_event(create_complete_step_cmd, handle, two_step_definition):
    event_handler_calls = []
    async def run_first_step_handler(evt):
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

    await handle(run_first_step_handler, cmd)

    assert len(event_handler_calls) == 0



P = ParamSpec("P")
R = TypeVar("R")
async def test_handle_returns_error_when_running_definitions_storage_exception(create_complete_step_cmd, two_step_definition):
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)
    def convert_to_storage_action(func: Callable[Concatenate[RunningDefinitionState | None, P], tuple[R, RunningDefinitionState]]):
        async def wrapper(run_id: RunIdValue, definition_id: DefinitionIdValue, *args: P.args, **kwargs: P.kwargs) -> R:
            raise RuntimeError("Running definitions storage error")
        return wrapper

    handle_res = await completeactionhandler.handle(convert_to_storage_action, step_running_event_handler, cmd)

    assert type(handle_res) is Result
    assert handle_res.is_error()



async def test_handle_returns_run_next_step_error_when_event_handler_error(create_complete_step_cmd, handle, two_step_definition):
    event_handler_error = Error("Event handler error")
    passed_data = {}
    async def error_event_handler(evt: RunningDefinitionState.Events.StepRunning):
        passed_data["step_id"] = evt.step_id
        return Result.Error(event_handler_error)
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)

    handle_res = await handle(error_event_handler, cmd)

    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert type(handle_res.error) is completeactionhandler.RunNextStepError
    assert handle_res.error.step_id == passed_data["step_id"]
    assert handle_res.error.error == event_handler_error



async def test_handle_returns_complete_definition_error_when_event_handler_error(create_complete_step_cmd, handle, two_step_definition, html_response_result):
    event_handler_error = Error("Event handler error")
    async def error_event_handler(evt):
        return Result.Error(event_handler_error)
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

    handle_res = await handle(error_event_handler, cmd)

    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert type(handle_res.error) is completeactionhandler.CompleteDefinitionError
    assert handle_res.error.error == event_handler_error



async def test_handle_raises_exception_when_event_handler_exception(create_complete_step_cmd, handle, two_step_definition):
    expected_ex = RuntimeError("expected exception")
    async def event_handler_with_ex(evt):
        raise expected_ex
    def apply_set_first_step_running(state: RunningDefinitionState | None, cmd_dict: dict):
        state = RunningDefinitionState()
        state.apply_command(RunningDefinitionState.Commands.SetDefinition(two_step_definition))
        evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        cmd_dict["step_id"] = state.running_step_id()
        return (evt, state)
    _, cmd = await create_complete_step_cmd(apply_set_first_step_running)

    try:
        await handle(event_handler_with_ex, cmd)
        assert False
    except Exception as e:
        actual_ex = e

    assert actual_ex == expected_ex
