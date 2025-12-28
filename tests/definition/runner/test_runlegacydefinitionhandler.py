from expression import Result
import pytest
import pytest_asyncio

from shared.domainrunning import RunningDefinitionState
from shared.definitionsstore import legacy_definitions_storage
from shared.infrastructure.storage.repository import NotFoundError
from shared.runningdefinitionsstore import running_definitions_storage
from shared.completedresult import CompletedWith
from runner import runlegacydefinitionhandler
from shared.customtypes import DefinitionIdValue, Error, RunIdValue
from shared.domaindefinition import Definition
from stepdefinitions.html import GetContentFromHtml, GetContentFromHtmlConfig, GetLinksFromHtml, GetLinksFromHtmlConfig
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl

@pytest.fixture
def run_first_step_handler():
    async def handler(cmd, evt):
        return Result.Ok(None)
    return handler

@pytest.fixture(scope="session")
def definition1():
    input_data = FIRST_STEP_INPUT_DATA
    first_step_def = FIRST_STEP_DEFINITION
    second_step_def = FilterSuccessResponse()
    two_steps_def = [first_step_def, second_step_def]
    definition = Definition.from_steps(input_data, two_steps_def).ok
    return definition

@pytest.fixture(scope="session")
def definition2():
    input_data = {"content": "html content"}
    first_step_def_config = GetContentFromHtmlConfig(css_selector="a", regex_selector=None, output_name=None)
    first_step_def = GetContentFromHtml(first_step_def_config)
    second_step_def_config = GetLinksFromHtmlConfig(None, None)
    second_step_def = GetLinksFromHtml(second_step_def_config)
    two_steps_def = [first_step_def, second_step_def]
    definition = Definition.from_steps(input_data, two_steps_def).ok
    return definition

@pytest_asyncio.fixture(scope="session")
async def existing_definition_id1(definition1: Definition):
    definition_id = DefinitionIdValue.new_id()
    await legacy_definitions_storage.add(definition_id, definition1)
    yield definition_id

@pytest_asyncio.fixture(scope="session")
async def existing_definition_id2(definition2: Definition):
    definition_id = DefinitionIdValue.new_id()
    await legacy_definitions_storage.add(definition_id, definition2)
    yield definition_id

@pytest.fixture
def run_id():
    return RunIdValue.new_id()

@pytest.fixture
def cmd1(existing_definition_id1, run_id):
    cmd = runlegacydefinitionhandler.RunDefinitionCommand(definition_id=existing_definition_id1, run_id=run_id)
    return cmd

@pytest.fixture
def cmd2(existing_definition_id2, run_id):
    cmd = runlegacydefinitionhandler.RunDefinitionCommand(definition_id=existing_definition_id2, run_id=run_id)
    return cmd

FIRST_STEP_INPUT_DATA = {"url": "http://localhost", "http_method": "GET"}
FIRST_STEP_DEFINITION = RequestUrl()



async def test_handle_returns_first_step_running_event(run_first_step_handler, cmd1):
    handle_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.input_data == FIRST_STEP_INPUT_DATA
    assert type(evt.step_definition) is type(FIRST_STEP_DEFINITION)



async def test_handle_returns_new_first_step_running_event_when_first_step_already_running(cmd1, definition1, run_first_step_handler):
    def run_first_step(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition1))
        evt = new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        return (evt, new_state)
    
    running_evt = await running_definitions_storage.with_storage(run_first_step)(cmd1.run_id, cmd1.definition_id)
    handle_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert type(running_evt) is RunningDefinitionState.Events.StepRunning
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_id != running_evt.step_id



async def test_handle_returns_first_step_running_event_when_first_step_canceled(cmd1, definition1, run_first_step_handler):
    def run_first_step_then_cancel(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition1))
        new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        evt = new_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
        return (evt, new_state)
    
    canceled_evt = await running_definitions_storage.with_storage(run_first_step_then_cancel)(cmd1.run_id, cmd1.definition_id)
    handle_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert type(canceled_evt) is RunningDefinitionState.Events.StepCanceled
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_id != canceled_evt.step_id



async def test_handle_returns_first_step_running_event_when_first_step_failed(cmd1, definition1, run_first_step_handler):
    def run_first_step_then_fail(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition1))
        new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        evt = new_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error("some error")))
        return (evt, new_state)
    
    failed_evt = await running_definitions_storage.with_storage(run_first_step_then_fail)(cmd1.run_id, cmd1.definition_id)
    handle_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert type(failed_evt) is RunningDefinitionState.Events.StepFailed
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_id != failed_evt.step_id



async def test_handle_returns_no_event_when_first_step_already_completed(cmd1, definition1, run_first_step_handler):
    def run_first_step_then_complete(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition1))
        new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        evt = new_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(CompletedWith.Data("completed result data")))
        return (evt, new_state)
    
    completed_evt = await running_definitions_storage.with_storage(run_first_step_then_complete)(cmd1.run_id, cmd1.definition_id)
    handle_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert type(completed_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert evt is None



async def test_handle_returns_NotFoundError_when_definition_id_does_not_exist(run_first_step_handler):
    def_id = DefinitionIdValue.new_id()
    run_id = RunIdValue.new_id()
    cmd = runlegacydefinitionhandler.RunDefinitionCommand(definition_id=def_id, run_id=run_id)
    
    handle_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd)

    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert type(handle_res.error) is NotFoundError



async def test_handle_passes_correct_data_to_run_first_step_handler(cmd1):
    passed_data = {}
    async def run_first_step_handler(evt: RunningDefinitionState.Events.StepRunning, def_ver):
        passed_data["input_data"] = evt.input_data
        passed_data["step_definition"] = evt.step_definition
        passed_data["def_ver"] = def_ver
        return Result.Ok(None)
    
    await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert FIRST_STEP_INPUT_DATA == passed_data["input_data"]
    assert type(FIRST_STEP_DEFINITION) is type(passed_data["step_definition"])
    assert 1 == passed_data["def_ver"]



async def test_handle_returns_error_when_run_first_step_handler_error(cmd1):
    expected_error = Error("expected error")
    async def run_first_step_handler_with_err(cmd, evt):
        return Result.Error(expected_error)
    
    handle_res = await runlegacydefinitionhandler.handle(run_first_step_handler_with_err, cmd1)

    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert handle_res.error.error == expected_error



async def test_handle_raises_exception_when_run_first_step_handler_exception(cmd1):
    expected_ex = RuntimeError("expected exception")
    async def run_first_step_handler_with_ex(evt, def_ver):
        raise expected_ex
    
    try:
        await runlegacydefinitionhandler.handle(run_first_step_handler_with_ex, cmd1)
        assert False
    except Exception as e:
        actual_ex = e

    assert actual_ex == expected_ex



async def test_handle_does_not_invoke_run_first_step_handler_when_first_step_already_completed(cmd1, definition1):
    run_first_step_handler_calls = []
    async def run_first_step_handler(cmd, evt):
        run_first_step_handler_calls.append(evt)
        return Result.Ok(None)
    def run_then_complete_first_step(state: RunningDefinitionState | None):
        if state is not None:
            raise RuntimeError()
        new_state = RunningDefinitionState()
        new_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition1))
        new_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
        completed_step_result = CompletedWith.Data("completed result data")
        evt = new_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(completed_step_result))
        return (evt, new_state)
    
    await running_definitions_storage.with_storage(run_then_complete_first_step)(cmd1.run_id, cmd1.definition_id)
    await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert len(run_first_step_handler_calls) == 0



async def test_handle_returns_error_when_running_definitions_storage_exception(run_first_step_handler, cmd1, set_running_definitions_storage_error):
    set_running_definitions_storage_error(RuntimeError("Running definitions storage error"))

    handle_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert type(handle_res) is Result
    assert handle_res.is_error()



async def test_handle_returns_error_when_definitions_storage_exception(run_first_step_handler, cmd1, set_definitions_storage_error):
    set_definitions_storage_error(RuntimeError("Definitions storage error"))

    res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)

    assert type(res) is Result
    assert res.is_error()



async def test_handle_returns_two_different_first_step_running_events_when_invoked_with_same_run_id_but_different_definition_id(run_first_step_handler, cmd1, cmd2):
    handle1_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd1)
    handle2_res = await runlegacydefinitionhandler.handle(run_first_step_handler, cmd2)

    assert cmd1.run_id == cmd2.run_id
    assert type(handle1_res) is Result
    assert type(handle2_res) is Result
    assert handle1_res.is_ok()
    assert handle2_res.is_ok()
    evt1 = handle1_res.ok
    evt2 = handle2_res.ok
    assert type(evt1) is RunningDefinitionState.Events.StepRunning
    assert type(evt2) is RunningDefinitionState.Events.StepRunning
    assert evt1.step_id != evt2.step_id