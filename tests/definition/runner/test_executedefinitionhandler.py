from expression import Result
import pytest

from shared.action import ActionName, ActionType
from shared.customtypes import DefinitionIdValue, RunIdValue
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
def cmd1():
    run_id = RunIdValue.new_id()
    definition_id = DefinitionIdValue.new_id()
    second_step_definition = ActionDefinition(ActionName("filtersuccessresponse"), ActionType.CUSTOM, None)
    definition = Definition((FIRST_STEP_DEFINITION, second_step_definition))
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