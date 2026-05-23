import pytest
from shared.action import ActionName, ActionType
from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, Error, StepIdValue
from shared.definition import ActionDefinition, Definition
from shared.groupofrunningdefinitions import GroupOfRunningDefinitionsState, DefinitionIdWithValue

@pytest.fixture
def request_url_data():
    return {"url": "http://localhost", "http_method": "GET"}

@pytest.fixture
def two_steps():
    first_step = ActionDefinition(ActionName("requesturl"), ActionType.CUSTOM, {})
    second_step = ActionDefinition(ActionName("filtersuccessresponse"), ActionType.CUSTOM, {})
    return (first_step, second_step)

@pytest.fixture
def definition_1(request_url_data, two_steps):
    return Definition(request_url_data, two_steps)

@pytest.fixture
def definition_2(request_url_data, two_steps):
    return Definition(request_url_data | {"url": "http://b"}, two_steps)

@pytest.fixture
def definition_ids():
    return DefinitionIdValue.new_id(), DefinitionIdValue.new_id()

@pytest.fixture
def two_definitions(definition_ids, definition_1, definition_2):
    return (
        DefinitionIdWithValue(definition_ids[0], definition_1),
        DefinitionIdWithValue(definition_ids[1], definition_2)
    )

@pytest.fixture
def group_state(two_definitions):
    state = GroupOfRunningDefinitionsState()
    state.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(two_definitions))
    return state

@pytest.fixture
def running_group_state(two_definitions):
    state = GroupOfRunningDefinitionsState()
    state.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(two_definitions))
    state.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
    return state

@pytest.fixture
def test_result():
    return CompletedWith.Data("test_data")

@pytest.fixture
def test_error():
    return Error("test failure")



def test_set_definitions(two_definitions):
    state = GroupOfRunningDefinitionsState()
    
    evt = state.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(two_definitions))
    
    assert type(evt) is GroupOfRunningDefinitionsState.Events.DefinitionsAdded
    assert len(evt.definitions) == 2



def test_cant_set_definitions_when_already_set(group_state, two_definitions):
    evt = group_state.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(two_definitions))

    assert evt is None



def test_cant_set_definitions_when_already_running(running_group_state, two_definitions):
    evt = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(two_definitions))

    assert evt is None



def test_run_definitions(group_state):
    evt = group_state.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())

    assert type(evt) is GroupOfRunningDefinitionsState.Events.DefinitionsRunning
    assert len(evt.definitions) == 2
    # Verify step IDs are generated and unique
    step_ids = {rd.step_id for rd in evt.definitions}
    assert len(step_ids) == 2



def test_cant_run_definitions_without_definitions():
    state = GroupOfRunningDefinitionsState()
    
    evt = state.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
    
    assert evt is None



def test_cant_run_definitions_when_already_running(running_group_state):
    evt = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())

    assert evt is None



def test_complete_first_definition(running_group_state, test_result):
    # Capture step_id from internal projection (exposed for testing consistency)
    first_def_id = running_group_state._running_definitions[0].definition_id
    first_step_id = running_group_state._running_definitions[0].value
    
    evt = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, first_def_id, test_result))

    assert type(evt) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted
    assert evt.definition_id == first_def_id
    assert evt.result == test_result



def test_cant_complete_definition_when_not_running(running_group_state, test_result):
    # Use a random definition ID not in the running set
    fake_def_id = DefinitionIdValue.new_id()
    fake_step_id = StepIdValue.new_id()
    
    evt = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(fake_step_id, fake_def_id, test_result))

    assert evt is None



def test_cant_complete_definition_with_mismatched_step_id(running_group_state, test_result):
    first_def_id = running_group_state._running_definitions[0].definition_id
    wrong_step_id = StepIdValue.new_id()
    
    evt = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(wrong_step_id, first_def_id, test_result))
    
    assert evt is None



def test_complete_definition_idempotent_with_matching_step_id(running_group_state, test_result):
    first_def_id = running_group_state._running_definitions[0].definition_id
    first_step_id = running_group_state._running_definitions[0].value
    
    evt1 = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, first_def_id, test_result))
    # Retry with exact same command
    evt2 = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, first_def_id, test_result))
    
    assert type(evt1) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted
    assert type(evt2) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted
    assert evt1.result == evt2.result



def test_all_definitions_completed_triggered(running_group_state, test_result):
    first_def_id = running_group_state._running_definitions[0].definition_id
    first_step_id = running_group_state._running_definitions[0].value
    second_def_id = running_group_state._running_definitions[1].definition_id
    second_step_id = running_group_state._running_definitions[1].value

    # Complete first definition
    running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, first_def_id, test_result))
    # Complete second (last) definition
    evt = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(second_step_id, second_def_id, test_result))
    
    assert type(evt) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted
    # Returned value should contain populated results
    assert len(evt.results) == 2
    assert all(r.value == test_result for r in evt.results)



def test_all_definitions_completed_journal_stores_empty_results(running_group_state, test_result):
    first_def_id = running_group_state._running_definitions[0].definition_id
    first_step_id = running_group_state._running_definitions[0].value
    second_def_id = running_group_state._running_definitions[1].definition_id
    second_step_id = running_group_state._running_definitions[1].value
    # Complete both definitions to trigger terminal state
    running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, first_def_id, test_result))
    running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(second_step_id, second_def_id, test_result))
    
    # Journal should store AllDefinitionsCompleted with empty tuple
    journal_evts = running_group_state.get_events()
    terminal_evt = journal_evts[-1]

    assert type(terminal_evt) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted
    assert terminal_evt.results == ()



def test_all_definitions_completed_idempotent_returns_terminal(running_group_state, test_result):
    first_def_id = running_group_state._running_definitions[0].definition_id
    first_step_id = running_group_state._running_definitions[0].value
    second_def_id = running_group_state._running_definitions[1].definition_id
    second_step_id = running_group_state._running_definitions[1].value
    # Complete first
    running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, first_def_id, test_result))
    # Complete second (triggers terminal)
    running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(second_step_id, second_def_id, test_result))
    
    # Retry completion on already completed second definition
    evt = running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(second_step_id, second_def_id, test_result))

    assert type(evt) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted
    assert len(evt.results) == 2



def test_fail_group(group_state, test_error):
    evt = group_state.apply_command(GroupOfRunningDefinitionsState.Commands.Fail(test_error))

    assert type(evt) is GroupOfRunningDefinitionsState.Events.Failed
    assert evt.error == test_error



def test_cant_fail_uninitialized(test_error):
    state = GroupOfRunningDefinitionsState()

    evt = state.apply_command(GroupOfRunningDefinitionsState.Commands.Fail(test_error))

    assert evt is None



def test_get_events_journal_sequence(running_group_state, test_result):
    # Record step IDs
    first_def_id = running_group_state._running_definitions[0].definition_id
    first_step_id = running_group_state._running_definitions[0].value
    second_def_id = running_group_state._running_definitions[1].definition_id
    second_step_id = running_group_state._running_definitions[1].value
    
    running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, first_def_id, test_result))
    running_group_state.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(second_step_id, second_def_id, test_result))
    journal = running_group_state.get_events()

    assert len(journal) == 5  # DefinitionsAdded, DefinitionsRunning, DefinitionCompleted, DefinitionCompleted, AllDefinitionsCompleted
    assert type(journal[0]) is GroupOfRunningDefinitionsState.Events.DefinitionsAdded
    assert type(journal[1]) is GroupOfRunningDefinitionsState.Events.DefinitionsRunning
    assert type(journal[2]) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted
    assert type(journal[3]) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted
    assert type(journal[4]) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted
