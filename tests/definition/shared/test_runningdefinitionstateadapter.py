from shared.action import ActionName, ActionType
from shared.completedresult import CompletedWith
from shared.definition import ActionDefinition, AggregateActionDefinition, Definition
from shared.runningdefinition import RunningDefinitionState, RunningDefinitionStateAdapter


def test_to_list_and_back():
    request_url_data = {
        "url": "http://localhost",
        "http_method": "GET"
    }
    first_step_def = ActionDefinition(ActionName("requesturl"), ActionType.CUSTOM, None)
    second_step_def = ActionDefinition(ActionName("filtersuccessresponse"), ActionType.CUSTOM, None)
    third_step_def = ActionDefinition(ActionName("filterhtmlresponse"), ActionType.CUSTOM, None)
    three_steps = (first_step_def, second_step_def, third_step_def)
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = CompletedWith.Data("first test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = CompletedWith.Data("second test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())

    list_state = RunningDefinitionStateAdapter.to_list(running_definition_state)
    restored_state = RunningDefinitionStateAdapter.from_list(list_state).ok

    assert type(restored_state) is RunningDefinitionState
    assert restored_state.recent_completed_step_id() == running_definition_state.recent_completed_step_id()
    assert restored_state.running_step_id() == running_definition_state.running_step_id()
    assert len(restored_state.get_events()) == len(running_definition_state.get_events())



def test_to_list_and_back_with_aggregate_action():
    get_links_data = {
        "id": "123"
    }
    first_step_def = ActionDefinition(ActionName("getlinks"), ActionType.CUSTOM, None)
    second_aggr_step_def = AggregateActionDefinition(ActionName("requesturl"), ActionType.CUSTOM, None)
    two_steps = (first_step_def, second_aggr_step_def)
    definition = Definition(get_links_data, two_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
    list_state = RunningDefinitionStateAdapter.to_list(running_definition_state)
    running_definition_state = RunningDefinitionStateAdapter.from_list(list_state).ok
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    list_state = RunningDefinitionStateAdapter.to_list(running_definition_state)
    running_definition_state = RunningDefinitionStateAdapter.from_list(list_state).ok
    result1 = CompletedWith.Data([{"url": "http://localhost/1","http_method": "GET"},{"url": "http://localhost/2","http_method": "GET"}])
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    list_state = RunningDefinitionStateAdapter.to_list(running_definition_state)
    running_definition_state = RunningDefinitionStateAdapter.from_list(list_state).ok
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    list_state = RunningDefinitionStateAdapter.to_list(running_definition_state)
    restored_state = RunningDefinitionStateAdapter.from_list(list_state).ok

    assert type(restored_state) is RunningDefinitionState
    assert restored_state.recent_completed_step_id() == running_definition_state.recent_completed_step_id()
    assert restored_state.running_step_id() == running_definition_state.running_step_id()
    assert len(restored_state.get_events()) == len(running_definition_state.get_events())