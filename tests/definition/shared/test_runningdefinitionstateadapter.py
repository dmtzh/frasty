from shared.domainrunning import RunningDefinitionState
from shared.dtorunning import RunningDefinitionStateAdapter
from shared.completedresult import CompletedWith
from shared.domaindefinition import Definition
from stepdefinitions.html import FilterHtmlResponse
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl

def test_to_list_and_back():
    request_url_data = {
        "url": "http://localhost",
        "http_method": "GET"
    }
    first_step_def = RequestUrl()
    second_step_def = FilterSuccessResponse()
    third_step_def = FilterHtmlResponse()
    three_steps = [first_step_def, second_step_def, third_step_def]
    definition = Definition.from_steps(request_url_data, three_steps).ok
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = CompletedWith.Data("first test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = CompletedWith.Data("second test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())

    list_state = RunningDefinitionStateAdapter.to_list(running_definition_state)
    restored_state = RunningDefinitionStateAdapter.from_list(list_state).ok

    assert type(restored_state) is RunningDefinitionState
    assert restored_state.recent_completed_step_id() == running_definition_state.recent_completed_step_id()
    assert restored_state.running_step_id() == running_definition_state.running_step_id()
    assert len(restored_state.get_events()) == len(running_definition_state.get_events())