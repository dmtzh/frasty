import pytest

from shared.action import ActionName, ActionType
from shared.completedresult import CompletedWith
from shared.customtypes import Error
from shared.definition import ActionDefinition, Definition
from shared.runningdefinition import RunningDefinitionState

@pytest.fixture
def request_url_data():
    return {
        "url": "http://localhost",
        "http_method": "GET"
    }

@pytest.fixture
def two_steps():
    first_step_def = ActionDefinition(ActionName("requesturl"), ActionType.CUSTOM, None)
    second_step_def = ActionDefinition(ActionName("filtersuccessresponse"), ActionType.CUSTOM, None)
    return (first_step_def, second_step_def)

@pytest.fixture
def three_steps(two_steps: tuple[ActionDefinition, ...]) -> tuple[ActionDefinition, ...]:
    third_step_def = ActionDefinition(ActionName("filterhtmlresponse"), ActionType.CUSTOM, None)
    return two_steps + (third_step_def,)

@pytest.fixture
def definition(request_url_data: dict[str, str], two_steps: tuple[ActionDefinition, ...]):
    return Definition(request_url_data, two_steps)

@pytest.fixture
def running_definition_state(definition: Definition):
    res = RunningDefinitionState()
    cmd = RunningDefinitionState.Commands.SetDefinition(definition)
    res.apply_command(cmd)
    return res

@pytest.fixture
def test_failure():
    return Error("test failure")

def completed_with_data(data: str):
    return CompletedWith.Data(data)



def test_set_definition(definition: Definition):
    state = RunningDefinitionState()
    cmd = RunningDefinitionState.Commands.SetDefinition(definition)
    evt = state.apply_command(cmd)
    assert type(evt) is RunningDefinitionState.Events.DefinitionAdded



def test_cant_set_definition_if_definition_already_set(running_definition_state: RunningDefinitionState, definition: Definition):
    cmd = RunningDefinitionState.Commands.SetDefinition(definition)
    evt = running_definition_state.apply_command(cmd)
    assert evt is None



def test_cant_set_definition_if_already_running(running_definition_state: RunningDefinitionState, definition: Definition):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    cmd = RunningDefinitionState.Commands.SetDefinition(definition)
    evt = running_definition_state.apply_command(cmd)
    assert evt is None



def test_run_first_step(definition: Definition):
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
    
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition == definition.steps[0]



def test_run_first_step_output_has_correct_input_data(definition: Definition):
    state = RunningDefinitionState()
    cmd = RunningDefinitionState.Commands.SetDefinition(definition)
    state.apply_command(cmd)
    evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.input_data == definition.input_data



def test_cant_run_first_step_without_definition():
    running_definition_state = RunningDefinitionState()
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert evt is None



def test_cant_run_first_step_if_already_running(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert evt is None



def test_cancel_first_running_step(running_definition_state: RunningDefinitionState):
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert type(evt) is RunningDefinitionState.Events.StepCanceled
    assert evt.step_id == step_running_evt.step_id # type: ignore



def test_cant_cancel_if_first_step_not_running(running_definition_state: RunningDefinitionState):
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_cant_cancel_already_canceled_first_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_run_first_step_after_cancel(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning



def test_fail_first_running_step(running_definition_state: RunningDefinitionState, test_failure: Error):
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert type(evt) is RunningDefinitionState.Events.StepFailed
    assert evt.step_id == step_running_evt.step_id # type: ignore



def test_cant_fail_if_first_step_not_running(running_definition_state: RunningDefinitionState, test_failure: Error):
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_cant_fail_already_failed_first_step(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_run_first_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning



def test_cant_cancel_first_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_cant_fail_first_step_after_cancel(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_complete_first_step(running_definition_state: RunningDefinitionState):
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    assert type(evt) is RunningDefinitionState.Events.StepCompleted
    assert evt.step_id == step_running_evt.step_id # type: ignore
    assert evt.result == result



def test_cant_complete_first_step_if_not_running(running_definition_state: RunningDefinitionState):
    result = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    assert evt is None



def test_cant_complete_already_completed_first_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    assert evt is None



def test_cant_run_first_step_after_first_step_completed(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert evt is None



def test_cant_complete_first_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    result = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    assert evt is None



def test_cant_complete_first_step_after_cancel(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    result = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    assert evt is None



def test_run_second_step(definition: Definition):
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition == definition.steps[1]



def test_run_second_step_output_has_correct_input_data(definition: Definition):
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.input_data == result.data



def test_cant_run_next_step_without_already_completed_step(running_definition_state: RunningDefinitionState):
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert evt is None



def test_cant_run_next_step_if_already_running(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert evt is None



def test_cancel_second_running_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert type(evt) is RunningDefinitionState.Events.StepCanceled
    assert evt.step_id == step_running_evt.step_id # type: ignore



def test_cant_cancel_if_second_step_not_running(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_cant_cancel_already_canceled_second_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_run_second_step_after_cancel(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning



def test_fail_second_running_step(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert type(evt) is RunningDefinitionState.Events.StepFailed
    assert evt.step_id == step_running_evt.step_id # type: ignore



def test_cant_fail_if_second_step_not_running(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_cant_fail_already_failed_second_step(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_run_second_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning



def test_cant_cancel_second_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_cant_fail_second_step_after_cancel(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_complete_second_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    assert type(evt) is RunningDefinitionState.Events.StepCompleted
    assert evt.step_id == step_running_evt.step_id # type: ignore
    assert evt.result == result2



def test_cant_complete_second_step_if_not_running(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    assert evt is None



def test_cant_complete_already_completed_second_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    assert evt is None



def test_cant_complete_second_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    assert evt is None



def test_cant_complete_second_step_after_cancel(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    assert evt is None



def test_run_next_step_issues_definition_completed_when_all_steps_completed(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted



def test_definition_completed_data_is_same_as_result_data_from_recent_completed_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
    assert type(evt.result) is CompletedWith.Data
    assert evt.result.data == result2.data



def test_multiple_run_next_step_issues_definition_completed_when_all_steps_completed(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
    assert type(evt.result) is CompletedWith.Data
    assert evt.result.data == result2.data



def test_run_third_step(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("first test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("second test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition == definition.steps[2]



def test_run_third_step_output_has_correct_input_data(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("first test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("second test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.input_data == result2.data



def test_complete_third_step(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("first test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("second test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result3 = completed_with_data("third test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result3))
    assert type(evt) is RunningDefinitionState.Events.StepCompleted
    assert evt.step_id == step_running_evt.step_id # type: ignore
    assert evt.result == result3



def test_complete_first_step_with_no_data_result(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    no_data_result = CompletedWith.NoData()
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(no_data_result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
    assert type(evt.result) is CompletedWith.NoData



def test_complete_second_step_with_error_result(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("first test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    error_result = CompletedWith.Error("test error")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(error_result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
    assert type(evt.result) is CompletedWith.Error
    assert evt.result.message == error_result.message



def test_get_events_has_no_input_data_set_in_step_running_events(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("first test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("second test_data")
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(result2))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    step_running_evts = [evt for evt in running_definition_state.get_events() if type(evt) is RunningDefinitionState.Events.StepRunning]
    assert len(step_running_evts) == 3
    assert all(evt.input_data is None for evt in step_running_evts)
    

    