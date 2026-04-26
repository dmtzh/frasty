import pytest

from shared.action import ActionName, ActionType
from shared.completedresult import CompletedWith
from shared.customtypes import Error, StepIdValue
from shared.definition import ActionDefinition, AggregateActionDefinition, Definition
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



def test_cant_set_definition_when_definition_already_set(running_definition_state: RunningDefinitionState, definition: Definition):
    cmd = RunningDefinitionState.Commands.SetDefinition(definition)
    evt = running_definition_state.apply_command(cmd)
    assert evt is None



def test_cant_set_definition_when_already_running(running_definition_state: RunningDefinitionState, definition: Definition):
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



def test_cant_run_first_step_when_already_running(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert evt is None



def test_cancel_first_running_step(running_definition_state: RunningDefinitionState):
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert type(evt) is RunningDefinitionState.Events.StepCanceled
    assert evt.step_id == step_running_evt.step_id # type: ignore



def test_cant_cancel_when_first_step_not_running(running_definition_state: RunningDefinitionState):
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



def test_cant_fail_when_first_step_not_running(running_definition_state: RunningDefinitionState, test_failure: Error):
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
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    assert type(evt) is RunningDefinitionState.Events.StepCompleted
    assert evt.step_id == step_running_evt.step_id # type: ignore
    assert evt.result == result



def test_cant_complete_first_step_when_not_running(running_definition_state: RunningDefinitionState):
    result = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(StepIdValue.new_id(), result))
    assert evt is None



def test_cant_complete_already_completed_first_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result2))
    assert evt is None



def test_cant_run_first_step_after_first_step_completed(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert evt is None



def test_cant_complete_first_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    result = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    assert evt is None



def test_cant_complete_first_step_after_cancel(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    result = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    assert evt is None



def test_cant_complete_first_step_with_mismatched_step_id(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    wrong_running_step_id = StepIdValue.new_id()
    result = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(wrong_running_step_id, result))
    assert evt is None



def test_run_second_step(definition: Definition):
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition == definition.steps[1]



def test_run_second_step_output_has_correct_input_data(definition: Definition):
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.input_data == result.data



def test_cant_run_next_step_without_already_completed_step(running_definition_state: RunningDefinitionState):
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert evt is None



def test_cant_run_next_step_when_already_running(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert evt is None



def test_cancel_second_running_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert type(evt) is RunningDefinitionState.Events.StepCanceled
    assert evt.step_id == step_running_evt.step_id # type: ignore



def test_cant_cancel_when_second_step_not_running(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_cant_cancel_already_canceled_second_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_run_second_step_after_cancel(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning



def test_fail_second_running_step(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert type(evt) is RunningDefinitionState.Events.StepFailed
    assert evt.step_id == step_running_evt.step_id # type: ignore



def test_cant_fail_when_second_step_not_running(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_cant_fail_already_failed_second_step(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_run_second_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning



def test_cant_cancel_second_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    assert evt is None



def test_cant_fail_second_step_after_cancel(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    assert evt is None



def test_complete_second_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    assert type(evt) is RunningDefinitionState.Events.StepCompleted
    assert evt.step_id == step_running_evt.step_id # type: ignore
    assert evt.result == result2



def test_cant_complete_second_step_when_not_running(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result2))
    assert evt is None



def test_cant_complete_already_completed_second_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    assert evt is None



def test_cant_complete_second_step_after_fail(running_definition_state: RunningDefinitionState, test_failure: Error):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.FailRunningStep(test_failure))
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    assert evt is None



def test_cant_complete_second_step_after_cancel(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    result2 = completed_with_data("test_data")
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    assert evt is None



def test_run_next_step_emits_definition_completed_when_all_steps_completed(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted



def test_definition_completed_data_is_same_as_result_data_from_recent_completed_step(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
    assert type(evt.result) is CompletedWith.Data
    assert evt.result.data == result2.data



def test_multiple_run_next_step_emits_definition_completed_when_all_steps_completed(running_definition_state: RunningDefinitionState):
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
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
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("second test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition == definition.steps[2]



def test_run_third_step_output_has_correct_input_data(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("first test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("second test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.input_data == result2.data



def test_complete_third_step(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("first test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("second test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    step_running_evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result3 = completed_with_data("third test_data")
    third_running_step_id = running_definition_state.running_step_id()
    assert third_running_step_id is not None
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(third_running_step_id, result3))
    assert type(evt) is RunningDefinitionState.Events.StepCompleted
    assert evt.step_id == step_running_evt.step_id # type: ignore
    assert evt.result == result3



def test_complete_first_step_with_no_data_result(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    no_data_result = CompletedWith.NoData()
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, no_data_result))
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(evt) is RunningDefinitionState.Events.DefinitionCompleted
    assert type(evt.result) is CompletedWith.NoData



def test_complete_second_step_with_error_result(request_url_data: dict[str, str], three_steps: tuple[ActionDefinition, ...]):
    definition = Definition(request_url_data, three_steps)
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))

    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result1 = completed_with_data("first test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    error_result = CompletedWith.Error("test error")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, error_result))
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
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result1))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    result2 = completed_with_data("second test_data")
    second_running_step_id = running_definition_state.running_step_id()
    assert second_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(second_running_step_id, result2))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    step_running_evts = [evt for evt in running_definition_state.get_events() if type(evt) is RunningDefinitionState.Events.StepRunning]
    assert len(step_running_evts) == 3
    assert all(evt.input_data is None for evt in step_running_evts)



# -----------------------------------------------------------------------------
# Single-Step Aggregate Tests
# -----------------------------------------------------------------------------
@pytest.fixture
def aggregate_step_def():
    """Creates an ActionDefinition marked as an aggregate."""
    return AggregateActionDefinition(ActionName("process_item"), ActionType.CUSTOM, None)

@pytest.fixture
def list_input_data():
    """Standard list input for aggregate tests."""
    return [{"id": 1}, {"id": 2}, {"id": 3}]

@pytest.fixture
def single_aggregate_step_definition(list_input_data: list[dict], aggregate_step_def: AggregateActionDefinition):
    """A definition with a single aggregate step."""
    return Definition(list_input_data, (aggregate_step_def,))



def test_run_first_step_emits_aggregate_event(single_aggregate_step_definition: Definition):
    """Verifies that starting an aggregate step emits AggregateStepsRunning instead of StepRunning."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    child_step_def = ActionDefinition(single_aggregate_step_definition.steps[0].name, single_aggregate_step_definition.steps[0].type, single_aggregate_step_definition.steps[0].config)
    
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    assert agg_evt.parent_step_id is not None
    # Verify each child event has the correct input data item
    assert type(single_aggregate_step_definition.input_data) is list
    for i, child_evt in enumerate(agg_evt.child_running_events):
        assert type(child_evt) is RunningDefinitionState.Events.StepRunning
        assert child_evt.input_data == single_aggregate_step_definition.input_data[i]
        assert child_evt.step_definition == child_step_def



def test_run_first_step_raises_value_error_when_pass_empty_input_data_to_aggregate(aggregate_step_def: AggregateActionDefinition):
    """Verifies that an aggregate step with empty input_data raises ValueError."""
    empty_def = Definition([], (aggregate_step_def,))
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(empty_def))

    with pytest.raises(ValueError):
        state.apply_command(RunningDefinitionState.Commands.RunFirstStep())



def test_first_aggregate_step_auto_completes_after_all_children_done(single_aggregate_step_definition: Definition):
    """Verifies that completing all child steps triggers automatic parent completion."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    
    # Complete all children
    children = agg_evt.child_running_events
    res = [CompletedWith.Data({"processed": child_evt.input_data["id"]}) for child_evt in children]
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[0].step_id, res[0]))
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[1].step_id, res[1]))
    final_evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[2].step_id, res[2]))
    
    # The final event should be the Parent's StepCompleted
    assert type(final_evt) is RunningDefinitionState.Events.StepCompleted
    assert final_evt.step_id == agg_evt.parent_step_id
    assert type(final_evt.result) is CompletedWith.Data
    # Verify state cleanup
    assert state.running_step_id() is None
    assert state.recent_completed_step_id() == agg_evt.parent_step_id



def test_first_aggregate_step_preserves_mixed_child_result_types(single_aggregate_step_definition: Definition):
    """Verifies that Data, NoData, and Error types are preserved in the aggregated list."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    
    children = agg_evt.child_running_events
    res1 = CompletedWith.Data("ok")
    res2 = CompletedWith.NoData()
    res3 = CompletedWith.Error("fail")
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[0].step_id, res1))
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[1].step_id, res2))
    final_evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[2].step_id, res3))
    
    assert type(final_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(final_evt.result) is CompletedWith.Data
    agg_results = final_evt.result.data
    assert type(agg_results) is list
    assert agg_results[0] == res1
    assert agg_results[1] == res2
    assert agg_results[2] == res3



def test_cant_complete_directly_when_first_aggregate_step_is_running(single_aggregate_step_definition: Definition):
    """Verifies that completing the parent ID directly is rejected."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    # Complete one child
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(agg_evt.child_running_events[0].step_id, CompletedWith.Data("partial")))
    
    # Force complete parent
    force_result = CompletedWith.Data("forced")
    evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(agg_evt.parent_step_id, force_result))
    
    assert evt is None



def test_fail_first_running_aggregate_step(single_aggregate_step_definition: Definition):
    """Verifies that parent failure terminates the aggregate and rejects late children."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    
    err = Error("test failure")
    evt = state.apply_command(RunningDefinitionState.Commands.FailRunningStep(err))
    # Late child completion rejected
    reject_evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(agg_evt.child_running_events[0].step_id, CompletedWith.Data("data")))
    
    assert type(evt) is RunningDefinitionState.Events.StepFailed
    assert evt.step_id == agg_evt.parent_step_id
    assert evt.error == err
    assert state.running_step_id() is None
    assert reject_evt is None



def test_run_first_aggregate_step_after_fail(single_aggregate_step_definition: Definition):
    """Verifies that aggregate can run after failure."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    state.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error("test failure")))
    
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    assert state.running_step_id() == agg_evt.parent_step_id



def test_cancel_first_running_aggregate_step(single_aggregate_step_definition: Definition):
    """Verifies that canceling the parent terminates the aggregate and rejects late children."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    
    evt = state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    # Late child completion rejected
    reject_evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(agg_evt.child_running_events[0].step_id, CompletedWith.Data("data")))
    
    assert type(evt) is RunningDefinitionState.Events.StepCanceled
    assert evt.step_id == agg_evt.parent_step_id
    assert state.running_step_id() is None
    assert reject_evt is None



def test_run_first_aggregate_step_after_cancel(single_aggregate_step_definition: Definition):
    """Verifies that aggregate can run after canceled."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    state.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    assert state.running_step_id() == agg_evt.parent_step_id



def test_cant_run_next_step_when_first_aggregate_step_is_running(single_aggregate_step_definition: Definition):
    """Verifies that RunNextStep returns None while aggregate children are pending."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    
    evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    assert evt is None



def test_complete_child_step_when_first_aggregate_step_is_running(single_aggregate_step_definition: Definition):
    """Verifies that completing a child emits StepCompleted."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    child_id = agg_evt.child_running_events[0].step_id
    
    res = CompletedWith.Data("data")
    evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(child_id, res))

    assert type(evt) is RunningDefinitionState.Events.StepCompleted
    assert evt.step_id == child_id
    assert evt.result == res



def test_cant_complete_child_step_with_mismatched_id_when_first_aggregate_step_is_running(single_aggregate_step_definition: Definition):
    """Verifies that completing a non-existent child ID is rejected."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    
    wrong_id = StepIdValue.new_id()
    evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(wrong_id, CompletedWith.Data("data")))
    
    assert evt is None



def test_cant_complete_already_completed_child_step_when_first_aggregate_step_is_running(single_aggregate_step_definition: Definition):
    """Verifies that completing the same child twice is rejected."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    child_id = agg_evt.child_running_events[0].step_id
    
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(child_id, CompletedWith.Data("first")))
    evt = state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(child_id, CompletedWith.Data("second")))

    assert evt is None
