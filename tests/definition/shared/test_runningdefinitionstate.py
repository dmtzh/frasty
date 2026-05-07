import pytest

from shared.action import ActionName, ActionType
from shared.completedresult import CompletedResultAdapter, CompletedWith
from shared.customtypes import DefinitionIdValue, Error, StepIdValue
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
    first_step_def = ActionDefinition(ActionName("requesturl"), ActionType.CUSTOM, {})
    second_step_def = ActionDefinition(ActionName("filtersuccessresponse"), ActionType.CUSTOM, {})
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



def test_run_first_step_with_auto_generated_definition_id(definition: Definition):
    assert definition.steps[0].config is not None
    definition.steps[0].config["definition_id"] = "auto"
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
    
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())

    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition.config is not None
    auto_generated_definition_id = DefinitionIdValue.from_value_with_checksum(evt.step_definition.config["definition_id"])
    assert auto_generated_definition_id is not None



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



def test_run_second_step_with_auto_generated_definition_id(definition: Definition):
    assert definition.steps[1].config is not None
    definition.steps[1].config["definition_id"] = "auto"
    running_definition_state = RunningDefinitionState()
    running_definition_state.apply_command(RunningDefinitionState.Commands.SetDefinition(definition))
    running_definition_state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    result = completed_with_data("test_data")
    first_running_step_id = running_definition_state.running_step_id()
    assert first_running_step_id is not None
    running_definition_state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(first_running_step_id, result))
    
    evt = running_definition_state.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition.config is not None
    auto_generated_definition_id = DefinitionIdValue.from_value_with_checksum(evt.step_definition.config["definition_id"])
    assert auto_generated_definition_id is not None



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
    return AggregateActionDefinition(ActionName("process_item"), ActionType.CUSTOM, {})

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



def test_run_first_aggregate_step_with_auto_generated_definition_id(single_aggregate_step_definition: Definition):
    assert single_aggregate_step_definition.steps[0].config is not None
    single_aggregate_step_definition.steps[0].config["definition_id"] = "auto"
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(single_aggregate_step_definition))
    
    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    # Verify each child event has unique definition_id
    auto_generated_definition_ids = set[DefinitionIdValue]()
    for child_evt in agg_evt.child_running_events:
        assert type(child_evt) is RunningDefinitionState.Events.StepRunning
        assert child_evt.step_definition.config is not None
        auto_generated_definition_id = DefinitionIdValue.from_value_with_checksum(child_evt.step_definition.config["definition_id"])
        assert auto_generated_definition_id is not None
        auto_generated_definition_ids.add(auto_generated_definition_id)
    assert len(agg_evt.child_running_events) == len(auto_generated_definition_ids)



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
    assert agg_results[0] == CompletedResultAdapter.to_dict(res1)
    assert agg_results[1] == CompletedResultAdapter.to_dict(res2)
    assert agg_results[2] == CompletedResultAdapter.to_dict(res3)



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

    assert type(evt) is RunningDefinitionState.Events.AggregateStepCompleted
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



# -----------------------------------------------------------------------------
# Multi-Step Aggregate Tests
# -----------------------------------------------------------------------------
@pytest.fixture
def three_step_def_with_aggregate(aggregate_step_def: ActionDefinition):
    """Tuple of 3 steps where the middle step is an aggregate."""
    step1 = ActionDefinition(ActionName("fetch"), ActionType.CUSTOM, None)
    step3 = ActionDefinition(ActionName("finalize"), ActionType.CUSTOM, None)
    return (step1, aggregate_step_def, step3)

@pytest.fixture
def three_step_definition_with_aggregate(three_step_def_with_aggregate: tuple[ActionDefinition, ...]):
    """Definition with initial input data and an aggregate in position 1."""
    return Definition([{"init": True}], three_step_def_with_aggregate)

@pytest.fixture
def state_at_aggregate_start(three_step_definition_with_aggregate, list_input_data):
    """State advanced to the point where the aggregate step has just started."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(three_step_definition_with_aggregate))
    state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    
    # Step 1 completes with a list to trigger the aggregate
    step1_id = state.running_step_id()
    assert step1_id is not None
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(step1_id, CompletedWith.Data(list_input_data)))
    
    return state



def test_run_next_step_emits_aggregate_event(state_at_aggregate_start, aggregate_step_def, list_input_data):
    """Verifies RunNextStep emits AggregateStepsRunning when target step is marked aggregate."""
    child_step_def = ActionDefinition(aggregate_step_def.name, aggregate_step_def.type, aggregate_step_def.config)

    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    assert state_at_aggregate_start.running_step_id() == agg_evt.parent_step_id
    # Verify each child event has the correct input data item
    for i, child_evt in enumerate(agg_evt.child_running_events):
        assert type(child_evt) is RunningDefinitionState.Events.StepRunning
        assert child_evt.input_data == list_input_data[i]
        assert child_evt.step_definition == child_step_def



def test_run_second_aggregate_step_with_auto_generated_definition_id(three_step_definition_with_aggregate, list_input_data):
    assert three_step_definition_with_aggregate.steps[1].config is not None
    three_step_definition_with_aggregate.steps[1].config["definition_id"] = "auto"
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(three_step_definition_with_aggregate))
    state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    # Step 1 completes with a list to trigger the aggregate
    step1_id = state.running_step_id()
    assert step1_id is not None
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(step1_id, CompletedWith.Data(list_input_data)))

    agg_evt = state.apply_command(RunningDefinitionState.Commands.RunNextStep())

    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    # Verify each child event has unique definition_id
    auto_generated_definition_ids = set[DefinitionIdValue]()
    for child_evt in agg_evt.child_running_events:
        assert type(child_evt) is RunningDefinitionState.Events.StepRunning
        assert child_evt.step_definition.config is not None
        auto_generated_definition_id = DefinitionIdValue.from_value_with_checksum(child_evt.step_definition.config["definition_id"])
        assert auto_generated_definition_id is not None
        auto_generated_definition_ids.add(auto_generated_definition_id)
    assert len(agg_evt.child_running_events) == len(auto_generated_definition_ids)



def test_run_next_step_raises_value_error_when_pass_empty_input_data_to_aggregate(three_step_definition_with_aggregate):
    """Verifies that an aggregate step with empty input_data raises ValueError."""
    state = RunningDefinitionState()
    state.apply_command(RunningDefinitionState.Commands.SetDefinition(three_step_definition_with_aggregate))
    state.apply_command(RunningDefinitionState.Commands.RunFirstStep())
    step1_id = state.running_step_id()
    assert step1_id is not None
    state.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(step1_id, CompletedWith.Data([])))
    
    with pytest.raises(ValueError):
        state.apply_command(RunningDefinitionState.Commands.RunNextStep())



def test_second_aggregate_step_auto_completes_after_all_children_done(state_at_aggregate_start):
    """Verifies that completing all child steps triggers automatic parent completion."""
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    
    # Complete all children
    for child_evt in agg_evt.child_running_events:
        child_result = CompletedWith.Data({"processed": child_evt.input_data["id"]})
        final_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(child_evt.step_id, child_result))

    # The final event should be the Parent's StepCompleted
    assert type(final_evt) is RunningDefinitionState.Events.StepCompleted  
    assert final_evt.step_id == agg_evt.parent_step_id
    assert type(final_evt.result) is CompletedWith.Data
    assert state_at_aggregate_start.running_step_id() is None
    assert state_at_aggregate_start.recent_completed_step_id() == agg_evt.parent_step_id



def test_second_aggregate_step_preserves_mixed_child_result_types(state_at_aggregate_start):
    """Verifies that Data, NoData, and Error types are preserved in the aggregated list."""
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning

    children = agg_evt.child_running_events
    res1 = CompletedWith.Data("ok")
    res2 = CompletedWith.NoData()
    res3 = CompletedWith.Error("fail")
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[0].step_id, res1))
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[1].step_id, res2))
    final_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(children[2].step_id, res3))
    
    assert type(final_evt) is RunningDefinitionState.Events.StepCompleted
    assert type(final_evt.result) is CompletedWith.Data
    agg_results = final_evt.result.data
    assert type(agg_results) is list
    assert agg_results[0] == CompletedResultAdapter.to_dict(res1)
    assert agg_results[1] == CompletedResultAdapter.to_dict(res2)
    assert agg_results[2] == CompletedResultAdapter.to_dict(res3)



def test_cant_complete_directly_when_second_aggregate_step_is_running(state_at_aggregate_start):
    """Verifies that completing the parent ID directly is rejected."""
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    # Complete one child
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(agg_evt.child_running_events[0].step_id, CompletedWith.Data("partial")))
    
    # Force complete parent
    force_result = CompletedWith.Data("forced")
    evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(agg_evt.parent_step_id, force_result))
    
    assert evt is None



def test_fail_second_running_aggregate_step(state_at_aggregate_start):
    """Verifies that parent failure terminates the aggregate and rejects late children."""
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    
    err = Error("test failure")
    evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.FailRunningStep(err))
    # Late child completion rejected
    reject_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(agg_evt.child_running_events[0].step_id, CompletedWith.Data("data")))
    
    assert type(evt) is RunningDefinitionState.Events.StepFailed
    assert evt.step_id == agg_evt.parent_step_id
    assert evt.error == err
    assert state_at_aggregate_start.running_step_id() is None
    assert reject_evt is None



def test_run_second_aggregate_step_after_fail(state_at_aggregate_start):
    """Verifies that aggregate can run after failure."""
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.FailRunningStep(Error("test failure")))
    
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    assert state_at_aggregate_start.running_step_id() == agg_evt.parent_step_id



def test_cancel_second_running_aggregate_step(state_at_aggregate_start):
    """Verifies that canceling the parent terminates the aggregate and rejects late children."""
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    
    evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    # Late child completion rejected
    reject_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(agg_evt.child_running_events[0].step_id, CompletedWith.Data("data")))
    
    assert type(evt) is RunningDefinitionState.Events.StepCanceled
    assert evt.step_id == agg_evt.parent_step_id
    assert state_at_aggregate_start.running_step_id() is None
    assert reject_evt is None



def test_run_second_aggregate_step_after_cancel(state_at_aggregate_start):
    """Verifies that aggregate can run after canceled."""
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CancelRunningStep())
    
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    assert state_at_aggregate_start.running_step_id() == agg_evt.parent_step_id



def test_cant_run_next_step_when_second_aggregate_step_is_running(state_at_aggregate_start):
    """Verifies that RunNextStep returns None while aggregate children are pending."""
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    assert evt is None



def test_complete_child_step_when_second_aggregate_step_is_running(state_at_aggregate_start):
    """Verifies that completing a child emits StepCompleted."""
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    child_id = agg_evt.child_running_events[0].step_id
    
    res = CompletedWith.Data("data")
    evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(child_id, res))

    assert type(evt) is RunningDefinitionState.Events.AggregateStepCompleted
    assert evt.step_id == child_id
    assert evt.result == res



def test_cant_complete_child_step_with_mismatched_id_when_second_aggregate_step_is_running(state_at_aggregate_start):
    """Verifies that completing a non-existent child ID is rejected."""
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    
    wrong_id = StepIdValue.new_id()
    evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(wrong_id, CompletedWith.Data("data")))
    
    assert evt is None



def test_cant_complete_already_completed_child_step_when_second_aggregate_step_is_running(state_at_aggregate_start):
    """Verifies that completing the same child twice is rejected."""
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    child_id = agg_evt.child_running_events[0].step_id
    
    state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(child_id, CompletedWith.Data("first")))
    evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(child_id, CompletedWith.Data("second")))

    assert evt is None



def test_run_next_step_after_second_aggregate_step_completes(state_at_aggregate_start):
    """Verifies RunNextStep proceeds to step 3 after aggregate auto-completes."""
    agg_evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())
    assert type(agg_evt) is RunningDefinitionState.Events.AggregateStepsRunning
    # Complete all children
    child_results = []
    for child_evt in agg_evt.child_running_events:
        child_result = CompletedWith.Data({"processed": child_evt.input_data["id"]})
        child_results.append(CompletedResultAdapter.to_dict(child_result))
        state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.CompleteRunningStep(child_evt.step_id, child_result))
    
    evt = state_at_aggregate_start.apply_command(RunningDefinitionState.Commands.RunNextStep())

    assert type(evt) is RunningDefinitionState.Events.StepRunning
    assert evt.step_definition.name == "finalize"  # step3
    assert evt.input_data == child_results
