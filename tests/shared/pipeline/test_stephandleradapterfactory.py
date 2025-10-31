from dataclasses import dataclass
from typing import Any, Callable

from expression import Result

from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import RunIdValue, StepIdValue
from shared.domaindefinition import StepDefinition
from shared.pipeline.handlers import StepHandler, StepHandlerAdapterFactory, StepHandlerContinuation
from shared.runstepdata import RunStepData
from shared.utils.parse import parse_value

class TestStepDefinition(StepDefinition[None]):
    __test__ = False  # Instruct pytest to ignore this class for test collection
    def __init__(self):
        super().__init__(config=None)
    @property
    def input_type(self) -> type:
        return dict
    @property
    def output_type(self) -> type:
        return dict
    @staticmethod
    def validate_input(data):
        return parse_value(data, "data", lambda v: v if isinstance(v, dict) else None)

@dataclass(frozen=True)
class TestStepInputData(RunStepData[None, dict]):
    __test__ = False  # Instruct pytest to ignore this class for test collection

class TestStepActionHandler[TCfg, D]:
    __test__ = False  # Instruct pytest to ignore this class for test collection
    def __call__(self, func: StepHandlerContinuation[TCfg, D]):
        self._func = func
    def pass_result(self, result: Result[RunStepData[TCfg, D], Any]):
        return self._func(result)



async def test_uses_handler_creator_to_create_step_handler_adapter():
    data = {"data": {"foo": "bar"}}
    expected_step_input_data = TestStepInputData(RunIdValue.new_id(), StepIdValue.new_id(), None, data, {})
    state = {}
    async def func(v: RunStepData[None, dict]):
        state["actual_step_input_data"] = v
        return CompletedWith.Data(v.data)
    step_handler = TestStepActionHandler[None, dict]()
    def handler_creator(step_definition_type: type[StepDefinition[None]], data_validator: Callable[[Any], Result[dict, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, None, dict, dict], RunStepData[None, dict]]) -> StepHandler[None, dict]:
        return step_handler
    async def complete_step_func(run_id: RunIdValue, step_id: StepIdValue, completed_result: CompletedResult, metadata: dict) -> Result:
        return Result.Ok(None)

    factory = StepHandlerAdapterFactory(handler_creator, complete_step_func)
    adapter = factory(TestStepDefinition, TestStepDefinition.validate_input, TestStepInputData)
    adapter(func)

    await step_handler.pass_result(Result.Ok(expected_step_input_data))

    assert state["actual_step_input_data"] == expected_step_input_data



async def test_uses_complete_step_func_to_create_step_handler_adapter():
    expected_run_id = RunIdValue.new_id()
    expected_step_id = StepIdValue.new_id()
    expected_data = {"data": {"foo": "bar"}}
    step_input_data = TestStepInputData(expected_run_id, expected_step_id, None, expected_data, {})
    async def func(v: RunStepData[None, dict]):
        return CompletedWith.Data(v.data)
    step_handler = TestStepActionHandler[None, dict]()
    def handler_creator(step_definition_type: type[StepDefinition[None]], data_validator: Callable[[Any], Result[dict, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, None, dict, dict], RunStepData[None, dict]]) -> StepHandler[None, dict]:
        return step_handler
    state = {}
    async def complete_step_func(run_id: RunIdValue, step_id: StepIdValue, completed_result: CompletedResult, metadata: dict) -> Result:
        state["actual_run_id"] = run_id
        state["actual_step_id"] = step_id
        match completed_result:
            case CompletedWith.Data(data):
                state["actual_data"] = data
        return Result.Ok(None)

    factory = StepHandlerAdapterFactory(handler_creator, complete_step_func)
    adapter = factory(TestStepDefinition, TestStepDefinition.validate_input, TestStepInputData)
    adapter(func)

    await step_handler.pass_result(Result.Ok(step_input_data))

    assert state["actual_run_id"] == expected_run_id
    assert state["actual_step_id"] == expected_step_id
    assert state["actual_data"] == expected_data