from dataclasses import dataclass
from typing import Any
from expression import Result
import pytest

from shared.completedresult import CompletedWith
from shared.customtypes import Metadata, RunIdValue, StepIdValue
from shared.domaindefinition import StepDefinition
from shared.pipeline.handlers import StepHandlerAdapter, StepHandlerContinuation
from shared.pipeline.types import CompleteStepData, StepData
from shared.utils.parse import parse_value

@dataclass(frozen=True)
class TestStepData(StepData[None, dict]):
    __test__ = False  # Instruct pytest to ignore this class for test collection

class TestStepActionHandler[TCfg, D]:
    __test__ = False  # Instruct pytest to ignore this class for test collection
    def __call__(self, func: StepHandlerContinuation[TCfg, D]):
        self._func = func
    def pass_result(self, result: Result[StepData[TCfg, D], Any]):
        return self._func(result)

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

@pytest.fixture
def step_data():
    data = {"data": {"foo": "bar"}}
    return TestStepData(RunIdValue.new_id(), StepIdValue.new_id(), TestStepDefinition(), data, Metadata())



async def test_when_handler_pass_success_result_then_func_invokes_with_success_result_value(step_data: TestStepData):
    expected_step_data = step_data
    step_handler = TestStepActionHandler[None, dict]()
    state = {}
    async def complete_step_func(data: CompleteStepData) -> Result:
        return Result.Ok(None)
    async def func(v: StepData[None, dict]):
        state["actual_step_data"] = v
        return CompletedWith.Data(v.data)
    StepHandlerAdapter(step_handler, complete_step_func)(func)

    await step_handler.pass_result(Result.Ok(step_data))

    assert state["actual_step_data"] == expected_step_data



async def test_when_handler_pass_error_result_then_func_not_invoked():
    step_handler = TestStepActionHandler[None, dict]()
    async def complete_step_func(data: CompleteStepData) -> Result:
        return Result.Ok(None)
    state = {}
    async def func(v: StepData[None, dict]):
        state["actual_step_data"] = v
        return CompletedWith.Data(v.data)
    StepHandlerAdapter(step_handler, complete_step_func)(func)

    await step_handler.pass_result(Result.Error("Test error"))

    assert "actual_step_data" not in state



async def test_when_handler_pass_error_result_then_complete_step_func_not_invoked():
    step_handler = TestStepActionHandler[None, dict]()
    state = {}
    async def complete_step_func(data: CompleteStepData) -> Result:
        state["actual_completed_result"] = data.result
        return Result.Ok(None)
    async def func(v: StepData[None, dict]):
        return CompletedWith.Data(v.data)
    StepHandlerAdapter(step_handler, complete_step_func)(func)

    await step_handler.pass_result(Result.Error("Test error"))

    assert "actual_completed_result" not in state



async def test_when_func_returns_completed_result_then_result_is_passed_to_complete_step_func(step_data: TestStepData):
    expected_completed_result = CompletedWith.Data(step_data.data)
    step_handler = TestStepActionHandler[None, dict]()
    state = {}
    async def complete_step_func(data: CompleteStepData) -> Result:
        state["actual_completed_result"] = data.result
        return Result.Ok(None)
    async def func(v: StepData[None, dict]):
        return expected_completed_result
    StepHandlerAdapter(step_handler, complete_step_func)(func)

    await step_handler.pass_result(Result.Ok(step_data))

    assert state["actual_completed_result"] == expected_completed_result



async def test_when_func_returns_none_then_complete_step_func_not_invoked(step_data: TestStepData):
    step_handler = TestStepActionHandler[None, dict]()
    state = {}
    async def complete_step_func(data: CompleteStepData) -> Result:
        state["actual_completed_result"] = data.result
        return Result.Ok(None)
    async def func(v: StepData[None, dict]):
        return None
    StepHandlerAdapter(step_handler, complete_step_func)(func)

    await step_handler.pass_result(Result.Ok(step_data))

    assert "actual_completed_result" not in state