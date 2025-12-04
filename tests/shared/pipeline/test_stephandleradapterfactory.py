from typing import Any

from expression import Result

from shared.completedresult import CompletedWith
from shared.customtypes import Metadata, RunIdValue, StepIdValue
from shared.domaindefinition import StepDefinition
from shared.pipeline.handlers import (
    StepDefinitionType,
    StepHandler,
    StepHandlerAdapterFactory,
    StepHandlerContinuation
)
from shared.pipeline.types import CompleteStepData, StepData
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

class TestStepActionHandler[TCfg, D]:
    __test__ = False  # Instruct pytest to ignore this class for test collection
    def __call__(self, func: StepHandlerContinuation[TCfg, D]):
        self._func = func
    def pass_result(self, result: Result[StepData[TCfg, Any], Any]):
        return self._func(result)



async def test_uses_handler_creator_to_create_step_handler_adapter():
    data = {"data": {"foo": "bar"}}
    expected_step_data = StepData[None, dict](RunIdValue.new_id(), StepIdValue.new_id(), TestStepDefinition(), data, Metadata())
    state = {}
    async def func(v: StepData[None, dict]):
        state["actual_step_input_data"] = v
        return CompletedWith.Data(v.data)
    step_handler = TestStepActionHandler[None, dict]()
    def handler_creator(step_definition_type: StepDefinitionType[None]) -> StepHandler[None, dict]:
        return step_handler
    async def complete_step_func(data: CompleteStepData) -> Result:
        return Result.Ok(None)

    factory = StepHandlerAdapterFactory(handler_creator, complete_step_func)
    adapter = factory(TestStepDefinition, TestStepDefinition.validate_input)
    adapter(func)

    await step_handler.pass_result(Result.Ok(expected_step_data))

    assert state["actual_step_input_data"] == expected_step_data



async def test_when_data_validator_returns_error_then_func_not_invoked():
    non_dict_data = ["data", "foo", "bar"]
    step_data_with_invalid_data = StepData[None, list](RunIdValue.new_id(), StepIdValue.new_id(), TestStepDefinition(), non_dict_data, Metadata())
    state = {}
    async def func(v: StepData[None, dict]):
        state["actual_step_input_data"] = v
        return CompletedWith.Data(v.data)
    step_handler = TestStepActionHandler[None, dict]()
    def handler_creator(step_definition_type: StepDefinitionType[None]) -> StepHandler[None, dict]:
        return step_handler
    async def complete_step_func(data: CompleteStepData) -> Result:
        return Result.Ok(None)

    factory = StepHandlerAdapterFactory(handler_creator, complete_step_func)
    adapter = factory(TestStepDefinition, TestStepDefinition.validate_input)
    adapter(func)

    await step_handler.pass_result(Result.Ok(step_data_with_invalid_data))

    assert "actual_step_input_data" not in state



async def test_when_data_validator_returns_error_then_complete_step_func_not_invoked():
    expected_run_id = RunIdValue.new_id()
    expected_step_id = StepIdValue.new_id()
    non_dict_data = ["data", "foo", "bar"]
    step_data_with_invalid_data = StepData(expected_run_id, expected_step_id, TestStepDefinition(), non_dict_data, Metadata())
    async def func(v: StepData[None, dict]):
        return CompletedWith.Data(v.data)
    step_handler = TestStepActionHandler[None, dict]()
    def handler_creator(step_definition_type: StepDefinitionType[None]) -> StepHandler[None, dict]:
        return step_handler
    state = {}
    async def complete_step_func(data: CompleteStepData) -> Result:
        state["actual_run_id"] = data.run_id
        return Result.Ok(None)

    factory = StepHandlerAdapterFactory(handler_creator, complete_step_func)
    adapter = factory(TestStepDefinition, TestStepDefinition.validate_input)
    adapter(func)

    await step_handler.pass_result(Result.Ok(step_data_with_invalid_data))

    assert "actual_run_id" not in state



async def test_uses_complete_step_func_to_create_step_handler_adapter():
    expected_run_id = RunIdValue.new_id()
    expected_step_id = StepIdValue.new_id()
    expected_data = {"data": {"foo": "bar"}}
    step_data = StepData[None, dict](expected_run_id, expected_step_id, TestStepDefinition(), expected_data, Metadata())
    async def func(v: StepData[None, dict]):
        return CompletedWith.Data(v.data)
    step_handler = TestStepActionHandler[None, dict]()
    def handler_creator(step_definition_type: StepDefinitionType[None]) -> StepHandler[None, dict]:
        return step_handler
    state = {}
    async def complete_step_func(data: CompleteStepData) -> Result:
        state["actual_run_id"] = data.run_id
        state["actual_step_id"] = data.step_id
        match data.result:
            case CompletedWith.Data(data):
                state["actual_data"] = data
        return Result.Ok(None)

    factory = StepHandlerAdapterFactory(handler_creator, complete_step_func)
    adapter = factory(TestStepDefinition, TestStepDefinition.validate_input)
    adapter(func)

    await step_handler.pass_result(Result.Ok(step_data))

    assert state["actual_run_id"] == expected_run_id
    assert state["actual_step_id"] == expected_step_id
    assert state["actual_data"] == expected_data