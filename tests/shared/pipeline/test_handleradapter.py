from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any, Callable

from expression import Result

from shared.pipeline.handlers import HandlerAdapter

@dataclass(frozen=True)
class TestValue:
    __test__ = False  # Instruct pytest to ignore this class for test collection
    result: str

class TestHandler:
    __test__ = False  # Instruct pytest to ignore this class for test collection

    def __call__(self, func: Callable[[Result[TestValue, Any]], Coroutine[Any, Any, Result | None]]):
        self._func = func
    
    def pass_result(self, result: Result[TestValue, Any]):
        return self._func(result)



async def test_when_handler_pass_success_result_then_func_invokes_with_success_result_value():
    expected_value = TestValue('test result')
    state = {}
    async def func(v: TestValue):
        state["invoked_func_value"] = v
        return Result.Ok("success")
    handler = TestHandler()
    HandlerAdapter(handler)(func)

    await handler.pass_result(Result.Ok(expected_value))

    actual_value = state["invoked_func_value"]
    assert actual_value == expected_value



async def test_when_handler_pass_error_result_then_func_not_invoked():
    state = {}
    async def func(v: TestValue):
        state["invoked_func_value"] = v
        return Result.Ok("success")
    handler = TestHandler()
    HandlerAdapter(handler)(func)

    await handler.pass_result(Result.Error("Test error"))

    assert "invoked_func_value" not in state