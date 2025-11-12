from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.pipeline.handlers import Handler, HandlerContinuation, with_middleware

@dataclass(frozen=True)
class TestValue:
    __test__ = False  # Instruct pytest to ignore this class for test collection
    result: str

def create_handler[T](input_res: Result[T, Any]) -> Handler[T]:
    async def handler(func: HandlerContinuation[T]):
        await func(input_res)
        return None
    return handler



async def test_middleware_func_receives_input_value():
    expected_value = Result.Ok(TestValue('test result'))
    input_res = expected_value
    handler = create_handler(input_res)
    state = {}
    def decorate_with_test_middleware(decoratee: HandlerContinuation[TestValue]):
        def middleware_func(v: Result[TestValue, Any]):
            state["invoked_middleware_func_value"] = v
            return decoratee(v)
        return middleware_func
    handler_with_middleware = with_middleware(handler, decorate_with_test_middleware)
    async def handler_func(v: Result[TestValue, Any]):
        return Result.Ok("success")
    
    await handler_with_middleware(handler_func)

    actual_value = state["invoked_middleware_func_value"]
    assert actual_value == expected_value



async def test_when_middleware_func_invokes_decoratee_then_handler_func_receives_input_value():
    expected_value = Result.Ok(TestValue('test result'))
    input_res = expected_value
    handler = create_handler(input_res)
    def decorate_with_test_middleware(decoratee: HandlerContinuation[TestValue]):
        def middleware_func(v: Result[TestValue, Any]):
            return decoratee(v)
        return middleware_func
    handler_with_middleware = with_middleware(handler, decorate_with_test_middleware)
    state = {}
    async def handler_func(v: Result[TestValue, Any]):
        state["invoked_handler_func_value"] = v
        return Result.Ok("success")
    
    await handler_with_middleware(handler_func)

    actual_value = state["invoked_handler_func_value"]
    assert actual_value == expected_value



async def test_when_middleware_func_does_not_invoke_decoratee_then_handler_func_not_invoked():
    expected_value = Result.Ok(TestValue('test result'))
    input_res = expected_value
    handler = create_handler(input_res)
    def decorate_with_test_middleware(decoratee: HandlerContinuation[TestValue]):
        async def middleware_func(v: Result[TestValue, Any]):
            return Result.Error("short circuited")
        return middleware_func
    handler_with_middleware = with_middleware(handler, decorate_with_test_middleware)
    state = {}
    async def handler_func(v: Result[TestValue, Any]):
        state["invoked_handler_func_value"] = v
        return Result.Ok("success")
    
    await handler_with_middleware(handler_func)

    assert "invoked_handler_func_value" not in state



async def test_handler_receives_result_from_middleware_func():
    expected_result = Result.Ok(TestValue('test result->entered middleware->handler_func->exited middleware'))
    state = {}
    async def handler(func: HandlerContinuation[TestValue]):
        res = await func(Result.Ok(TestValue('test result')))
        state["invoked_func_result"] = res
        return None
    def decorate_with_test_middleware(decoratee: HandlerContinuation[TestValue]):
        async def middleware_func(v: Result[TestValue, Any]):
            decoratee_v = v.map(lambda tr: TestValue(f"{tr.result}->entered middleware"))
            opt_decoratee_res = await decoratee(decoratee_v)
            match opt_decoratee_res:
                case None:
                    return None
                case decoratee_res:
                    return decoratee_res.map(lambda tr: TestValue(f"{tr.result}->exited middleware"))
        return middleware_func
    handler_with_middleware = with_middleware(handler, decorate_with_test_middleware)
    async def handler_func(v: Result[TestValue, Any]):
        return v.map(lambda tr: TestValue(f"{tr.result}->handler_func"))
    
    await handler_with_middleware(handler_func)

    actual_result = state["invoked_func_result"]
    assert actual_result == expected_result