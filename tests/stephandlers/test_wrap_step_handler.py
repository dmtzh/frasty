from dataclasses import dataclass

from expression import Result

from shared.completedresult import CompletedResult, CompletedWith
from stephandlers.wrapper import wrap_step_handler

@dataclass(frozen=True)
class TestValue:
    __test__ = False  # Instruct pytest to ignore this class for test collection
    result: str

async def step_handler_w_data(v: TestValue):
    return CompletedWith.Data(v)

async def send_response_success_handler(v: TestValue, completed_res: CompletedResult):
    return Result.Ok(None)



async def test_wrap_step_handler_when_pass_success_result_then_invokes_handler_with_success_result_value():
    state = {}
    async def invoked_step_handler(v: TestValue):
        state["invoked_step_handler_value"] = v
        return CompletedWith.NoData()
    expected_value = TestValue('test result')
    succ_res = Result.Ok(expected_value)
    handler = wrap_step_handler(invoked_step_handler)

    await handler(send_response_success_handler, succ_res)
    actual_value = state["invoked_step_handler_value"]

    assert actual_value == expected_value



async def test_wrap_step_handler_when_pass_error_result_then_handler_is_not_invoked():
    state = {}
    async def invoked_step_handler(v: TestValue):
        state["invoked_step_handler_value"] = v
        return CompletedWith.NoData()
    err_res = Result.Error("Test error")
    handler = wrap_step_handler(invoked_step_handler)

    await handler(send_response_success_handler, err_res)

    assert "invoked_step_handler_value" not in state



async def test_wrap_step_handler_when_pass_error_result_then_returns_none():
    err_res = Result.Error("Test error")
    handler = wrap_step_handler(step_handler_w_data)

    actual_res = await handler(send_response_success_handler, err_res)

    assert actual_res is None



async def test_wrap_step_handler_when_handler_returns_completed_result_then_returns_success_completed_result():
    value = TestValue('test result')
    succ_res = Result.Ok(value)
    handler = wrap_step_handler(step_handler_w_data)

    actual_res = await handler(send_response_success_handler, succ_res)

    assert type(actual_res) is Result
    assert actual_res.is_ok()
    assert type(actual_res.ok) is CompletedWith.Data



async def test_wrap_step_handler_when_pass_success_result_then_invokes_send_response_handler_with_success_result_value():
    state = {}
    async def invoked_send_response_success_handler(v: TestValue, completed_res: CompletedResult):
        state["invoked_step_handler_value"] = v
        return Result.Ok(None)
    expected_value = TestValue('test result')
    succ_res = Result.Ok(expected_value)
    handler = wrap_step_handler(step_handler_w_data)

    await handler(invoked_send_response_success_handler, succ_res)
    actual_value = state["invoked_step_handler_value"]

    assert actual_value == expected_value



async def test_wrap_step_handler_when_handler_returns_completed_result_then_invokes_send_response_handler_with_this_completed_result():
    state = {}
    async def invoked_step_handler(v: TestValue):
        res = CompletedWith.Data(v)
        state["invoked_step_handler_return_value"] = res
        return res
    async def invoked_send_response_success_handler(v: TestValue, completed_res: CompletedResult):
        state["invoked_step_handler_completed_res"] = completed_res
        return Result.Ok(None)
    value = TestValue('test result')
    succ_res = Result.Ok(value)
    handler = wrap_step_handler(invoked_step_handler)

    await handler(invoked_send_response_success_handler, succ_res)
    actual_completed_result = state["invoked_step_handler_completed_res"]
    expected_completed_result = state["invoked_step_handler_return_value"]

    assert actual_completed_result == expected_completed_result



async def test_wrap_step_handler_when_handler_returns_none_then_returns_none():
    async def invoked_step_handler(v: TestValue):
        return None
    value = TestValue('test result')
    succ_res = Result.Ok(value)
    handler = wrap_step_handler(invoked_step_handler)

    actual_res = await handler(send_response_success_handler, succ_res)

    assert actual_res is None



async def test_wrap_step_handler_when_handler_returns_none_then_send_response_handler_is_not_invoked():
    state = {}
    async def invoked_step_handler(v: TestValue):
        return None
    async def invoked_send_response_success_handler(v: TestValue, completed_res: CompletedResult):
        state["invoked_step_handler_completed_res"] = completed_res
        return Result.Ok(None)
    value = TestValue('test result')
    succ_res = Result.Ok(value)
    handler = wrap_step_handler(invoked_step_handler)

    await handler(invoked_send_response_success_handler, succ_res)

    assert "invoked_step_handler_completed_res" not in state



async def test_wrap_step_handler_when_send_response_handler_returns_error_then_returns_error():
    expected_res = Result.Error("Test error")
    async def invoked_send_response_success_handler(v: TestValue, completed_res: CompletedResult):
        return expected_res
    value = TestValue('test result')
    succ_res = Result.Ok(value)
    handler = wrap_step_handler(step_handler_w_data)

    actual_res = await handler(invoked_send_response_success_handler, succ_res)

    assert actual_res == expected_res