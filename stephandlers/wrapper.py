from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, TypeVar

from expression import Result

from shared.completedresult import CompletedResult
from shared.utils.asyncresult import AsyncResult, async_result, coroutine_result
from shared.utils.result import ResultTag

T = TypeVar("T")
TCfg = TypeVar("TCfg")

@dataclass(frozen=True)
class InputValidationError:
    error: Any

class HandlerReturnedNoneError:
    '''Handler returned None error'''

def wrap_step_handler(handler: Callable[[T], Coroutine[Any, Any, CompletedResult | None]]):
    async def wrapper(send_response_handler: Callable[[T, CompletedResult], Coroutine[Any, Any, Result]], input_res: Result[T, Any]) -> Result[CompletedResult, Any] | None:
        @coroutine_result()
        async def handle_step():
            input = await AsyncResult.from_result(input_res).map_error(InputValidationError)
            opt_completed_res = await handler(input)
            completed_res = await AsyncResult.from_result(Result.Ok(opt_completed_res) if opt_completed_res is not None else Result.Error(HandlerReturnedNoneError()))
            await async_result(send_response_handler)(input, completed_res)
            return completed_res
        
        handle_step_res = await handle_step()
        match handle_step_res:
            case Result(tag=ResultTag.ERROR, error=InputValidationError()):
                return None
            case Result(tag=ResultTag.ERROR, error=HandlerReturnedNoneError()):
                return None
            case _:
                return handle_step_res
    return wrapper