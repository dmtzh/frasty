from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any

from expression import Result

type HandlerContinuation[T] = Callable[[Result[T, Any]], Coroutine[Any, Any, Result | None]]
type Handler[T] = Callable[[HandlerContinuation[T]], Any]

class _to_continuation_with_custom_name[T]:
    def __init__(self, continuation: HandlerContinuation[T], name: str):
        self._continuation = continuation
        self.__name__ = name
    
    async def __call__(self, input_res: Result[T, Any]) -> Result | None:
        return await self._continuation(input_res)

def map_handler[T, R](handler: Handler[T], func: Callable[[Result[T, Any]], Result[R, Any]]) -> Handler[R]:
    return lambda cont_r: handler(_to_continuation_with_custom_name[T](lambda t_res: cont_r(func(t_res)), cont_r.__name__))

def with_middleware[T](handler: Handler[T], func: Callable[[HandlerContinuation[T]], HandlerContinuation[T]]) -> Handler[T]:
    return lambda cont: handler(_to_continuation_with_custom_name(func(cont), cont.__name__))

class HandlerAdapter[T]:
    def __init__(self, handler: Handler[T]):
        self._handler = handler
    
    def __call__(self, func: Callable[[T], Coroutine[Any, Any, Result | None]]):
        @wraps(func)
        async def wrapper(input_res: Result[T, Any]) -> Result | None:
            async def err_to_none(_):
                return None
            return await input_res\
                .map(func)\
                .map_error(err_to_none)\
                .merge()

        return self._handler(wrapper)

# def sample_middleware(subscriber: Handler[Metadata], from_: str):
#     def short_circuit_if_not_from(decoratee: HandlerContinuation[Metadata]):
#         async def middleware_func(data: Metadata):
#             return await decoratee(Result.Ok(data)) if data.get_from() == from_ else None
#         return to_continuation(middleware_func)
#     return with_middleware(subscriber, short_circuit_if_not_from)
