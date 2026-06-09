from __future__ import annotations
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from expression import Result

from shared.utils.asynchronous import make_async

P = ParamSpec("P")
T = TypeVar("T")
TErr = TypeVar("TErr")

class _AsyncResultError(Exception):
    """
    Makes the Error case a valid exception for async result handling in await expressions.

    Do not use directly.
    """

class AsyncResult[T, TErr]:
    """
    A class that wraps an asynchronous result in a way that allows it to return
    the value of the ok case when used with await expression.

    Args:
        value (Coroutine[Any, Any, Result[T, TErr]]): The asynchronous result to be wrapped.

    Returns:
        T: The value of the ok case of the result.

    Raises:
        _AsyncResultError: If the result is an error.

    Notes:
        This class is intended to be used as part of an await
        expression in a function, decorated with @result_coroutine.
        It will yield the value of the ok case of the result,
        or raise an _AsyncResultError exception if the result is an error.
    """
    def __init__(self, value: Coroutine[Any, Any, Result[T, TErr]]):
        """
        Initializes an instance of AsyncResult.

        Args:
            value (Coroutine[Any, Any, Result[T, TErr]]): The asynchronous result to be wrapped.
        """
        self._value = value

    def __await__(self):
        """
        Implements the awaitable protocol.

        This method is intended to be used as part of an await expression.
        It will yield the result of the coroutine wrapped in Result[T, TErr].Ok,
        or raise an _AsyncResultError exception if the result is an error.

        Returns:
            T: The value of the ok case of the result.
        Raises:
            _AsyncResultError: If the result is an error.
        """
        res = yield from self._value.__await__()
        match res:
            case Result(tag="ok", ok=value):
                return value
            case _:
                raise _AsyncResultError(res)
        return res
    
    def map_error[TErrOut](self, mapper: Callable[[TErr], TErrOut]) -> AsyncResult[T, TErrOut]:
        async def map_error_value(get_result: Coroutine[Any, Any, Result[T, TErr]], mapper: Callable[[TErr], TErrOut]) -> Result[T, TErrOut]:
            res = await get_result
            return res.map_error(mapper)
        return AsyncResult[T, TErrOut](map_error_value(self._value, mapper))
    
    def bind[TOut, TErrOut](self, mapper: Callable[[T], Coroutine[Any, Any, Result[TOut, TErrOut]]]):
        async def error_func(err: TErr):
            return Result[TOut, TErr | TErrOut].Error(err)
        async def get_result(t_result_coro: Coroutine[Any, Any, Result[T, TErr]], mapper: Callable[[T], Coroutine[Any, Any, Result[TOut, TErrOut]]]) -> Result[TOut, TErr | TErrOut]:
            t_res = await t_result_coro
            tout_res_coro = t_res\
                .map(mapper)\
                .default_with(error_func)
            tout_res = await tout_res_coro
            return tout_res
        return AsyncResult[TOut, TErr | TErrOut](get_result(self._value, mapper))
    
    async def get_or_else[TOut](self, error_getter: Callable[[TErr], TOut]) -> T | TOut:
        res = await self._value
        return res.default_with(error_getter)
    
    def or_else[TOut, TErrOut](self, func: Callable[[TErr], Coroutine[Any, Any, Result[TOut, TErrOut]]]):
        async def map_func(t: T):
            return Result[T | TOut, TErrOut].Ok(t)
        async def get_result(get_first_result: Coroutine[Any, Any, Result[T, TErr]], get_second_result_func: Callable[[TErr], Coroutine[Any, Any, Result[TOut, TErrOut]]]):            
            first_res = await get_first_result
            res_coro = first_res.map(map_func).default_with(get_second_result_func)
            res = await res_coro
            return res
        return AsyncResult[T | TOut, TErrOut](get_result(self._value, func))
    
    def to_coroutine(self):
        return self._value
    
    @staticmethod
    def from_result(res: Result[T, TErr]):
        get_res_async = make_async(lambda: res)
        res_coro = get_res_async()
        return AsyncResult[T, TErr](res_coro)

def async_result(func: Callable[P, Coroutine[Any, Any, Result[T, TErr]]]) -> Callable[P, AsyncResult[T, TErr]]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs):
        async_res = func(*args, **kwargs)
        return AsyncResult(async_res)
    return wrapper

class coroutine_result[TErr]():
    def __call__(self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, Result[T, TErr]]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, TErr]:
            try:
                val = await func(*args, **kwargs)
                return Result[T, TErr].Ok(val)
            except _AsyncResultError as ex:
                return ex.args[0]
        return wrapper
