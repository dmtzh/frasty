from __future__ import annotations
from collections.abc import Callable, Coroutine
from functools import wraps
from inspect import signature
from typing import Any, Concatenate, ParamSpec, Type

from expression import Result

from shared.customtypes import Error
from shared.utils.result import ResultTag
from shared.utils.asynchronous import make_async

P = ParamSpec("P")

def ex_to_error_result[TExErr](ex_to_err: Callable[[Exception], TExErr] | Callable[Concatenate[Exception, P], TExErr], exception: Type[Exception] = Exception):
    """
    A decorator factory that converts any exceptions raised by a function into a Result error.

    This function returns a decorator that wraps a given function. If the function executes
    successfully, the result is wrapped in a Result.Ok. If an exception occurs, the exception 
    is passed to the `ex_to_err` function to convert it into an error type, which is then 
    wrapped in a Result.Error. If the function returns a Result, it is returned as is.

    Args:
        ex_to_err (Callable[[Exception], TExErr] | Callable[Concatenate[Exception, P], TExErr]): 
            A function that converts an exception to an error type. To get values of
            the parameters passed to the decorated function, add all parameters after ex parameter
            to the function signature.
    Returns:
        Callable[[Callable[P, T] | Callable[P, Result[T, TErr]]], 
                 Callable[P, Result[T, TExErr]] | Callable[P, Result[T, TErr | TExErr]]]:
            A decorator that wraps a function and applies exception-to-error conversion.
    """

    num_of_params = len(signature(ex_to_err).parameters)
    ex_to_err_with_args = ex_to_err if num_of_params > 1 else lambda ex, *args, **kwargs: ex_to_err(ex)
    def decorator[T, TErr](func: Callable[P, T] | Callable[P, Result[T, TErr]]) -> Callable[P, Result[T, TExErr]] | Callable[P, Result[T, TErr | TExErr]]:
        """
        A decorator that wraps a function and converts any exceptions raised into a Result error.

        If the function executes successfully, the result is wrapped in a Result.Ok. If an exception
        occurs, the exception is passed to the `ex_to_err` function to convert it into an error type, which is then 
        wrapped in a Result.Error. If the function returns a Result, it is returned as is.

        Args:
            func (Callable[P, T] | Callable[P, Result[T, TErr]]): The function to be wrapped.

        Returns:
            Callable[P, Result[T, TExErr]] | Callable[P, Result[T, TErr | TExErr]]:
                A function that wraps the given function and applies the above logic.
        """

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs):
            try:
                val = func(*args, **kwargs)
                match val:
                    case Result():
                        return val
                    case _:
                        return Result[T, TExErr].Ok(val)
            except exception as ex:
                err = ex_to_err_with_args(ex, *args, **kwargs)
                return Result[T, TExErr].Error(err)
        return wrapper
    return decorator

class _AsyncResultError(Exception):
    """
    Makes the Error case a valid exception for async result handling in await expressions.

    Do not use directly.
    """

# def async_ex_to_error_result[TErr](ex_to_err: Callable[[Exception], TErr] | Callable[Concatenate[Exception, P], TErr]):
#     num_of_params = len(signature(ex_to_err).parameters)
#     ex_to_err_with_args = ex_to_err if num_of_params > 1 else lambda ex, *args, **kwargs: ex_to_err(ex)
#     def decorator[T](func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, Result[T, TErr]]]:
#         @wraps(func)
#         async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, TErr]:
#             try:
#                 val = await func(*args, **kwargs)
#                 return Result[T, TErr].Ok(val)
#             except Exception as ex:
#                 err = ex_to_err_with_args(ex, *args, **kwargs)
#                 return Result[T, TErr].Error(err)
#         return wrapper
#     return decorator

def async_ex_to_error_result[TExErr](ex_to_err: Callable[[Exception], TExErr] | Callable[Concatenate[Exception, P], TExErr], exception: Type[Exception] = Exception):
    """
    A decorator factory that converts any exceptions raised by an asynchronous function into a Result error.

    This function returns a decorator that wraps a given function. If the function executes
    successfully, the result is wrapped in a Result.Ok. If an exception occurs, the exception 
    is passed to the `ex_to_err` function to convert it into an error type, which is then 
    wrapped in a Result.Error. If the function returns a Result, it is returned as is.

    Args:
        ex_to_err (Callable[[Exception], TExErr] | Callable[Concatenate[Exception, P], TExErr]): 
            A function that converts an exception to an error type. To get values of
            the parameters passed to the decorated function, add all parameters after ex parameter
            to the function signature.
    Returns:
        Callable[[Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]], 
                 Callable[P, Coroutine[Any, Any, Result[T, TExErr]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | TExErr]]]]:
            A decorator that wraps an asynchronous function and applies exception-to-error conversion.
    """

    num_of_params = len(signature(ex_to_err).parameters)
    ex_to_err_with_args = ex_to_err if num_of_params > 1 else lambda ex, *args, **kwargs: ex_to_err(ex)
    def decorator[T, TErr](func: Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]) -> Callable[P, Coroutine[Any, Any, Result[T, TExErr]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | TExErr]]]:
        """
        A decorator that wraps an asynchronous function and converts any exceptions raised into a Result error.

        If the function executes successfully, the result is wrapped in a Result.Ok. If an exception
        occurs, the exception is passed to the `ex_to_err` function to convert it into an error type, which is then 
        wrapped in a Result.Error. If the function returns a Result, it is returned as is.

        Args:
            func (Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]): The function to be wrapped.

        Returns:
            Callable[P, Coroutine[Any, Any, Result[T, TExErr]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | TExErr]]]:
                A function that wraps the given function and applies the above logic.
        """
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs):
            try:
                val = await func(*args, **kwargs)
                match val:
                    case Result(tag="ok", ok=_) | Result(tag="error", error=_):
                        return val
                    case _:
                        return Result[T, TExErr].Ok(val)
            except exception as ex:
                err = ex_to_err_with_args(ex, *args, **kwargs)
                return Result[T, TExErr].Error(err)
        return wrapper
    return decorator

def async_catch_ex[T, TErr](func: Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]) -> Callable[P, Coroutine[Any, Any, Result[T, Error]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | Error]]]:
    return async_ex_to_error_result(Error.from_exception)(func)

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
    
    async def get_or_else[TOut](self, error_getter: Callable[[TErr], Coroutine[Any, Any, TOut]]) -> T | TOut:
        res = await self._value
        match res:
            case Result(tag=ResultTag.OK, ok=value):
                return value
            case Result(tag=ResultTag.ERROR, error=error):
                return await error_getter(error)
    
    def or_else(self, func: Callable[[TErr], AsyncResult[T, TErr]]):
        async def get_result(get_first_result: Coroutine[Any, Any, Result[T, TErr]], get_second_result_func: Callable[[TErr], Coroutine[Any, Any, Result[T, TErr]]]):            
            first_res = await get_first_result
            match first_res:
                case Result(tag=ResultTag.ERROR, error=error):
                    second_result = await get_second_result_func(error)
                    return second_result
                case _:
                    return first_res
        return AsyncResult[T, TErr](get_result(self._value, func))
    
    def to_coroutine_result(self):
        return self._value
    
    @staticmethod
    def from_result[T, TErr](res: Result[T, TErr]):
        get_res_async = make_async(lambda: res)
        res_coro = get_res_async()
        return AsyncResult[T, TErr](res_coro)
   
def async_result[T, TErr](func: Callable[P, Coroutine[Any, Any, Result[T, TErr]]]) -> Callable[P, AsyncResult[T, TErr]]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs):
        async_res = func(*args, **kwargs)
        return AsyncResult(async_res)
    return wrapper

class coroutine_result[TErr]():
    def __call__[T](self, func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, Result[T, TErr]]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, TErr]:
            try:
                val = await func(*args, **kwargs)
                return Result[T, TErr].Ok(val)
            except _AsyncResultError as ex:
                return ex.args[0]
        return wrapper
