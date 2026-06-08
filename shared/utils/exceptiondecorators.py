from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, Concatenate, Type

from expression import Result

from shared.customtypes import Error

def async_ex_to_error_result[TExErr](ex_to_err: Callable[[Exception], TExErr], exception: Type[Exception] = Exception):
    """
    A decorator factory that converts any exceptions raised by an asynchronous function into a Result error.

    This function returns a decorator that wraps a given function. If the function executes
    successfully, the result is wrapped in a Result.Ok. If an exception occurs, the exception 
    is passed to the `ex_to_err` function to convert it into an error type, which is then 
    wrapped in a Result.Error. If the function returns a Result, it is returned as is.

    Args:
        ex_to_err (Callable[[Exception], TExErr]: 
            A function that converts an exception to an error type.
    Returns:
        Callable[[Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]], 
                 Callable[P, Coroutine[Any, Any, Result[T, TExErr]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | TExErr]]]]:
            A decorator that wraps an asynchronous function and applies exception-to-error conversion.
    """

    def decorator[T, TErr, **P](func: Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]) -> Callable[P, Coroutine[Any, Any, Result[T, TExErr]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | TExErr]]]:
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
                    case Result():
                        return val
                    case _:
                        return Result[T, TExErr].Ok(val)
            except exception as ex:
                err = ex_to_err(ex)
                return Result[T, TExErr].Error(err)
        return wrapper
    return decorator

def async_ex_to_error_result_with_args[TExErr, **P](ex_to_err: Callable[Concatenate[Exception, P], TExErr], exception: Type[Exception] = Exception):
    """
    A decorator factory that converts any exceptions raised by an asynchronous function into a Result error.

    This function returns a decorator that wraps a given function. If the function executes
    successfully, the result is wrapped in a Result.Ok. If an exception occurs, the exception 
    is passed to the `ex_to_err` function to convert it into an error type, which is then 
    wrapped in a Result.Error. If the function returns a Result, it is returned as is.

    Args:
        ex_to_err (Callable[Concatenate[Exception, P], TExErr]): 
            A function that converts an exception to an error type. First parameter is the
            exception, other are the same as the decorated function.
    Returns:
        Callable[[Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]], 
                 Callable[P, Coroutine[Any, Any, Result[T, TExErr]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | TExErr]]]]:
            A decorator that wraps an asynchronous function and applies exception-to-error conversion.
    """

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
                    case Result():
                        return val
                    case _:
                        return Result[T, TExErr].Ok(val)
            except exception as ex:
                err = ex_to_err(ex, *args, **kwargs)
                return Result[T, TExErr].Error(err)
        return wrapper
    return decorator

def async_catch_ex[T, TErr, **P](func: Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]) -> Callable[P, Coroutine[Any, Any, Result[T, Error]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | Error]]]:
    return async_ex_to_error_result(Error.from_exception)(func)
