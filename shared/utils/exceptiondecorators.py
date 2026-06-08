from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar

from expression import Result

from shared.customtypes import Error
from shared.utils.asyncresult import async_ex_to_error_result

P = ParamSpec("P")
T = TypeVar("T")
TErr = TypeVar("TErr")

def async_catch_ex(func: Callable[P, Coroutine[Any, Any, T]] | Callable[P, Coroutine[Any, Any, Result[T, TErr]]]) -> Callable[P, Coroutine[Any, Any, Result[T, Error]]] | Callable[P, Coroutine[Any, Any, Result[T, TErr | Error]]]:
    return async_ex_to_error_result(Error.from_exception)(func)
