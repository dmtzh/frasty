from collections.abc import Callable, Coroutine
from enum import StrEnum
from functools import wraps
from typing import Any, Concatenate, ParamSpec, TypeVar

from expression import Result

class ResultTag(StrEnum):
    OK = "ok"
    ERROR = "error"

T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")
TErr = TypeVar("TErr")
TErr1 = TypeVar("TErr1")
def lift_param(func: Callable[Concatenate[T, P], Coroutine[Any, Any, Result[R, TErr]]]) -> Callable[Concatenate[Result[T, TErr1], P], Coroutine[Any, Any, Result[R, TErr | TErr1]]]:
    async def err1_to_result(err1: TErr1):
        return Result[R, TErr | TErr1].Error(err1)
    @wraps(func)
    def wrapper(t_res: Result[T, TErr1], *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, Result[R, TErr | TErr1]]:
        return t_res.map(lambda t: func(t, *args, **kwargs)).default_with(err1_to_result)
    return wrapper