from collections.abc import Callable
from enum import StrEnum
from itertools import chain
from typing import Any

from expression import Result

class ResultTag(StrEnum):
    OK = "ok"
    ERROR = "error"

def to_error_list[TErr](*results: Result[Any, TErr]) -> list[TErr]:
    errors = map(lambda result: result.map(lambda _: []).default_with(lambda err: [err]), results)
    return list(chain.from_iterable(errors))

def to_ok_list[T](*results: Result[T, Any]) -> list[T]:
    oks = map(lambda result: result.map(lambda t: [t]).default_with(lambda _: []), results)
    return list(chain.from_iterable(oks))

def apply[T1, T2, R, TErr, RErr](f: Callable[[T1, T2], R], f_err: Callable[[tuple[TErr, ...]], RErr], res1: Result[T1, TErr] | Result[T1, tuple[TErr, ...]], res2: Result[T2, TErr]):
    match res1.is_ok(), res2.is_ok():
        case True, True:
            return Result[R, RErr].Ok(f(res1.ok, res2.ok))
        case True, False:
            return Result[R, RErr].Error(f_err((res2.error,)))
        case False, True:
            match res1.error:
                case (*_,):
                    return Result[R, RErr].Error(f_err(res1.error))
                case _:
                    return Result[R, RErr].Error(f_err((res1.error,)))
        case False, False:
            match res1.error:
                case (*_,):
                    return Result[R, RErr].Error(f_err((*res1.error, res2.error)))
                case _:
                    return Result[R, RErr].Error(f_err((res1.error, res2.error)))
            