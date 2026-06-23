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
                case (*_,) as err1_tuple:
                    return Result[R, RErr].Error(f_err(err1_tuple))
                case err1:
                    return Result[R, RErr].Error(f_err((err1,)))
        case False, False:
            match res1.error:
                case (*_,) as err1_tuple:
                    return Result[R, RErr].Error(f_err(err1_tuple + (res2.error,)))
                case err1:
                    return Result[R, RErr].Error(f_err((err1, res2.error)))

def apply3[T1, T2, T3, R, TErr, RErr](f: Callable[[T1, T2, T3], R], f_err: Callable[[tuple[TErr, ...]], RErr], res1: Result[T1, TErr] | Result[T1, tuple[TErr, ...]], res2: Result[T2, TErr], res3: Result[T3, TErr]):
    res_1_2 = apply(lambda t1, t2: (t1, t2), lambda err: err, res1, res2)
    return apply(lambda t_1_2, t3: f(*t_1_2, t3), f_err, res_1_2, res3)

def apply4[T1, T2, T3, T4, R, TErr, RErr](f: Callable[[T1, T2, T3, T4], R], f_err: Callable[[tuple[TErr, ...]], RErr], res1: Result[T1, TErr] | Result[T1, tuple[TErr, ...]], res2: Result[T2, TErr], res3: Result[T3, TErr], res4: Result[T4, TErr]):
    res_1_2 = apply(lambda t1, t2: (t1, t2), lambda err: err, res1, res2)
    return apply3(lambda t_1_2, t3, t4: f(*t_1_2, t3, t4), f_err, res_1_2, res3, res4)