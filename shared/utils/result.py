from collections.abc import Callable, Iterable
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

def apply[T1, T2, R, TErr, RErr](f: Callable[[T1, T2], R], f_err: Callable[[tuple[TErr, ...]], RErr], res1: Result[T1, TErr] | Result[T1, tuple[TErr, ...]], res2: Result[T2, TErr] | Result[T2, tuple[TErr, ...]]):
    match res1.is_ok(), res2.is_ok():
        case True, True:
            return Result[R, RErr].Ok(f(res1.ok, res2.ok))
        case True, False:
            match res2.error:
                case (*_,) as err2_tuple:
                    return Result[R, RErr].Error(f_err(err2_tuple))
                case err2:
                    return Result[R, RErr].Error(f_err((err2,)))
        case False, True:
            match res1.error:
                case (*_,) as err1_tuple:
                    return Result[R, RErr].Error(f_err(err1_tuple))
                case err1:
                    return Result[R, RErr].Error(f_err((err1,)))
        case False, False:
            match res1.error, res2.error:
                case (*_,) as err1_tuple, (*_,) as err2_tuple:
                    return Result[R, RErr].Error(f_err(err1_tuple + err2_tuple))
                case (*_,) as err1_tuple, err2:
                    return Result[R, RErr].Error(f_err(err1_tuple + (err2,)))
                case err1, (*_,) as err2_tuple:
                    return Result[R, RErr].Error(f_err((err1,) + err2_tuple))
                case err1, err2:
                    return Result[R, RErr].Error(f_err((err1, err2)))

def apply3[T1, T2, T3, R, TErr, RErr](f: Callable[[T1, T2, T3], R], f_err: Callable[[tuple[TErr, ...]], RErr], res1: Result[T1, TErr] | Result[T1, tuple[TErr, ...]], res2: Result[T2, TErr], res3: Result[T3, TErr]):
    res_1_2 = apply(lambda t1, t2: (t1, t2), lambda err: err, res1, res2)
    return apply(lambda t_1_2, t3: f(*t_1_2, t3), f_err, res_1_2, res3)

def apply4[T1, T2, T3, T4, R, TErr, RErr](f: Callable[[T1, T2, T3, T4], R], f_err: Callable[[tuple[TErr, ...]], RErr], res1: Result[T1, TErr] | Result[T1, tuple[TErr, ...]], res2: Result[T2, TErr], res3: Result[T3, TErr], res4: Result[T4, TErr]):
    res_1_2 = apply(lambda t1, t2: (t1, t2), lambda err: err, res1, res2)
    return apply3(lambda t_1_2, t3, t4: f(*t_1_2, t3, t4), f_err, res_1_2, res3, res4)

def _normalize_error[TErr](error: TErr | tuple[TErr, ...]) -> tuple[TErr, ...]:
    """
    Normalize a single error or tuple of errors into a tuple.
    This allows func to return either Result[R, TErr] or Result[R, tuple[TErr, ...]].
    """
    match error:
        case tuple():
            return error
        case _:
            return (error,)

def traverse_accumulating[T, R, TErr](
    items: Iterable[T],
    func: Callable[[T], Result[R, TErr] | Result[R, tuple[TErr, ...]]]
) -> Result[list[R], tuple[TErr, ...]]:
    """
    Apply func to each element of items, accumulating all errors.
    
    This function processes all elements in the iterable, collecting successful
    results and errors separately. It does NOT stop on the first error (unlike
    fail-fast traverse). Instead, it continues processing all elements and
    returns either:
    - Result.Ok(list[R]) containing all successful results if all succeeded
    - Result.Error(tuple[TErr, ...]) containing all errors if any failed
    
    Key characteristics:
    - Order preservation: results and errors appear in the same order as encountered
    - Atomic result: if any error occurs, successful results are discarded
    - Error normalization: func can return either single errors or tuples of errors
    - O(N) complexity: uses list.append() instead of concatenation
    - Lazy input: accepts Iterable, but materializes output
    
    Args:
        items: Iterable of elements to process
        func: Function that takes an element and returns Result[R, TErr] or
              Result[R, tuple[TErr, ...]]. Exceptions are NOT caught.
    
    Returns:
        Result[list[R], tuple[TErr, ...]]:
        - Ok(list[R]) if all elements processed successfully
        - Error(tuple[TErr, ...]) if any element failed, containing all errors
    
    Examples:
        >>> traverse_accumulating([1, 2, 3], lambda x: Result.Ok(x * 2))
        Result.Ok([2, 4, 6])
        
        >>> traverse_accumulating([1, 2, 3], lambda x: Result.Ok(x * 2) if x > 1 else Result.Error("too small"))
        Result.Error(("too small",))
        
        >>> traverse_accumulating([1, 2, 3], lambda x: Result.Error(f"err{x}"))
        Result.Error(("err1", "err2", "err3"))
    
    Complexity:
        Time: O(N) where N is the number of elements
        Space: O(N) for storing results or errors
    """
    def append_to_ok_buffer(value: R):
        ok_buffer.append(value)
    def extend_err_buffer(errors: TErr | tuple[TErr, ...]):
        err_buffer.extend(_normalize_error(errors))

    ok_buffer: list[R] = []
    err_buffer: list[TErr] = []
    
    for item in items:
        result = func(item)
        result.map(append_to_ok_buffer).default_with(extend_err_buffer)
    
    if err_buffer:
        return Result.Error(tuple(err_buffer))
    else:
        return Result.Ok(ok_buffer)

def traverse_accumulating_with_index[T, R, TErr](
    items: Iterable[T],
    func: Callable[[int, T], Result[R, TErr] | Result[R, tuple[TErr, ...]]]
) -> Result[list[R], tuple[TErr, ...]]:
    """
    Apply func to each element of items with its index, accumulating all errors.
    
    This is a variant of traverse_accumulating that passes the element's index
    (0-based) as the first argument to func. This is useful for generating
    informative error messages like "input_data[3]: missing key 'id'".
    
    All other characteristics are identical to traverse_accumulating:
    - Accumulating semantics (does not stop on first error)
    - Order preservation
    - Atomic result
    - Error normalization
    - O(N) complexity
    
    Args:
        items: Iterable of elements to process
        func: Function that takes (index, element) and returns Result[R, TErr] or
              Result[R, tuple[TErr, ...]]. Exceptions are NOT caught.
    
    Returns:
        Result[list[R], tuple[TErr, ...]]:
        - Ok(list[R]) if all elements processed successfully
        - Error(tuple[TErr, ...]) if any element failed, containing all errors
    
    Examples:
        >>> traverse_accumulating_with_index(
        ...     [{"id": "1"}, {"name": "test"}],
        ...     lambda idx, dto: Result.Ok(dto) if "id" in dto else Result.Error(f"input_data[{idx}]: missing 'id'")
        ... )
        Result.Error(("input_data[1]: missing 'id'",))
    
    Complexity:
        Time: O(N) where N is the number of elements
        Space: O(N) for storing results or errors
    """
    def append_to_ok_buffer(value: R):
        ok_buffer.append(value)
    def extend_err_buffer(errors: TErr | tuple[TErr, ...]):
        err_buffer.extend(_normalize_error(errors))

    ok_buffer: list[R] = []
    err_buffer: list[TErr] = []
    
    for idx, item in enumerate(items):
        result = func(idx, item)
        result.map(append_to_ok_buffer).default_with(extend_err_buffer)
    
    if err_buffer:
        return Result.Error(tuple(err_buffer))
    else:
        return Result.Ok(ok_buffer)
