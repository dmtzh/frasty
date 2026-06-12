from collections.abc import Coroutine

import pytest
from expression import Result
from typing import Any, Callable

from shared.utils.asyncresult import AsyncResult 

# --- Helper functor laws functions (mappers) ---
def add_two(t: int) -> int:
    return t + 2

def multiply_by_three(t: int) -> int:
    return t * 3

def add_greeting(t: str) -> str:
    return f"Hello {t}"

def add_lastname(t: str) -> str:
    return f"{t} Black"


# --- Test Data Preparation ---
# Using AsyncResult.from_result to easily create test instances from synchronous Results
@pytest.mark.parametrize("res", [
    Result[int, Any].Ok(5),
    Result[int, Any].Ok(-2),
    Result[int, Any].Error("int error"),
    Result[str, Any].Ok("John"),
    Result[str, Any].Error("str error"),
])
async def test_async_result_functor_satisfies_identity_law(res: Result[Any, Any]):
    """
    First Functor Law (Identity): 
    Mapping with an identity function should be equivalent to doing nothing.
    map(lambda x: x) == id
    """
    # Get the underlying Result without triggering the __await__ exception on Error
    original_result = await AsyncResult.from_result(res).to_coroutine()
    
    # Apply identity mapping and get the underlying Result
    identity_mapped_result = await AsyncResult.from_result(res).map(lambda x: x).to_coroutine()
    
    assert original_result == identity_mapped_result


@pytest.mark.parametrize("res, map_func1, map_func2", [
    (Result[int, Any].Ok(5), add_two, multiply_by_three),
    (Result[int, Any].Ok(-2), add_two, multiply_by_three),
    (Result[int, Any].Error("int error"), add_two, multiply_by_three),
    (Result[str, Any].Ok("John"), add_greeting, add_lastname),
    (Result[str, Any].Error("str error"), add_greeting, add_lastname),
])
async def test_async_result_functor_satisfies_composition_law(
    res: Result[Any, Any], 
    map_func1: Callable[[Any], Any], 
    map_func2: Callable[[Any], Any]
):
    """
    Second Functor Law (Composition): 
    Mapping with a composition of two functions should be equivalent 
    to mapping with the first function and then mapping with the second.
    map(f . g) == map(f) . map(g)
    """
    # Left side: Sequential mapping (map g, then map f)
    sequential_map_result = await AsyncResult.from_result(res).map(map_func1).map(map_func2).to_coroutine()
    
    # Right side: Single mapping with composed function (map (f . g))
    composed_map_result = await AsyncResult.from_result(res).map(lambda x: map_func2(map_func1(x))).to_coroutine()
    
    assert sequential_map_result == composed_map_result

# --- Helper monad laws functions (mappers) ---
async def double_val(x: int) -> Result[int, str]:
    return Result.Ok(x * 2)

async def add_ten_val(x: int) -> Result[int, str]:
    return Result.Ok(x + 10)

async def fail_if_negative(x: int) -> Result[int, str]:
    return Result.Error("negative value") if x < 0 else Result.Ok(x)

async def append_suffix(x: str) -> Result[str, str]:
    return Result.Ok(f"{x}_suffix")

async def async_return[T](x: T) -> Result[T, Any]:
    """Monad 'return' (or 'pure') function for AsyncResult."""
    return Result.Ok(x)


# ==============================================================================
# 1. Left Identity Law: return(x).bind(f) == f(x)
# ==============================================================================
@pytest.mark.parametrize("x, mapper_func", [
    (5, double_val),
    (-5, fail_if_negative),
    ("hello", append_suffix),
])
async def test_async_result_monad_left_identity(x: Any, mapper_func: Callable[[Any], Coroutine[Any, Any, Result[Any, str]]]):
    """
    Left Identity Law: Wrapping a value and then binding it to a function 
    should be the same as just applying the function to the value.
    """
    # Left side: return(x).bind(f)
    left_side_coro = AsyncResult.from_result(Result.Ok(x)).bind(mapper_func).to_coroutine()
    
    # Right side: f(x)
    right_side_coro = mapper_func(x)

    left_res = await left_side_coro
    right_res = await right_side_coro

    assert left_res == right_res


# ==============================================================================
# 2. Right Identity Law: m.bind(return) == m
# ==============================================================================
@pytest.mark.parametrize("res", [
    Result[int, str].Ok(42),
    Result[int, str].Ok(-10),
    Result[int, str].Error("initial error"),
    Result[str, str].Ok("test"),
    Result[str, str].Error("string error"),
])
async def test_async_result_monad_right_identity(res: Result[Any, Any]):
    """
    Right Identity Law: Binding a monadic value to the 'return' function 
    should leave the monadic value unchanged.
    """
    # Left side: m.bind(return)
    left_side_coro = AsyncResult.from_result(res).bind(async_return).to_coroutine()
    
    # Right side: m
    right_side_coro = AsyncResult.from_result(res).to_coroutine()

    left_res = await left_side_coro
    right_res = await right_side_coro

    assert left_res == right_res


# ==============================================================================
# 3. Associativity Law: (m.bind(f)).bind(g) == m.bind(lambda x: f(x).bind(g))
# ==============================================================================
@pytest.mark.parametrize("res, f, g", [
    # Success chain
    (Result.Ok(5), double_val, add_ten_val),
    # Failure at first step (f)
    (Result.Ok(-5), fail_if_negative, add_ten_val),
    # Failure at second step (g)
    (Result.Ok(3), double_val, fail_if_negative),
    # Initial error (should short-circuit immediately)
    (Result.Error("start error"), double_val, add_ten_val),
])
async def test_async_result_monad_associativity(
    res: Result[Any, Any], 
    f: Callable[[Any], Coroutine[Any, Any, Result[Any, str]]], 
    g: Callable[[Any], Coroutine[Any, Any, Result[Any, str]]]
):
    """
    Associativity Law: The order of nesting bind operations should not matter.
    Chaining binds should be equivalent to binding to a function that performs the chain.
    """
    # Left side: (m.bind(f)).bind(g)
    left_side_coro = AsyncResult.from_result(res).bind(f).bind(g).to_coroutine()

    # Right side: m.bind(lambda x: (f(x) >>= g))
    # Since f(x) returns a Coroutine, we wrap it in AsyncResult to use .bind(g),
    # then await the final Result to satisfy the outer bind's signature.
    async def right_side_mapper(x: Any) -> Result[Any, Any]:
        inner_chain = AsyncResult(f(x)).bind(g)
        return await inner_chain.to_coroutine()

    right_side_coro = AsyncResult.from_result(res).bind(right_side_mapper).to_coroutine()

    left_res = await left_side_coro
    right_res = await right_side_coro

    assert left_res == right_res
