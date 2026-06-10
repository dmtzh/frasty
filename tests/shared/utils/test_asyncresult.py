import pytest
from expression import Result
from typing import Any, Callable

from shared.utils.asyncresult import AsyncResult 

# --- Helper Functions (Mappers) ---
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