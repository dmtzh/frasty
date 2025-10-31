from collections.abc import Callable, Coroutine
from expression import Result
from typing import Any

import pytest

from shared.pipeline.handlers import Handler, map_handler
from shared.utils.result import ResultTag

async def multiply_by_two_if_not_zero(t: Result[int, Any]) -> Result | None:
    match t:
        case Result(tag=ResultTag.OK, ok=0):
            return None
        case _:
            return t.map(lambda t: t * 2)

async def add_greeting(t: Result[str, Any]) -> Result | None:
    return t.map(lambda t: f"Hello {t}")

def create_handler[T](val: Result[T, Any]) -> Handler[T]:
    async def handler(func: Callable[[Result[T, Any]], Coroutine[Any, Any, Result | None]]):
        res = await func(val)
        return str(res)
    return handler

def add_two(t: Result[int, Any]) -> Result[int, Any]:
    return t.map(lambda t: t + 2)

def add_three(t: Result[int, Any]) -> Result[int, Any]:
    return t.map(lambda t: t + 3)

def add_middlename(t: Result[str, Any]) -> Result[str, Any]:
    return t.map(lambda t: f"{t} J.")

def add_lastname(t: Result[str, Any]) -> Result[str, Any]:
    return t.map(lambda t: f"{t} Black")



@pytest.mark.parametrize(("input_value", "func"), [
    (Result[int, Any].Ok(-2), multiply_by_two_if_not_zero),
    (Result[int, Any].Ok(0), multiply_by_two_if_not_zero),
    (Result[int, Any].Ok(2), multiply_by_two_if_not_zero),
    (Result[int, Any].Error("int error"), multiply_by_two_if_not_zero),
    (Result[str, Any].Ok("John"), add_greeting),
    (Result[str, Any].Error("str error"), add_greeting)
])
async def test_handler_functor_satisfy_identity_law(input_value, func):
    handler = create_handler(input_value)
    handler_with_id = map_handler(handler, lambda t: t)

    handler_output = await handler(func)
    handler_with_id_output = await handler_with_id(func)

    assert handler_output == handler_with_id_output



@pytest.mark.parametrize(("input_value", "map_func1", "map_func2", "func"), [
    (Result[int, Any].Ok(-7), add_two, add_three, multiply_by_two_if_not_zero),
    (Result[int, Any].Ok(-5), add_two, add_three, multiply_by_two_if_not_zero),
    (Result[int, Any].Ok(-2), add_two, add_three, multiply_by_two_if_not_zero),
    (Result[int, Any].Ok(0), add_two, add_three, multiply_by_two_if_not_zero),
    (Result[int, Any].Ok(2), add_two, add_three, multiply_by_two_if_not_zero),
    (Result[int, Any].Error("int error"), add_two, add_three, multiply_by_two_if_not_zero),
    (Result[str, Any].Ok("John"), add_middlename, add_lastname, add_greeting),
    (Result[str, Any].Error("str error"), add_middlename, add_lastname, add_greeting)
])
async def test_handler_functor_satisfy_composition_law(input_value, map_func1, map_func2, func):
    handler = create_handler(input_value)
    map_func1_then_map_func2_handler = map_handler(map_handler(handler, map_func1), map_func2)
    combined_map_func1_and_map_func2_handler = map_handler(handler, lambda t: map_func2(map_func1(t)))

    map_func1_then_map_func2_output = await map_func1_then_map_func2_handler(func)
    combined_map_func1_and_map_func2_output = await combined_map_func1_and_map_func2_handler(func)

    assert map_func1_then_map_func2_output == combined_map_func1_and_map_func2_output
