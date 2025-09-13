from collections.abc import Callable
import functools
from typing import Concatenate, ParamSpec, TypeVar

import pytest

import shared.definitionsstore as definitionsstore
import shared.runningdefinitionsstore as runningdefinitionsstore

T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")

_state = {
}

_RUNNING_DEFINITIONS_STORAGE_ERROR_KEY = "running_definitions_storage_error"
_DEFINITIONS_STORAGE_ERROR_KEY = "definitions_storage_error"

@pytest.fixture
def set_running_definitions_storage_error():
    def set_error(err):
        _state[_RUNNING_DEFINITIONS_STORAGE_ERROR_KEY] = err
    yield set_error
    if _RUNNING_DEFINITIONS_STORAGE_ERROR_KEY in _state:
        del _state[_RUNNING_DEFINITIONS_STORAGE_ERROR_KEY]

@pytest.fixture
def set_definitions_storage_error():
    def set_error(err):
        _state[_DEFINITIONS_STORAGE_ERROR_KEY] = err
    yield set_error
    if _DEFINITIONS_STORAGE_ERROR_KEY in _state:
        del _state[_DEFINITIONS_STORAGE_ERROR_KEY]

def wrap_first_param(func: Callable[Concatenate[T, P], R], first_param_creator: Callable[[T], T]) -> Callable[Concatenate[T, P], R]:
    @functools.wraps(func)
    def wrapper(first_param: T, *args: P.args, **kwargs: P.kwargs) -> R:
        return func(first_param_creator(first_param), *args, **kwargs)
    return wrapper

def wrap_with_running_definitions_storage_error_condition(func: Callable[P, R]) -> Callable[P, R]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        if _RUNNING_DEFINITIONS_STORAGE_ERROR_KEY in _state:
            raise _state[_RUNNING_DEFINITIONS_STORAGE_ERROR_KEY]
        return func(*args, **kwargs)
    return wrapper

def wrap_with_definitions_storage_error_condition(func: Callable[P, R]) -> Callable[P, R]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        if _DEFINITIONS_STORAGE_ERROR_KEY in _state:
            raise _state[_DEFINITIONS_STORAGE_ERROR_KEY]
        return func(*args, **kwargs)
    return wrapper

def wrap_storage_get_with_error_condition(get_func: Callable[P, R], err_key: str) -> Callable[P, R]:
    @functools.wraps(get_func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        if err_key in _state:
            raise _state[err_key]
        return get_func(*args, **kwargs)
    return wrapper

runningdefinitionsstore.running_definitions_storage.with_storage = wrap_first_param(
        runningdefinitionsstore.running_definitions_storage.with_storage,
        wrap_with_running_definitions_storage_error_condition
    )

definitionsstore.definitions_storage.get_with_ver = wrap_storage_get_with_error_condition(
        definitionsstore.definitions_storage.get_with_ver,
        _DEFINITIONS_STORAGE_ERROR_KEY
    )