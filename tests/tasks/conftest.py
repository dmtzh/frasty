from collections.abc import Callable
import functools
from typing import ParamSpec, TypeVar

import pytest

import shared.tasksstore as tasksstore

P = ParamSpec("P")
R = TypeVar("R")

_state = {
}

_TASKS_STORAGE_ERROR_KEY = "tasks_storage_error"

@pytest.fixture
def set_tasks_storage_error():
    def set_error(err):
        _state[_TASKS_STORAGE_ERROR_KEY] = err
    yield set_error
    if _TASKS_STORAGE_ERROR_KEY in _state:
        del _state[_TASKS_STORAGE_ERROR_KEY]

def wrap_func_with_error_condition(func: Callable[P, R], err_key: str) -> Callable[P, R]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        if err_key in _state:
            raise _state[err_key]
        return func(*args, **kwargs)
    return wrapper

tasksstore.tasks_storage.add = wrap_func_with_error_condition(
        tasksstore.tasks_storage.add,
        _TASKS_STORAGE_ERROR_KEY
    )