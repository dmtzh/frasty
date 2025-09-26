from collections.abc import Callable
import functools
from typing import ParamSpec, TypeVar

import pytest

import stephandlers.fetchnewdata.executingtasksstore as executingtasksstore

P = ParamSpec("P")
R = TypeVar("R")

_state = {
}

_EXECUTING_TASKS_STORAGE_ERROR_KEY = "executing_tasks_storage_error"

@pytest.fixture
def set_executing_tasks_storage_error():
    def set_error(err):
        _state[_EXECUTING_TASKS_STORAGE_ERROR_KEY] = err
    yield set_error
    if _EXECUTING_TASKS_STORAGE_ERROR_KEY in _state:
        del _state[_EXECUTING_TASKS_STORAGE_ERROR_KEY]

@pytest.fixture
def remove_executing_tasks_storage_error():
    return functools.partial(_state.pop, _EXECUTING_TASKS_STORAGE_ERROR_KEY)

def wrap_func_with_error_condition(func: Callable[P, R], err_key: str) -> Callable[P, R]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        if err_key in _state:
            raise _state[err_key]
        return func(*args, **kwargs)
    return wrapper

executingtasksstore.executing_tasks_storage.add = wrap_func_with_error_condition(
        executingtasksstore.executing_tasks_storage.add,
        _EXECUTING_TASKS_STORAGE_ERROR_KEY
    )
executingtasksstore.executing_tasks_storage.get = wrap_func_with_error_condition(
        executingtasksstore.executing_tasks_storage.get,
        _EXECUTING_TASKS_STORAGE_ERROR_KEY
    )
