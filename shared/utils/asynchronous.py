import asyncio
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")

def make_async(func: Callable[P, T]) -> Callable[P, Coroutine[Any, Any, T]]:
    """
    A decorator that converts a synchronous function into an asynchronous one.

    This decorator wraps a given function and returns a coroutine that can be awaited.

    Args:
        func (Callable[P, T]): The function to be wrapped.

    Returns:
        Callable[P, Coroutine[Any, Any, T]]: A function that wraps the given function
                                               and returns a coroutine that can be awaited.
    """

    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs):
        return func(*args, **kwargs)
    return wrapper

def async_timer(name: str):
    """
    A decorator that measures the execution time of an asynchronous function.

    This decorator logs the time taken by the wrapped asynchronous function
    to execute, printing the elapsed time with the given name as a label.

    Args:
        name (str): A label used in the log message to identify the measured
                    function.

    Returns:
        Callable[P, Coroutine[Any, Any, T]]: A function that wraps the given async
                                             function and logs its execution time.
    """
    def decorator(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, T]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs):
            start = asyncio.get_running_loop().time()
            res = await func(*args, **kwargs)
            end = asyncio.get_running_loop().time()
            print(f"{name}: {end - start:.4f} seconds")
            return res
        return wrapper
    return decorator

def make_sync(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, T]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs):
        coro = func(*args, **kwargs)
        send_val = None
        try:
            while 1:
                send_val = coro.send(send_val)
        except StopIteration as ex:
            res: T = ex.value
            return res
        raise RuntimeError("Should not reach here")
    return wrapper