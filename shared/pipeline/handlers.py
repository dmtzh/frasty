from collections.abc import Callable, Coroutine
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult
from shared.domaindefinition import StepDefinition

from .types import CompleteStepData, CompletedDefinitionData, StepData

type HandlerContinuation[T] = Callable[[Result[T, Any]], Coroutine[Any, Any, Result | None]]
type Handler[T] = Callable[[HandlerContinuation[T]], Any]

type StepHandlerContinuation[TCfg, D] = Callable[[Result[StepData[TCfg, D], Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]
type StepHandler[TCfg, D] = Callable[[StepHandlerContinuation[TCfg, D]], Any]
type StepDefinitionType[TCfg] = type[StepDefinition[TCfg]]

class to_continuation_with_custom_name[T]:
    def __init__(self, continuation: HandlerContinuation[T], name: str):
        self._continuation = continuation
        self.__name__ = name
    
    async def __call__(self, input_res: Result[T, Any]) -> Result | None:
        return await self._continuation(input_res)

def map_handler[T, R](handler: Handler[T], func: Callable[[Result[T, Any]], Result[R, Any]]) -> Handler[R]:
    return lambda cont_r: handler(to_continuation_with_custom_name[T](lambda t_res: cont_r(func(t_res)), cont_r.__name__))

def with_middleware[T](handler: Handler[T], func: Callable[[HandlerContinuation[T]], HandlerContinuation[T]]) -> Handler[T]:
    return lambda cont: handler(to_continuation_with_custom_name(func(cont), cont.__name__))

class to_continuation[T]:
    def __init__(self, func: Callable[[T], Coroutine[Any, Any, Result | None]]):
        self._func = func
        self.__name__ = func.__name__
    
    async def __call__(self, input_res: Result[T, Any]) -> Result | None:
        async def err_to_none(_):
            return None
        return await input_res\
            .map(self._func)\
            .map_error(err_to_none)\
            .merge()

class HandlerAdapter[T]:
    def __init__(self, handler: Handler[T]):
        self._handler = handler
    
    def __call__(self, func: Callable[[T], Coroutine[Any, Any, Result | None]]):
        cont = to_continuation(func)
        return self._handler(cont)

class to_step_function[TCfg, D]:
    def __init__(self, func: Callable[[StepData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]], complete_step_func: Callable[[CompleteStepData], Coroutine[Any, Any, Result]]):
        self._func = func
        self._complete_step_func = complete_step_func
        self.__name__ = func.__name__
    
    async def __call__(self, input_data: StepData[TCfg, D]) -> Result[CompletedResult, Any] | None:
        opt_completed_res = await self._func(input_data)
        match opt_completed_res:
            case None:
                return None
            case completed_res:
                data = CompleteStepData(input_data.run_id, input_data.step_id, completed_res, input_data.metadata)
                complete_step_res = await self._complete_step_func(data)
                return complete_step_res.map(lambda _: completed_res)

class StepHandlerAdapter[TCfg, D]:
    def __init__(self, handler: StepHandler[TCfg, D], complete_step_func: Callable[[CompleteStepData], Coroutine[Any, Any, Result]]):
        self._handler = handler
        self._complete_step_func = complete_step_func
    
    def __call__(self, func: Callable[[StepData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
        step_func = to_step_function(func, self._complete_step_func)
        cont = to_continuation(step_func)
        return self._handler(cont)

class StepHandlerAdapterFactory[TCfg]:
    def __init__(self, handler_creator: Callable[[StepDefinitionType[TCfg]], StepHandler[TCfg, Any]], complete_step_func: Callable[[CompleteStepData], Coroutine[Any, Any, Result]]):
        self._handler_creator = handler_creator
        self._complete_step_func = complete_step_func
    
    def __call__[D](self, step_definition_type: StepDefinitionType[TCfg], data_validator: Callable[[Any], Result[D, Any]]):
        step_handler_with_unvalidated_data = self._handler_creator(step_definition_type)
        step_handler = map_handler(step_handler_with_unvalidated_data, lambda data_res: data_res.bind(lambda data: data_validator(data.data).map(lambda d: StepData(data.run_id, data.step_id, data.definition, d, data.metadata))))
        return StepHandlerAdapter(step_handler, self._complete_step_func)

type Subscriber = Handler[CompletedDefinitionData]

def only_from(subscriber: Subscriber, from_: str):
    def short_circuit_if_not_from(decoratee: HandlerContinuation[CompletedDefinitionData]):
        async def middleware_func(data: CompletedDefinitionData):
            return await decoratee(Result.Ok(data)) if data.metadata.get_from() == from_ else None
        return to_continuation(middleware_func)
    return with_middleware(subscriber, short_circuit_if_not_from)

class DefinitionCompletedSubscriberAdapter[T](HandlerAdapter[T]):
    '''Definition completed subscriber adapter'''
