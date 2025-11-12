from collections.abc import Callable, Coroutine
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult
from shared.customtypes import StepIdValue, RunIdValue
from shared.domaindefinition import StepDefinition
from shared.stepinputdata import StepInputData
from shared.utils.result import ResultTag

from .logging import pipeline_logger

type HandlerContinuation[T] = Callable[[Result[T, Any]], Coroutine[Any, Any, Result | None]]
type Handler[T] = Callable[[HandlerContinuation[T]], Any]

type StepHandlerContinuation[TCfg, D] = Callable[[Result[StepInputData[TCfg, D], Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]
type StepHandler[TCfg, D] = Callable[[StepHandlerContinuation[TCfg, D]], Any]
type StepDefinitionType[TCfg] = type[StepDefinition[TCfg]]
type StepDataValidator[D] = Callable[[Any], Result[D, Any]]
type StepInputAdapter[TCfg, D] = Callable[[RunIdValue, StepIdValue, TCfg, D, dict], StepInputData[TCfg, D]]

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

def with_input_output_logging[T](handler: Handler[T], message_prefix: str) -> Handler[T]:
    def logs_middleware(cont: HandlerContinuation[T]):
        async def with_logs(input_res: Result[T, Any]) -> Result | None:
            logger = pipeline_logger(message_prefix, input_res)
            match input_res:
                case Result(tag=ResultTag.OK, ok=data):
                    first_100_chars = str(data)[:100]
                    output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                    logger.info(f"RECEIVED {output}")
                case Result(tag=ResultTag.ERROR, error=error):
                    logger.error(f"RECEIVED {error}")
                case unsupported_input:
                    logger.warning(f"RECEIVED UNSUPPORTED {unsupported_input}")
            res = await cont(input_res)
            match res:
                case Result(tag=ResultTag.OK, ok=result):
                    first_100_chars = str(result)[:100]
                    output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                    logger.info(f"successfully completed with output {output}")
                case Result(tag=ResultTag.ERROR, error=error):
                    logger.error(f"failed with error {error}")
                case None:
                    logger.warning("PROCESSING SKIPPED")
            return res
        return with_logs
    return with_middleware(handler, logs_middleware)

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

class StepFunctionAdapter[TCfg, D]:
    def __init__(self, func: Callable[[StepInputData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]], complete_step_func: Callable[[RunIdValue, StepIdValue, CompletedResult, dict], Coroutine[Any, Any, Result]]):
        self._func = func
        self._complete_step_func = complete_step_func
        self.__name__ = func.__name__
    
    async def __call__(self, input_data: StepInputData[TCfg, D]) -> Result[CompletedResult, Any] | None:
        opt_completed_res = await self._func(input_data)
        match opt_completed_res:
            case None:
                return None
            case completed_res:
                complete_step_res = await self._complete_step_func(input_data.run_id, input_data.step_id, completed_res, input_data.metadata)
                return complete_step_res.map(lambda _: completed_res)

class StepHandlerAdapter[TCfg, D]:
    def __init__(self, handler: StepHandler[TCfg, D], complete_step_func: Callable[[RunIdValue, StepIdValue, CompletedResult, dict], Coroutine[Any, Any, Result]]):
        self._handler = handler
        self._complete_step_func = complete_step_func
    
    def __call__(self, func: Callable[[StepInputData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
        step_func = StepFunctionAdapter(func, self._complete_step_func)
        cont = to_continuation(step_func)
        return self._handler(cont)

class StepHandlerAdapterFactory[TCfg, D]:
    def __init__(self, handler_creator: Callable[[StepDefinitionType[TCfg], StepDataValidator[D], StepInputAdapter[TCfg, D]], StepHandler[TCfg, D]], complete_step_func: Callable[[RunIdValue, StepIdValue, CompletedResult, dict], Coroutine[Any, Any, Result]]):
        self._handler_creator = handler_creator
        self._complete_step_func = complete_step_func
    
    def __call__(self, step_definition_type: StepDefinitionType[TCfg], data_validator: StepDataValidator[D], input_adapter: StepInputAdapter[TCfg, D]):
        step_handler = self._handler_creator(step_definition_type, data_validator, input_adapter)
        return StepHandlerAdapter(step_handler, self._complete_step_func)

type Subscriber[T] = Handler[T]

class DefinitionCompletedSubscriberAdapter[T](HandlerAdapter[T]):
    '''Definition completed subscriber adapter'''
