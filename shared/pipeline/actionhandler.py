from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import wraps
from typing import Any

from expression import Result

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.customtypes import Metadata, RunIdValue, StepIdValue
from shared.pipeline.logging import with_input_output_logging
from shared.utils.parse import parse_value

@dataclass(frozen=True)
class ActionDataDto:
    run_id: str
    step_id: str
    config: dict | None
    data: dict | list
    metadata: dict

type ActionHandler = Callable[[Result[ActionDataDto, Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]

class CompleteAction(Action):
    def __init__(self):
        super().__init__(ActionName("complete_action"), ActionType.CORE)

@dataclass(frozen=True)
class ActionData[TCfg, D]:
    run_id: RunIdValue
    step_id: StepIdValue
    config: TCfg
    data: D
    metadata: Metadata

@dataclass(frozen=True)
class CompleteActionData:
    run_id: RunIdValue
    step_id: StepIdValue
    result: CompletedResult
    metadata: Metadata

type ActionHandlerAdapterResult[TCfg, D] = Callable[[Result[ActionData[TCfg, D], Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]

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

def action_handler_adapter[TCfg, D](func: Callable[[ActionData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]], complete_action_func: Callable[[CompleteActionData], Coroutine[Any, Any, Result]]) -> ActionHandlerAdapterResult[TCfg, D]:
    @wraps(func)
    async def process_action_data(input_data: ActionData[TCfg, D]):
        opt_completed_res = await func(input_data)
        match opt_completed_res:
            case None:
                return None
            case completed_res:
                data = CompleteActionData(input_data.run_id, input_data.step_id, completed_res, input_data.metadata)
                complete_step_res = await complete_action_func(data)
                return complete_step_res.map(lambda _: completed_res)
    return to_continuation(process_action_data)

class validated_data_to_dto[TCfg, D]:
    def __init__(self, action_handler: ActionHandlerAdapterResult[TCfg, D], config_validator: Callable[[dict | None], Result[TCfg, Any]], data_validator: Callable[[dict | list], Result[D, Any]]):
        self._action_handler = action_handler
        self._config_validator = config_validator
        self._data_validator = data_validator
        self.__name__ = action_handler.__name__

    async def __call__(self, action_data_res: Result[ActionDataDto, Any]):
        def validate_dto(dto: ActionDataDto) -> Result[ActionData[TCfg, D], str]:
            run_id_res = parse_value(dto.run_id, "run_id", RunIdValue.from_value_with_checksum)
            step_id_res = parse_value(dto.step_id, "step_id", StepIdValue.from_value_with_checksum)
            config_res = self._config_validator(dto.config).map_error(str)
            data_res = self._data_validator(dto.data).map_error(str)
            metadata = Metadata(dto.metadata)
            errors_with_none = [run_id_res.swap().default_value(None), step_id_res.swap().default_value(None), config_res.swap().default_value(None), data_res.swap().default_value(None)]
            errors = [err for err in errors_with_none if err is not None]
            match errors:
                case []:
                    return Result.Ok(ActionData(run_id_res.ok, step_id_res.ok, config_res.ok, data_res.ok, metadata))
                case _:
                    err = ", ".join(errors)
                    return Result.Error(err)
        validated_action_data_res = action_data_res.bind(validate_dto)
        return await self._action_handler(validated_action_data_res)

class ActionHandlerFactory:
    def __init__(self, run_action: Callable[[str, ActionDataDto], Coroutine[Any, Any, Result[None, Any]]], action_handler: Callable[[str, ActionHandler], Any]):
        def complete_action(data: CompleteActionData):
            action_name = CompleteAction().get_name()
            run_id_str = data.run_id.to_value_with_checksum()
            step_id_str = data.step_id.to_value_with_checksum()
            result_dto = CompletedResultAdapter.to_dict(data.result)
            metadata_dict = data.metadata.to_dict()
            dto = ActionDataDto(run_id_str, step_id_str, None, result_dto, metadata_dict)
            return run_action(action_name, dto)
        self._complete_action = complete_action
        self._action_handler = action_handler
    
    def create[TCfg, D](self, action: Action, config_validator: Callable[[dict | None], Result[TCfg, Any]], data_validator: Callable[[dict | list], Result[D, Any]]):
        def wrapper(func: Callable[[ActionData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
            action_handler = action_handler_adapter(func, self._complete_action)
            message_prefix = action.name
            action_handler_with_logging = with_input_output_logging(action_handler, message_prefix)
            action_data_dto_handler = validated_data_to_dto(action_handler_with_logging, config_validator, data_validator)
            return self._action_handler(action.get_name(), action_data_dto_handler)
        return wrapper