from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, StepIdValue
from shared.pipeline.logging import with_input_output_logging
from shared.utils.parse import parse_value

@dataclass(frozen=True)
class ActionDataDto:
    run_id: str
    step_id: str
    data: dict
    metadata: dict

COMPLETE_ACTION = Action(ActionName("complete_action"), ActionType.CORE)

@dataclass(frozen=True)
class ActionData[TCfg, D]:
    run_id: RunIdValue
    step_id: StepIdValue
    config: TCfg
    input: D
    metadata: Metadata

    def to_dto(self) -> ActionDataDto:
        '''Convert ActionData to ActionDataDto. Should be overridden in subclasses.'''
        raise NotImplementedError()

@dataclass(frozen=True)
class CompleteActionData:
    run_id: RunIdValue
    step_id: StepIdValue
    result: CompletedResult
    metadata: Metadata

@dataclass(frozen=True)
class CompletedDefinitionData:
    definition_id: DefinitionIdValue
    result: CompletedResult

    def to_dict(self) -> dict:
        return {"definition_id": self.definition_id.to_value_with_checksum(), "result": CompletedResultAdapter.to_dict(self.result)}

type ActionHandler[TCfg, D] = Callable[[Result[ActionData[TCfg, D], Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]

def _action_handler_adapter[TCfg, D](func: Callable[[ActionData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]], complete_action_func: Callable[[CompleteActionData], Coroutine[Any, Any, Result]]) -> ActionHandler[TCfg, D]:
    async def process_action_data(input_data: ActionData[TCfg, D]):
        opt_completed_res = await func(input_data)
        match opt_completed_res:
            case None:
                return None
            case completed_res:
                data = CompleteActionData(input_data.run_id, input_data.step_id, completed_res, input_data.metadata)
                complete_step_res = await complete_action_func(data)
                return complete_step_res.map(lambda _: completed_res)
    def wrapper(action_data_res: Result[ActionData[TCfg, D], Any]):
        async def err_to_none(_):
            return None
        return action_data_res\
            .map(process_action_data)\
            .map_error(err_to_none)\
            .merge()
    wrapper.__name__ = func.__name__
    return wrapper

type DtoActionHandler = Callable[[Result[ActionDataDto, Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]

def _validated_data_to_dto[TCfg, D](action_handler: ActionHandler[TCfg, D], config_validator: Callable[[dict], Result[TCfg, Any]], input_validator: Callable[[dict], Result[D, Any]]) -> DtoActionHandler:
    def validate_dto(dto: ActionDataDto) -> Result[ActionData[TCfg, D], str]:
        run_id_res = parse_value(dto.run_id, "run_id", RunIdValue.from_value_with_checksum)
        step_id_res = parse_value(dto.step_id, "step_id", StepIdValue.from_value_with_checksum)
        config_res = config_validator(dto.data).map_error(str)
        input_res = input_validator(dto.data).map_error(str)
        metadata = Metadata(dto.metadata)
        errors_with_none = [run_id_res.swap().default_value(None), step_id_res.swap().default_value(None), config_res.swap().default_value(None), input_res.swap().default_value(None)]
        errors = [err for err in errors_with_none if err is not None]
        match errors:
            case []:
                return Result.Ok(ActionData(run_id_res.ok, step_id_res.ok, config_res.ok, input_res.ok, metadata))
            case _:
                err = ", ".join(errors)
                return Result.Error(err)
    async def wrapper(action_data_res: Result[ActionDataDto, Any]):
        validated_action_data_res = action_data_res.bind(validate_dto)
        return await action_handler(validated_action_data_res)
    wrapper.__name__ = action_handler.__name__
    return wrapper


class ActionHandlerFactory:
    def __init__(self, run_action: Callable[[str, ActionDataDto], Coroutine[Any, Any, Result[None, Any]]], action_handler: Callable[[str, Callable[[Result[ActionDataDto, Any]], Coroutine]], Any]):
        def complete_action(data: CompleteActionData):
            action_name = COMPLETE_ACTION.get_name()
            run_id_str = data.run_id.to_value_with_checksum()
            step_id_str = data.step_id.to_value_with_checksum()
            result_dto = CompletedResultAdapter.to_dict(data.result)
            metadata_dict = data.metadata.to_dict()
            dto = ActionDataDto(run_id_str, step_id_str, result_dto, metadata_dict)
            return run_action(action_name, dto)
        self._complete_action = complete_action
        self._action_handler = action_handler
    
    def create[TCfg, D](self, action: Action, config_validator: Callable[[dict], Result[TCfg, Any]], input_validator: Callable[[dict], Result[D, Any]]):
        def wrapper(func: Callable[[ActionData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
            validated_data_action_handler = _action_handler_adapter(func, self._complete_action)
            message_prefix = action.name
            action_handler_with_logging = with_input_output_logging(validated_data_action_handler, message_prefix)
            dto_data_action_handler = _validated_data_to_dto(action_handler_with_logging, config_validator, input_validator)
            return self._action_handler(action.get_name(), dto_data_action_handler)
        return wrapper
    
    def create_without_config[D](self, action: Action, input_validator: Callable[[dict], Result[D, Any]]):
        def wrapper(func: Callable[[ActionData[None, D]], Coroutine[Any, Any, CompletedResult | None]]):
            validated_data_action_handler = _action_handler_adapter(func, self._complete_action)
            message_prefix = action.name
            action_handler_with_logging = with_input_output_logging(validated_data_action_handler, message_prefix)
            dto_data_action_handler = _validated_data_to_dto(action_handler_with_logging,  lambda _: Result.Ok(None), input_validator)
            return self._action_handler(action.get_name(), dto_data_action_handler)
        return wrapper

def run_action_adapter(run_action: Callable[[str, ActionDataDto], Coroutine[Any, Any, Result[None, Any]]]):
    def wrapper[TCfg, D](action: Action, action_data: ActionData[TCfg, D]):
        action_name = action.get_name()
        action_data_dto = action_data.to_dto()
        return run_action(action_name, action_data_dto)
    return wrapper
