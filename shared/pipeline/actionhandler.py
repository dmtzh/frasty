from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedResultAdapter, CompletedWith
from shared.customtypes import Metadata, RunIdValue, StepIdValue
from shared.pipeline.logging import with_input_output_logging
from shared.utils.parse import parse_value
from shared.validation import ValueInvalid, ValueMissing, ValueError as ValueErr

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

type RunAsyncAction = Callable[[str, ActionDataDto], Coroutine[Any, Any, Result[None, Any]]]
type AsyncActionHandler = Callable[[str, Callable[[Result[ActionDataDto, Any]], Coroutine]], Any]

@dataclass(frozen=True)
class CompleteActionData:
    run_id: RunIdValue
    step_id: StepIdValue
    result: CompletedResult
    metadata: Metadata

    def run_complete(self, run_action: RunAsyncAction):
        action_name = COMPLETE_ACTION.get_name()
        run_id_str = self.run_id.to_value_with_checksum()
        step_id_str = self.step_id.to_value_with_checksum()
        result_dto = InputDataAdapter.to_dict(CompletedResultAdapter.to_dict(self.result))
        metadata_dict = self.metadata.to_dict()
        dto = ActionDataDto(run_id_str, step_id_str, result_dto, metadata_dict)
        return run_action(action_name, dto)

type ActionHandler[TCfg, D] = Callable[[Result[ActionData[TCfg, D], Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]

@dataclass(frozen=True)
class _ValidateActionError:
    error: str
    run_id: RunIdValue
    step_id: StepIdValue
    metadata: Metadata

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
        async def err_to_complete_action_or_none(err):
            match err:
                case _ValidateActionError():
                    completed_err = CompletedWith.Error(err.error)
                    data = CompleteActionData(err.run_id, err.step_id, completed_err, err.metadata)
                    complete_step_res = await complete_action_func(data)
                    return complete_step_res.map(lambda _: completed_err)
                case _:
                    return None
        # async def err_to_none(_):
        #     return None
        return action_data_res\
            .map(process_action_data)\
            .map_error(err_to_complete_action_or_none)\
            .merge()
            # .map_error(err_to_none)\
            # .merge()
    wrapper.__name__ = func.__name__
    return wrapper

type DtoActionHandler = Callable[[Result[ActionDataDto, Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]

def _validated_data_to_dto[TCfg, D](action_handler: ActionHandler[TCfg, D], config_validator: Callable[[dict], Result[TCfg, Any]], input_validator: Callable[[dict], Result[D, Any]]) -> DtoActionHandler:
    def validate_dto(dto: ActionDataDto) -> Result[ActionData[TCfg, D], _ValidateActionError | str]:
        run_id_res = parse_value(dto.run_id, "run_id", RunIdValue.from_value_with_checksum)
        step_id_res = parse_value(dto.step_id, "step_id", StepIdValue.from_value_with_checksum)
        config_res = config_validator(dto.data).map_error(str)
        input_res = input_validator(dto.data).map_error(str)
        metadata = Metadata(dto.metadata)
        parse_errors = [parse_err for parse_err in [run_id_res.swap().default_value(None), step_id_res.swap().default_value(None)] if parse_err is not None]
        validate_errors = [validate_err for validate_err in [config_res.swap().default_value(None), input_res.swap().default_value(None)] if validate_err is not None]
        match parse_errors, validate_errors:
            case [], []:
                return Result.Ok(ActionData(run_id_res.ok, step_id_res.ok, config_res.ok, input_res.ok, metadata))
            case [], [*v_errs]:
                err = _ValidateActionError(", ".join(v_errs), run_id_res.ok, step_id_res.ok, metadata)
                return Result.Error(err)
            case _:
                err = ", ".join(parse_errors + validate_errors)
                return Result.Error(err)
    async def wrapper(action_data_res: Result[ActionDataDto, Any]):
        validated_action_data_res = action_data_res.bind(validate_dto)
        return await action_handler(validated_action_data_res)
    wrapper.__name__ = action_handler.__name__
    return wrapper

class InputDataAdapter:
    @staticmethod
    def from_dict(data: dict) -> Result[list[dict[str, Any]], list[ValueErr]]:
        def parse_list_data_item(item) -> Result[dict[str, Any], list[ValueErr]]:
            match item:
                case {**item_dict} if item_dict:
                    all_keys_are_strings = all(isinstance(k, str) for k in item_dict)
                    return Result.Ok(item_dict) if all_keys_are_strings else Result.Error([ValueInvalid("input_data")])
                case _:
                    return Result.Error([ValueInvalid("input_data")])
        if "input_data" in data:
            match data["input_data"]:
                case []:
                    return Result.Error([ValueMissing("input_data")])
                case [*list_data]:
                    return traverse(
                        parse_list_data_item,
                        Block(list_data)
                    ).map(list)
                case _:
                    return Result.Error([ValueInvalid("input_data")])
        else:
            return Result.Error([ValueMissing("input_data")])
    
    @staticmethod
    def to_dict(input_data: dict[str, Any] | list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        match input_data:
            case {**dict_data}:
                return {"input_data": [dict_data]}
            case [*list_data]:
                return {"input_data": list_data}

class ActionHandlerFactory:
    def __init__(self, run_action: RunAsyncAction, action_handler: AsyncActionHandler):
        def complete_action(data: CompleteActionData):
            return data.run_complete(run_action)
        self._complete_action = complete_action
        self._action_handler = action_handler
    
    def create[TCfg, D](self, action: Action, config_validator: Callable[[dict], Result[TCfg, Any]], input_validator: Callable[[list[dict[str, Any]]], Result[D, Any]]):
        def config_validator_wrapper(data: dict) -> Result[TCfg, Any]:
            config_dict = {k: v for k, v in data.items() if k not in ["input_data"]}
            return config_validator(config_dict)
        def input_validator_wrapper(data: dict) -> Result[D, Any]:
            return InputDataAdapter.from_dict(data).bind(input_validator)
        def wrapper(func: Callable[[ActionData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
            validated_data_action_handler = _action_handler_adapter(func, self._complete_action)
            message_prefix = action.name
            action_handler_with_logging = with_input_output_logging(validated_data_action_handler, message_prefix)
            dto_data_action_handler = _validated_data_to_dto(action_handler_with_logging, config_validator_wrapper, input_validator_wrapper)
            return self._action_handler(action.get_name(), dto_data_action_handler)
        return wrapper
    
    def create_without_config[D](self, action: Action, input_validator: Callable[[list[dict[str, Any]]], Result[D, Any]]):
        def input_validator_wrapper(data: dict) -> Result[D, Any]:
            return InputDataAdapter.from_dict(data).bind(input_validator)
        def wrapper(func: Callable[[ActionData[None, D]], Coroutine[Any, Any, CompletedResult | None]]):
            validated_data_action_handler = _action_handler_adapter(func, self._complete_action)
            message_prefix = action.name
            action_handler_with_logging = with_input_output_logging(validated_data_action_handler, message_prefix)
            dto_data_action_handler = _validated_data_to_dto(action_handler_with_logging,  lambda _: Result.Ok(None), input_validator_wrapper)
            return self._action_handler(action.get_name(), dto_data_action_handler)
        return wrapper

class ActionDataInput(ABC):
    @abstractmethod
    def serialize(self) -> list[dict[str, Any]]:
        '''Serialize ActionDataInput. Should be overridden in subclasses.'''
        raise NotImplementedError()

def run_action_adapter(run_action: RunAsyncAction):
    def wrapper[TCfg, D: ActionDataInput](action: Action, action_data: ActionData[TCfg, D]):
        def to_dto() -> ActionDataDto:
            run_id_str = action_data.run_id.to_value_with_checksum()
            step_id_str = action_data.step_id.to_value_with_checksum()
            data_dict = InputDataAdapter.to_dict(action_data.input.serialize())
            metadata_dict = action_data.metadata.to_dict()
            return ActionDataDto(run_id_str, step_id_str, data_dict, metadata_dict)
        action_name = action.get_name()
        action_data_dto = to_dto()
        return run_action(action_name, action_data_dto)
    return wrapper
