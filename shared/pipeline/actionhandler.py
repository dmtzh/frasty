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
        data_dict = DataDtoAdapter.to_input_data(CompletedResultAdapter.to_dict(self.result))
        metadata_dict = self.metadata.to_dict()
        action_input = ActionDataDto(run_id_str, step_id_str, data_dict, metadata_dict)
        return run_action(action_name, action_input)

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

type DataDto = dict[str, Any]

class DataDtoAdapter:
    @staticmethod
    def from_input_data(input: dict) -> Result[list[DataDto], list[ValueErr]]:
        def parse_list_data_item(item) -> Result[dict[str, Any], list[ValueErr]]:
            match item:
                case {**item_dict} if item_dict:
                    all_keys_are_strings = all(isinstance(k, str) for k in item_dict)
                    return Result.Ok(item_dict) if all_keys_are_strings else Result.Error([ValueInvalid("input_data")])
                case _:
                    return Result.Error([ValueInvalid("input_data")])
        if "input_data" in input:
            match input["input_data"]:
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
    def to_input_data(dto: DataDto | list[DataDto]) -> dict:
        match dto:
            case {**dict_data}:
                return {"input_data": [dict_data]}
            case [*list_data]:
                return {"input_data": list_data}

type DtoActionHandler = Callable[[Result[ActionDataDto, Any]], Coroutine[Any, Any, Result[CompletedResult, Any] | None]]

def _validated_data_to_action_input[TCfg, D](action_handler: ActionHandler[TCfg, D], config_validator: Callable[[dict[str, Any]], Result[TCfg, Any]], input_validator: Callable[[list[DataDto]], Result[D, Any]]) -> DtoActionHandler:
    def parse_and_validate_action_input(action_input: ActionDataDto) -> Result[ActionData[TCfg, D], _ValidateActionError | str]:
        run_id_res = parse_value(action_input.run_id, "run_id", RunIdValue.from_value_with_checksum)
        step_id_res = parse_value(action_input.step_id, "step_id", StepIdValue.from_value_with_checksum)
        config_dict = {k: v for k, v in action_input.data.items() if k not in ["input_data"] and isinstance(k, str)}
        config_res = config_validator(config_dict).map_error(str)
        input_res = DataDtoAdapter.from_input_data(action_input.data).bind(input_validator).map_error(str)
        metadata = Metadata(action_input.metadata)
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
    async def wrapper(action_input_res: Result[ActionDataDto, Any]):
        validated_action_data_res = action_input_res.bind(parse_and_validate_action_input)
        return await action_handler(validated_action_data_res)
    wrapper.__name__ = action_handler.__name__
    return wrapper

class ActionHandlerFactory:
    def __init__(self, run_action: RunAsyncAction, action_handler: AsyncActionHandler):
        def complete_action(data: CompleteActionData):
            return data.run_complete(run_action)
        self._complete_action = complete_action
        self._action_handler = action_handler
    
    def create[TCfg, D](self, action: Action, config_validator: Callable[[dict[str, Any]], Result[TCfg, Any]], input_validator: Callable[[list[DataDto]], Result[D, Any]]):
        def wrapper(func: Callable[[ActionData[TCfg, D]], Coroutine[Any, Any, CompletedResult | None]]):
            validated_data_action_handler = _action_handler_adapter(func, self._complete_action)
            message_prefix = action.name
            action_handler_with_logging = with_input_output_logging(validated_data_action_handler, message_prefix)
            action_input_handler = _validated_data_to_action_input(action_handler_with_logging, config_validator, input_validator)
            return self._action_handler(action.get_name(), action_input_handler)
        return wrapper
    
    def create_without_config[D](self, action: Action, input_validator: Callable[[list[DataDto]], Result[D, Any]]):
        def wrapper(func: Callable[[ActionData[None, D]], Coroutine[Any, Any, CompletedResult | None]]):
            validated_data_action_handler = _action_handler_adapter(func, self._complete_action)
            message_prefix = action.name
            action_handler_with_logging = with_input_output_logging(validated_data_action_handler, message_prefix)
            action_input_handler = _validated_data_to_action_input(action_handler_with_logging,  lambda _: Result.Ok(None), input_validator)
            return self._action_handler(action.get_name(), action_input_handler)
        return wrapper

def run_action_adapter(run_action: RunAsyncAction):
    def wrapper[TCfg: dict[str, Any] | None, D: DataDto | list[DataDto]](action: Action, action_data: ActionData[TCfg, D]):
        def to_action_input() -> ActionDataDto:
            run_id_str = action_data.run_id.to_value_with_checksum()
            step_id_str = action_data.step_id.to_value_with_checksum()
            data_dict = DataDtoAdapter.to_input_data(action_data.input) | (action_data.config or {})
            metadata_dict = action_data.metadata.to_dict()
            return ActionDataDto(run_id_str, step_id_str, data_dict, metadata_dict)
        action_name = action.get_name()
        action_input = to_action_input()
        return run_action(action_name, action_input)
    return wrapper
