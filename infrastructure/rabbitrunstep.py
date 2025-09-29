from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
import functools
import inspect
from logging import LoggerAdapter
import pickle
from typing import Any, ParamSpec, TypeVar

from expression import Result
from faststream.broker.message import StreamMessage
from faststream.rabbit import RabbitMessage

from infrastructure import rabbitcompletestep as rabbit_complete_step
from shared.customtypes import Error, IdValue, StepIdValue, RunIdValue, TaskIdValue
import shared.domaindefinition as shdomaindef
import shared.dtodefinition as shdtodef
from shared.completedresult import CompletedWith, CompletedResult
from shared.infrastructure.rabbitmq.client import RabbitMQClient, Error as RabbitClientError
from shared.infrastructure.rabbitmq.error import rabbit_message_error_creator, RabbitMessageErrorCreator, ParseError, ValidationError, RabbitMessageError
from shared.infrastructure.rabbitmq.logging import RabbitMessageLoggerCreator
from shared.infrastructure.rabbitmq.pythonpickle import DataWithCorrelationId, PythonPickleMessage
from shared.infrastructure.stepdefinitioncreatorsstore import get_step_definition_name
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.parse import parse_from_dict, parse_value
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid

from .rabbitmiddlewares import error_result_to_negative_acknowledge_middleware, command_handler_logging_middleware, RequeueChance

D = TypeVar("D")
TCfg = TypeVar("TCfg")
R = TypeVar("R")
P = ParamSpec("P")

class _python_pickle:
    @staticmethod
    def data_to_message(run_id: RunIdValue, step_id: StepIdValue, definition: shdomaindef.StepDefinition, data: Any, metadata: dict) -> PythonPickleMessage:
        step_definition_dict = shdtodef.StepDefinitionAdapter.to_dict(definition)
        is_metadata_valid = isinstance(metadata, dict)
        if not is_metadata_valid:
            raise ValueError(f"Invalid 'metadata' value {metadata}")
        ids_dict = {"run_id": run_id.to_value_with_checksum(), "step_id": step_id.to_value_with_checksum()}
        command_data = ids_dict | {"config": step_definition_dict, "definition": step_definition_dict, "data": data, "metadata": metadata}
        correlation_id = ids_dict["run_id"]
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
    class decoder():
        def __init__(self, command: str, data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D, dict], R]):
            self._command = command
            self._data_validator = data_validator
            self._input_adapter = input_adapter
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[tuple[str, str, dict, Any, dict], RabbitMessageError]:
            correlation_id = IdValue.from_value_with_checksum(msg.correlation_id)
            if correlation_id is None:
                return Result.Error(rabbit_msg_err(ValidationError, "Invalid 'correlation_id'"))
            if not isinstance(msg.body, bytes):
                return Result.Error(rabbit_msg_err(ParseError, f"Expected body of bytes type, got {type(msg.body).__name__}"))
            try:
                decoded = pickle.loads(msg.body)
            except Exception as e:
                message = Error.from_exception(e).message
                return Result.Error(rabbit_msg_err(ParseError, message))
            if not isinstance(decoded, dict):
                return Result.Error(rabbit_msg_err(ParseError, f"Expected body of dict type, got {type(decoded).__name__}"))
            
            if "definition" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'definition' key not found in {decoded}"))
            step_definition_unvalidated = decoded["definition"]
            if not isinstance(step_definition_unvalidated, dict):
                return Result.Error(rabbit_msg_err(ParseError, f"'definition' should be {dict.__name__} value, got {type(step_definition_unvalidated).__name__}"))
        
            if "data" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'data' key not found in {decoded}"))
            data_unvalidated = decoded["data"]

            if "metadata" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'metadata' key not found in {decoded}"))
            metadata_unvalidated = decoded["metadata"]
            if not isinstance(metadata_unvalidated, dict):
                return Result.Error(rabbit_msg_err(ParseError, f"'metadata' should be {dict.__name__} value, got {type(metadata_unvalidated).__name__}"))
            
            if "run_id" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'run_id' key not found in {decoded}"))
            run_id_unvalidated = decoded["run_id"]
            if not isinstance(run_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'run_id' should be string value, got {type(run_id_unvalidated).__name__}"))
            if run_id_unvalidated != msg.correlation_id:
                return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'run_id' value {run_id_unvalidated}"))
            
            if "step_id" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'step_id' key not found in {decoded}"))
            step_id_unvalidated = decoded["step_id"]
            if not isinstance(step_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'step_id' should be string value, got {type(step_id_unvalidated).__name__}"))

            parsed_data = run_id_unvalidated, step_id_unvalidated, step_definition_unvalidated, data_unvalidated, metadata_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(command: str, data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D, dict], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str, str, dict, Any, dict]) -> Result[R, RabbitMessageError]:
            run_id_unvalidated, step_id_unvalidated, step_definition_unvalidated, data_unvalidated, metadata_unvalidated = parsed_data
            run_id_res = parse_value(run_id_unvalidated, "run_id", RunIdValue.from_value_with_checksum)
            step_id_res = parse_value(step_id_unvalidated, "step_id", StepIdValue.from_value_with_checksum)
            cfg_res = shdtodef.StepDefinitionAdapter.from_dict(step_definition_unvalidated)\
                .filter(lambda step_def: get_step_definition_name(type(step_def)) == command, [ValueInvalid("step")])\
                .map(lambda definition: definition.config)\
                .map_error(lambda error: f"Invalid 'definition' data: {step_definition_unvalidated}; error: {error}")
            data_res = data_validator(data_unvalidated)\
                .map_error(lambda _: f"Invalid 'data' value {data_unvalidated}")
            match run_id_res, step_id_res, cfg_res, data_res:
                case Result(tag=ResultTag.OK, ok=run_id), Result(tag=ResultTag.OK, ok=step_id), Result(tag=ResultTag.OK, ok=cfg), Result(tag=ResultTag.OK, ok=data):
                    res = input_adapter(run_id, step_id, cfg, data, metadata_unvalidated)
                    return Result.Ok(res)
                case _:
                    errors_with_none = [run_id_res.swap().default_value(None), step_id_res.swap().default_value(None), cfg_res.swap().default_value(None), data_res.swap().default_value(None)]
                    errors = [err for err in errors_with_none if err is not None]
                    err = rabbit_msg_err(ValidationError, ", ".join(errors))
                    return Result.Error(err)

        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding RUN_STEP_COMMAND({self._command})", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, self._command, self._data_validator, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

    @staticmethod
    def parse_message(msg: StreamMessage[Any]) -> tuple[TaskIdValue, RunIdValue, StepIdValue, dict]:
        if not isinstance(msg.body, bytes):
            return TaskIdValue(None), RunIdValue(None), StepIdValue(None), {}
        try:
            decoded = pickle.loads(msg.body)
        except Exception:
            return TaskIdValue(None), RunIdValue(None), StepIdValue(None), {}
        if not isinstance(decoded, dict):
            return TaskIdValue(None), RunIdValue(None), StepIdValue(None), {}
        metadata_res = parse_from_dict(decoded, "metadata", lambda m: m if isinstance(m, dict) else None)
        task_id_res = metadata_res.bind(lambda m: parse_from_dict(m, "task_id", TaskIdValue.from_value_with_checksum))
        run_id_res = parse_from_dict(decoded, "run_id", RunIdValue.from_value_with_checksum)
        step_id_res = parse_from_dict(decoded, "step_id", StepIdValue.from_value_with_checksum)
        task_id = task_id_res.default_value(TaskIdValue(None))
        run_id = run_id_res.default_value(RunIdValue(None))
        step_id = step_id_res.default_value(StepIdValue(None))
        metadata = metadata_res.default_value({})
        return task_id, run_id, step_id, metadata
    
    @staticmethod
    def create_logger(msg: StreamMessage[Any]) -> LoggerAdapter:
        task_id, run_id, step_id, _ = _python_pickle.parse_message(msg)
        logger_creator = RabbitMessageLoggerCreator(msg.raw_message)
        return logger_creator.create(task_id, run_id, step_id)

@dataclass(frozen=True)
class RunStepData[TCfg, D]:
    run_id: RunIdValue
    step_id: StepIdValue
    config: TCfg
    data: D
    metadata: dict

def run(rabbit_client: RabbitMQClient, run_id: RunIdValue, step_id: StepIdValue, definition: shdomaindef.StepDefinition, data: Any, metadata: dict):
    command = get_step_definition_name(type(definition))
    message = _python_pickle.data_to_message(run_id, step_id, definition, data, metadata)
    return rabbit_client.send_command(command, message)

class _send_response_middleware:
    def __init__(self, rabbit_client: RabbitMQClient, message_to_data: Callable[[StreamMessage[Any]], tuple[TaskIdValue, RunIdValue, StepIdValue, dict]]):
        self._rabbit_client = rabbit_client
        self._message_to_data = message_to_data

    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def _send_response(self, run_id: RunIdValue, step_id: StepIdValue, metadata: dict, result: CompletedResult):
        return rabbit_complete_step.run(self._rabbit_client, run_id, step_id, result, metadata)
    
    async def __call__(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        res = await call_next(msg)
        # should_send_reponse = isinstance(res, CompletedResult.__value__)
        if type(res) is not CompletedWith.Data and type(res) is not CompletedWith.NoData and type(res) is not CompletedWith.Error:
            return res
        _, run_id, step_id, metadata = self._message_to_data(msg)
        send_response_res = await self._send_response(run_id, step_id, metadata, res)
        return send_response_res.map(lambda _: res)

class handler[TCfg, D, R]:
    @staticmethod
    def _consume_input(input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D], R] | Callable[[RunIdValue, StepIdValue, TCfg, D, dict], R], run_id: RunIdValue, step_id: StepIdValue, cfg: TCfg, data: D, metadata: dict):
        params = inspect.signature(input_adapter).parameters
        include_metadata_param = len(params) > 4
        if include_metadata_param:
            params_array = [run_id, step_id, cfg, data, metadata]
            return input_adapter(*params_array)
        else:
            params_array = [run_id, step_id, cfg, data]
            return input_adapter(*params_array)

    def __init__(self, rabbit_client: RabbitMQClient, step_definition_type: type[shdomaindef.StepDefinition[TCfg]], data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D], R] | Callable[[RunIdValue, StepIdValue, TCfg, D, dict], R]):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return functools.partial(self._consume_input, input_adapter)
        self._rabbit_client = rabbit_client
        self._step_name = get_step_definition_name(step_definition_type)
        self._data_validator = data_validator
        self._input_adapter = validate_input_adapter()
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, CompletedResult | None]]):
        decoder = _python_pickle.decoder(self._step_name, self._data_validator, self._input_adapter)
        middlewares = (
            error_result_to_negative_acknowledge_middleware(RequeueChance.FIFTY_FIFTY),
            command_handler_logging_middleware(self._step_name, _python_pickle.create_logger),
            _send_response_middleware(self._rabbit_client, _python_pickle.parse_message)
        )
        return self._rabbit_client.command_handler(self._step_name, decoder, middlewares)(func)
