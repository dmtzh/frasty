from collections.abc import Callable, Coroutine
import functools
from logging import LoggerAdapter
import pickle
from typing import Any, ParamSpec, TypeVar

from expression import Result
from faststream.broker.message import StreamMessage
from faststream.rabbit import RabbitMessage

from shared.customtypes import Error, IdValue, RunIdValue, StepIdValue, TaskIdValue
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.utils.parse import parse_from_dict, parse_value
from shared.utils.result import ResultTag

from .client import RabbitMQClient
from .error import rabbit_message_error_creator, RabbitMessageErrorCreator, ParseError, ValidationError, RabbitMessageError
from .logging import RabbitMessageLoggerCreator
from .pythonpickle import DataWithCorrelationId, PythonPickleMessage
from .rabbitmiddlewares import error_result_to_negative_acknowledge_middleware, command_handler_logging_middleware, RequeueChance

D = TypeVar("D")
TCfg = TypeVar("TCfg")
R = TypeVar("R")
P = ParamSpec("P")
COMPLETE_STEP_COMMAND = "complete_step"

class _python_pickle:
    @staticmethod
    def data_to_message(run_id: RunIdValue, step_id: StepIdValue, result: CompletedResult, metadata: dict) -> PythonPickleMessage:
        result_dto = CompletedResultAdapter.to_dict(result)
        is_metadata_valid = isinstance(metadata, dict)
        if not is_metadata_valid:
            raise ValueError(f"Invalid 'metadata' value {metadata}")
        run_id_dict = {"run_id": run_id.to_value_with_checksum()}
        step_id_dict = {"step_id": step_id.to_value_with_checksum()}
        command_data = run_id_dict | step_id_dict | {"result": result_dto, "metadata": metadata}
        correlation_id = run_id_dict["run_id"]
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)

    class decoder():
        def __init__(self, input_adapter: Callable[[RunIdValue, StepIdValue, CompletedResult, dict], R]):
            self._input_adapter = input_adapter
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[tuple[str, str, dict, dict], RabbitMessageError]:
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
            
            if "result" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'result' key not found in {decoded}"))
            result_unvalidated = decoded["result"]
            if not isinstance(result_unvalidated, dict):
                return Result.Error(rabbit_msg_err(ParseError, f"'result' should be {dict.__name__} value, got {type(result_unvalidated).__name__}"))

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

            parsed_data = run_id_unvalidated, step_id_unvalidated, result_unvalidated, metadata_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(input_adapter: Callable[[RunIdValue, StepIdValue, CompletedResult, dict], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str, str, dict, dict]) -> Result[R, RabbitMessageError]:
            run_id_unvalidated, step_id_unvalidated, result_unvalidated, metadata_unvalidated = parsed_data    
            run_id_res = parse_value(run_id_unvalidated, "run_id", RunIdValue.from_value_with_checksum)
            step_id_res = parse_value(step_id_unvalidated, "step_id", StepIdValue.from_value_with_checksum)
            result_res = CompletedResultAdapter.from_dict(result_unvalidated).map_error(lambda _: rabbit_msg_err(ValidationError, f"Invalid 'result' value {result_unvalidated}"))
            match run_id_res, step_id_res, result_res:
                case Result(tag=ResultTag.OK, ok=run_id), Result(tag=ResultTag.OK, ok=step_id), Result(tag=ResultTag.OK, ok=result):
                    res = input_adapter(run_id, step_id, result, metadata_unvalidated)
                    return Result.Ok(res)
                case _:
                    errors_with_none = [run_id_res.swap().default_value(None), step_id_res.swap().default_value(None), result_res.swap().default_value(None)]
                    errors = [err for err in errors_with_none if err is not None]
                    err = rabbit_msg_err(ValidationError, ", ".join(errors))
                    return Result.Error(err)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding {COMPLETE_STEP_COMMAND}", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

    @staticmethod
    def create_logger(msg: StreamMessage[Any]) -> LoggerAdapter:
        logger_creator = RabbitMessageLoggerCreator(msg.raw_message)
        if not isinstance(msg.body, bytes):
            return logger_creator.create(TaskIdValue(None), RunIdValue(None), StepIdValue(None))
        try:
            decoded = pickle.loads(msg.body)
        except Exception:
            return logger_creator.create(TaskIdValue(None), RunIdValue(None), StepIdValue(None))
        if not isinstance(decoded, dict):
            return logger_creator.create(TaskIdValue(None), RunIdValue(None), StepIdValue(None))
        
        metadata_res = parse_from_dict(decoded, "metadata", lambda m: m if isinstance(m, dict) else None)
        task_id_res = metadata_res.bind(lambda m: parse_from_dict(m, "task_id", TaskIdValue.from_value_with_checksum))
        run_id_res = parse_from_dict(decoded, "run_id", RunIdValue.from_value_with_checksum)
        step_id_res = parse_from_dict(decoded, "step_id", StepIdValue.from_value_with_checksum)
        task_id = task_id_res.default_value(TaskIdValue(None))
        run_id = run_id_res.default_value(RunIdValue(None))
        step_id = step_id_res.default_value(StepIdValue(None))
        return logger_creator.create(task_id, run_id, step_id)

def run(rabbit_client: RabbitMQClient, run_id: RunIdValue, step_id: StepIdValue, result: CompletedResult, metadata: dict):
    message = _python_pickle.data_to_message(run_id, step_id, result, metadata)
    return rabbit_client.send_command(COMPLETE_STEP_COMMAND, message)

class handler:
    def __init__(self, rabbit_client: RabbitMQClient, input_adapter: Callable[[RunIdValue, StepIdValue, CompletedResult, dict], R]):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return input_adapter
        self._rabbit_client = rabbit_client
        self._input_adapter = validate_input_adapter()
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, Result | None]]):
        decoder = _python_pickle.decoder(self._input_adapter)
        middlewares = (
            error_result_to_negative_acknowledge_middleware(RequeueChance.HIGH),
            command_handler_logging_middleware(COMPLETE_STEP_COMMAND, _python_pickle.create_logger)
        )
        return self._rabbit_client.command_handler(COMPLETE_STEP_COMMAND, decoder, middlewares)(func)
