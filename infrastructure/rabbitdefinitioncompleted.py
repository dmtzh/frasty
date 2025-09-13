from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
import functools
import inspect
from logging import LoggerAdapter
import pickle
import secrets
from typing import Any, ParamSpec, TypeVar

from expression import Result
from faststream.exceptions import NackMessage
from faststream.rabbit import RabbitMessage

from shared.customtypes import Error, IdValue, RunIdValue, TaskIdValue
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.infrastructure.rabbitmq.client import RabbitMQClient
from shared.infrastructure.rabbitmq.error import ParseError, RabbitMessageError, RabbitMessageErrorCreator, ValidationError, rabbit_message_error_creator
from shared.infrastructure.rabbitmq.logging import RabbitMessageLoggerCreator
from shared.infrastructure.rabbitmq.pythonpickle import DataWithCorrelationId, PythonPickleMessage
from shared.utils.result import ResultTag

DEFINITION_COMPLETED_EVENT = "definition_completed"
DEFINITION_COMPLETED_EVENT_GROUP = "completed_definitions"

R = TypeVar("R")
P = ParamSpec("P")
T = TypeVar("T")

class _python_pickle:
    @staticmethod
    def data_to_message(opt_task_id: IdValue | None, run_id: IdValue, definition_id: IdValue, result: CompletedResult, metadata: dict) -> PythonPickleMessage:
        result_dto = CompletedResultAdapter.to_dict(result)
        is_metadata_valid = isinstance(metadata, dict)
        if not is_metadata_valid:
            raise ValueError(f"Invalid 'metadata' value {metadata}")
        task_id_dict = {"task_id": opt_task_id.to_value_with_checksum()} if opt_task_id is not None else {}
        run_id_with_checksum = run_id.to_value_with_checksum()
        metadata_dict = metadata | task_id_dict |\
            {"run_id": run_id_with_checksum, "definition_id": definition_id.to_value_with_checksum()}
        event_data = {"result": result_dto, "metadata": metadata_dict}
        correlation_id = run_id_with_checksum
        data_with_correlation_id = DataWithCorrelationId(event_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
    class decoder():
        def __init__(self, input_adapter: Callable[[IdValue | None, RunIdValue, IdValue, CompletedResult, dict, LoggerAdapter], R]):
            self._input_adapter = input_adapter
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[tuple[str | None, str, str, dict, dict], RabbitMessageError]:
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
            
            opt_task_id_unvalidated = metadata_unvalidated.get("task_id")
            if opt_task_id_unvalidated is not None and not isinstance(opt_task_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'task_id' should be string value, got {type(opt_task_id_unvalidated).__name__}"))

            if "run_id" not in metadata_unvalidated:
                return Result.Error(rabbit_msg_err(ParseError, f"'run_id' key not found in {metadata_unvalidated}"))
            run_id_unvalidated = metadata_unvalidated["run_id"]
            if not isinstance(run_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'run_id' should be string value, got {type(run_id_unvalidated).__name__}"))
            if run_id_unvalidated != msg.correlation_id:
                return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'run_id' value {run_id_unvalidated}"))
            
            if "definition_id" not in metadata_unvalidated:
                return Result.Error(rabbit_msg_err(ParseError, f"'definition_id' key not found in {metadata_unvalidated}"))
            definition_id_unvalidated = metadata_unvalidated["definition_id"]
            if not isinstance(definition_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'definition_id' should be string value, got {type(definition_id_unvalidated).__name__}"))
            
            parsed_data = opt_task_id_unvalidated, run_id_unvalidated, definition_id_unvalidated, result_unvalidated, metadata_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(logger_creator: RabbitMessageLoggerCreator, input_adapter: Callable[[IdValue | None, RunIdValue, IdValue, CompletedResult, dict, LoggerAdapter], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str | None, str, str, dict, dict]) -> Result[R, RabbitMessageError]:
            def validate_id[T](id_parser: Callable[[str], T | None], id_unvalidated: str, id_name: str) -> Result[T, str]:
                opt_id = id_parser(id_unvalidated)
                match opt_id:
                    case None:
                        return Result.Error(f"Invalid '{id_name}' value {id_unvalidated}")
                    case valid_id:
                        return Result.Ok(valid_id)
            def validate_task_id() -> Result[IdValue | None, str]:
                match opt_task_id_unvalidated:
                    case None:
                        return Result.Ok(None)
                    case task_id_unvalidated:
                        opt_task_id  = TaskIdValue.from_value_with_checksum(task_id_unvalidated)
                        match opt_task_id:
                            case None:
                                return Result.Error(f"Invalid 'task_id' value {task_id_unvalidated}")
                            case task_id:
                                return Result.Ok(task_id)
            
            opt_task_id_unvalidated, run_id_unvalidated, definition_id_unvalidated, result_unvalidated, metadata_unvalidated = parsed_data
            opt_task_id_res = validate_task_id()
            run_id_res = validate_id(RunIdValue.from_value_with_checksum, run_id_unvalidated, "run_id")
            definition_id_res = validate_id(IdValue.from_value_with_checksum, definition_id_unvalidated, "definition_id")
            result_res = CompletedResultAdapter.from_dict(result_unvalidated).map_error(lambda _: rabbit_msg_err(ValidationError, f"Invalid 'result' value {result_unvalidated}"))
            match opt_task_id_res, run_id_res, definition_id_res, result_res:
                case Result(tag=ResultTag.OK, ok=opt_task_id), Result(tag=ResultTag.OK, ok=run_id), Result(tag=ResultTag.OK, ok=definition_id), Result(tag=ResultTag.OK, ok=result):
                    logger = logger_creator.create(opt_task_id, run_id, None)
                    res = input_adapter(opt_task_id, run_id, definition_id, result, metadata_unvalidated, logger)
                    return Result.Ok(res)
                case _:
                    errors_with_none = [opt_task_id_res.swap().default_value(None), run_id_res.swap().default_value(None), definition_id_res.swap().default_value(None), result_res.swap().default_value(None)]
                    errors = [err for err in errors_with_none if err is not None]
                    err = rabbit_msg_err(ValidationError, ", ".join(errors))
                    return Result.Error(err)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            logger_creator = RabbitMessageLoggerCreator(msg.raw_message)
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding {DEFINITION_COMPLETED_EVENT}", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, logger_creator, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

@dataclass(frozen=True)
class DefinitionCompletedData:
    opt_task_id: IdValue | None
    run_id: RunIdValue
    definition_id: IdValue
    result: CompletedResult
    metadata: dict

def publish(rabbit_client: RabbitMQClient, opt_task_id: IdValue | None, run_id: IdValue, definition_id: IdValue, result: CompletedResult, metadata: dict):
    message = _python_pickle.data_to_message(opt_task_id, run_id, definition_id, result, metadata)
    return rabbit_client.send_event(DEFINITION_COMPLETED_EVENT, DEFINITION_COMPLETED_EVENT_GROUP, message)

class subscriber:
    def __init__(self, rabbit_client: RabbitMQClient, input_adapter: Callable[[IdValue | None, RunIdValue, IdValue, CompletedResult], R] | Callable[[IdValue | None, RunIdValue, IdValue, CompletedResult, dict], R], queue_name: str | None):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return input_adapter
        self._rabbit_client = rabbit_client
        self._input_adapter = validate_input_adapter()
        self._queue_name = queue_name
    
    def _consume_input(self, opt_task_id: IdValue | None, run_id: RunIdValue, definition_id: IdValue, result: CompletedResult, metadata: dict, logger: LoggerAdapter):
            logger.info(f"RECEIVED metadata {metadata}")
            params = inspect.signature(self._input_adapter).parameters
            include_metadata_param = len(params) > 4
            if include_metadata_param:
                params_array = [opt_task_id, run_id, definition_id, result, metadata]
                return self._input_adapter(*params_array)
            else:
                params_array = [opt_task_id, run_id, definition_id, result]
                return self._input_adapter(*params_array)
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, Any]]):
        decoder = _python_pickle.decoder(self._consume_input)
        return self._rabbit_client.event_handler(DEFINITION_COMPLETED_EVENT, DEFINITION_COMPLETED_EVENT_GROUP, self._queue_name, decoder)(func)

class Severity(StrEnum):
    LOW = "low"
    HIGH = "high"

def handle_processing_failure(severity: Severity):
    match severity:
        case Severity.LOW:
            bits = secrets.randbits(1)
        case Severity.HIGH:
            bits = secrets.randbits(3)
    requeue = bits > 0
    raise NackMessage(requeue=requeue)