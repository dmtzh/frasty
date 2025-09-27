from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
import functools
from logging import LoggerAdapter
import pickle
import secrets
from typing import Any, ParamSpec, TypeVar

from expression import Result
from faststream.broker.message import StreamMessage
from faststream.exceptions import NackMessage
from faststream.rabbit import RabbitMessage

from shared.customtypes import Error, RunIdValue, TaskIdValue, StepIdValue
from shared.infrastructure.rabbitmq.client import RabbitMQClient
from shared.infrastructure.rabbitmq.error import rabbit_message_error_creator, RabbitMessageErrorCreator, ParseError, ValidationError, RabbitMessageError
from shared.infrastructure.rabbitmq.logging import RabbitMessageLoggerCreator
from shared.infrastructure.rabbitmq.pythonpickle import DataWithCorrelationId, PythonPickleMessage
from shared.utils.parse import parse_value
from shared.utils.result import ResultTag

R = TypeVar("R")
P = ParamSpec("P")
RUN_TASK_COMMAND = "run_task"

class _python_pickle:
    @staticmethod
    def data_to_message(task_id: TaskIdValue, run_id: RunIdValue, from_: str, metadata: dict) -> PythonPickleMessage:
        is_metadata_valid = isinstance(metadata, dict)
        if not is_metadata_valid:
            raise ValueError(f"Invalid 'metadata' value {metadata}")
        task_id_with_checksum = task_id.to_value_with_checksum()
        run_id_with_checksum = run_id.to_value_with_checksum()
        metadata_dict = metadata |\
            {"from": from_}
        command_data = {"task_id": task_id_with_checksum, "run_id": run_id_with_checksum, "metadata": metadata_dict}
        correlation_id = run_id_with_checksum
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
    class decoder():
        def __init__(self, input_adapter: Callable[[TaskIdValue, RunIdValue, dict], R]):
            self._input_adapter = input_adapter
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[tuple[str, str, dict], RabbitMessageError]:
            correlation_id = RunIdValue.from_value_with_checksum(msg.correlation_id)
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
            
            if "metadata" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'metadata' key not found in {decoded}"))
            metadata_unvalidated = decoded["metadata"]
            if not isinstance(metadata_unvalidated, dict):
                return Result.Error(rabbit_msg_err(ParseError, f"'metadata' should be {dict.__name__} value, got {type(metadata_unvalidated).__name__}"))
            
            if "task_id" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'task_id' key not found in {decoded}"))
            task_id_unvalidated = decoded["task_id"]
            if not isinstance(task_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'task_id' should be string value, got {type(task_id_unvalidated).__name__}"))

            if "run_id" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'run_id' key not found in {decoded}"))
            run_id_unvalidated = decoded["run_id"]
            if not isinstance(run_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'run_id' should be string value, got {type(run_id_unvalidated).__name__}"))
            if run_id_unvalidated != msg.correlation_id:
                return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'run_id' value {run_id_unvalidated}"))
            
            parsed_data = task_id_unvalidated, run_id_unvalidated, metadata_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(input_adapter: Callable[[TaskIdValue, RunIdValue, dict], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str, str, dict]) -> Result[R, RabbitMessageError]:
            task_id_unvalidated, run_id_unvalidated, metadata_unvalidated = parsed_data
            task_id_res = parse_value(task_id_unvalidated, "task_id", TaskIdValue.from_value_with_checksum)
            run_id_res = parse_value(run_id_unvalidated, "run_id", RunIdValue.from_value_with_checksum)
            match task_id_res, run_id_res:
                case Result(tag=ResultTag.OK, ok=task_id), Result(tag=ResultTag.OK, ok=run_id):
                    res = input_adapter(task_id, run_id, metadata_unvalidated)
                    return Result.Ok(res)
                case _:
                    errors_with_none = [task_id_res.swap().default_value(None), run_id_res.swap().default_value(None)]
                    errors = [err for err in errors_with_none if err is not None]
                    err = rabbit_msg_err(ValidationError, ", ".join(errors))
                    return Result.Error(err)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding {RUN_TASK_COMMAND}", msg.correlation_id)
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
                
        task_id_res = parse_value(decoded.get("task_id", None), "task_id", TaskIdValue.from_value_with_checksum)
        run_id_res = parse_value(decoded.get("run_id", None), "run_id", RunIdValue.from_value_with_checksum)
        task_id = task_id_res.default_value(TaskIdValue(None))
        run_id = run_id_res.default_value(RunIdValue(None))
        return logger_creator.create(task_id, run_id, StepIdValue(None))
    
@dataclass(frozen=True)
class RunTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: dict

def run(rabbit_client: RabbitMQClient, task_id: TaskIdValue, run_id: RunIdValue, from_: str, metadata: dict):
    command = RUN_TASK_COMMAND
    message = _python_pickle.data_to_message(task_id, run_id, from_, metadata)
    return rabbit_client.send_command(command, message)

async def _handler_error_result_to_negative_acknowledge_middleware(
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        res = await call_next(msg)
        match res:
            case Result(tag=ResultTag.ERROR, error=_):
                bits = secrets.randbits(1)
                requeue = bits == 1
                raise NackMessage(requeue=requeue)
        return res

async def _handler_logging_middleware(
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        PREFIX = RUN_TASK_COMMAND
        logger = _python_pickle.create_logger(msg)
        decoded_data = await msg.decode()
        logger.info(f"{PREFIX} RECEIVED {decoded_data}")
        res = await call_next(msg)
        match res:
            case Result(tag=ResultTag.OK, ok=result):
                first_100_chars = str(result)[:100]
                output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                logger.info(f"{PREFIX} successfully completed with output {output}")
            case Result(tag=ResultTag.ERROR, error=error):
                logger.error(f"{PREFIX} failed with error {error}")
            case None:
                logger.warning(f"{PREFIX} PROCESSING SKIPPED")
        return res

class handler:
    def __init__(self, rabbit_client: RabbitMQClient, input_adapter: Callable[[TaskIdValue, RunIdValue, dict], R]):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return input_adapter
        self._rabbit_client = rabbit_client
        self._input_adapter = validate_input_adapter()
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, Result | None]]):
        decoder = _python_pickle.decoder(self._input_adapter)
        middlewares = (_handler_error_result_to_negative_acknowledge_middleware, _handler_logging_middleware)
        return self._rabbit_client.command_handler(RUN_TASK_COMMAND, decoder, middlewares)(func)