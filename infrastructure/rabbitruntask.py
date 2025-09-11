from collections.abc import Callable, Coroutine
import functools
from logging import LoggerAdapter
import pickle
import secrets
from typing import Any, ParamSpec, TypeVar

from expression import Result
from faststream.exceptions import NackMessage
from faststream.rabbit import RabbitMessage

from shared.customtypes import Error, RunIdValue, TaskIdValue
from shared.infrastructure.rabbitmq.client import RabbitMQClient
from shared.infrastructure.rabbitmq.error import rabbit_message_error_creator, RabbitMessageErrorCreator, ParseError, ValidationError, RabbitMessageError
from shared.infrastructure.rabbitmq.logging import RabbitMessageLoggerCreator
from shared.infrastructure.rabbitmq.pythonpickle import DataWithCorrelationId, PythonPickleMessage
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
            {"task_id": task_id_with_checksum, "run_id": run_id_with_checksum, "from": from_}
        command_data = {"metadata": metadata_dict}
        correlation_id = run_id_with_checksum
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
    class decoder():
        def __init__(self, input_adapter: Callable[[TaskIdValue, RunIdValue, dict, LoggerAdapter], R]):
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
            
            if "task_id" not in metadata_unvalidated:
                return Result.Error(rabbit_msg_err(ParseError, f"'task_id' key not found in {metadata_unvalidated}"))
            task_id_unvalidated = metadata_unvalidated["task_id"]
            if not isinstance(task_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'task_id' should be string value, got {type(task_id_unvalidated).__name__}"))

            if "run_id" not in metadata_unvalidated:
                return Result.Error(rabbit_msg_err(ParseError, f"'run_id' key not found in {metadata_unvalidated}"))
            run_id_unvalidated = metadata_unvalidated["run_id"]
            if not isinstance(run_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'run_id' should be string value, got {type(run_id_unvalidated).__name__}"))
            if run_id_unvalidated != msg.correlation_id:
                return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'run_id' value {run_id_unvalidated}"))
            
            parsed_data = task_id_unvalidated, run_id_unvalidated, metadata_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(logger_creator: RabbitMessageLoggerCreator, input_adapter: Callable[[TaskIdValue, RunIdValue, dict, LoggerAdapter], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str, str, dict]) -> Result[R, RabbitMessageError]:
            task_id_unvalidated, run_id_unvalidated, metadata_unvalidated = parsed_data
            def validate_id[T](id_parser: Callable[[str], T | None], id_unvalidated: str, id_name: str) -> Result[T, RabbitMessageError]:
                opt_id = id_parser(id_unvalidated)
                match opt_id:
                    case None:
                        return Result.Error(rabbit_msg_err(ValidationError, f"Invalid '{id_name}' value {id_unvalidated}"))
                    case valid_id:
                        return Result.Ok(valid_id)
            
            task_id_res = validate_id(TaskIdValue.from_value_with_checksum, task_id_unvalidated, "task_id")
            run_id_res = validate_id(RunIdValue.from_value_with_checksum, run_id_unvalidated, "run_id")
            match task_id_res, run_id_res:
                case Result(tag=ResultTag.OK, ok=task_id), Result(tag=ResultTag.OK, ok=run_id):
                    logger = logger_creator.create(task_id, run_id, None)
                    res = input_adapter(task_id, run_id, metadata_unvalidated, logger)
                    return Result.Ok(res)
                case _:
                    error = task_id_res.swap().default_value(None) or run_id_res.error
                    return Result.Error(error)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            logger_creator = RabbitMessageLoggerCreator(msg.raw_message)
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding {RUN_TASK_COMMAND}", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, logger_creator, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

def run(rabbit_client: RabbitMQClient, task_id: TaskIdValue, run_id: RunIdValue, from_: str, metadata: dict):
    command = RUN_TASK_COMMAND
    message = _python_pickle.data_to_message(task_id, run_id, from_, metadata)
    return rabbit_client.send_command(command, message)

class handler:
    def __init__(self, rabbit_client: RabbitMQClient, input_adapter: Callable[[TaskIdValue, RunIdValue, dict], R]):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return input_adapter
        self._rabbit_client = rabbit_client
        self._input_adapter = validate_input_adapter()
    
    def _consume_input(self, task_id: TaskIdValue, run_id: RunIdValue, metadata: dict, logger: LoggerAdapter):
        logger.info(f"{RUN_TASK_COMMAND} RECEIVED metadata {metadata}")
        self._task_id = task_id
        self._run_id = run_id
        self._metadata = metadata
        self._logger = logger
        return self._input_adapter(task_id, run_id, metadata)
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, Result | None]]):
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs):
            run_task_res = await func(*args, **kwargs)
            match run_task_res:
                case Result(tag=ResultTag.OK, ok=result):
                    first_100_chars = str(result)[:100]
                    output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                    self._logger.info(f"{RUN_TASK_COMMAND} successfully completed with output {output}")
                case Result(tag=ResultTag.ERROR, error=error):
                    bits = secrets.randbits(1)
                    requeue = bits == 1
                    self._logger.error(f"{RUN_TASK_COMMAND} failed with error {error}")
                    self._logger.error(f"requeue={requeue}")
                    raise NackMessage(requeue=requeue)
            return run_task_res
        
        decoder = _python_pickle.decoder(self._consume_input)
        return self._rabbit_client.command_handler(RUN_TASK_COMMAND, decoder)(wrapper)