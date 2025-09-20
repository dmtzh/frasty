from collections.abc import Callable, Coroutine, Generator
from dataclasses import dataclass
from enum import StrEnum
import functools
from logging import LoggerAdapter
import pickle
import secrets
from typing import Any, Optional, ParamSpec, TypeVar

from expression import Result, effect
from faststream.exceptions import NackMessage
from faststream.rabbit import RabbitMessage

from shared.customtypes import ScheduleIdValue, TaskIdValue, Error, RunIdValue, StepIdValue
from shared.infrastructure.rabbitmq.client import RabbitMQClient
from shared.infrastructure.rabbitmq.error import ParseError, RabbitMessageError, RabbitMessageErrorCreator, ValidationError, rabbit_message_error_creator
from shared.infrastructure.rabbitmq.logging import RabbitMessageLoggerCreator
from shared.infrastructure.rabbitmq.pythonpickle import DataWithCorrelationId, PythonPickleMessage
from shared.utils.parse import parse_from_str, parse_from_dict
from shared.utils.result import ResultTag
from shared.utils.string import strip_and_lowercase

R = TypeVar("R")
P = ParamSpec("P")
CHANGE_TASK_SCHEDULE_COMMAND = "change_task_schedule"

@dataclass(frozen=True)
class ClearCommand:
    '''Clear task schedule command'''

type Command = ClearCommand

class CommandDtoTypes(StrEnum):
    CLEAR = ClearCommand.__name__.lower()
    @staticmethod
    def parse(command_type: str) -> Optional["CommandDtoTypes"]:
        if command_type is None:
            return None
        match strip_and_lowercase(command_type):
            case CommandDtoTypes.CLEAR:
                return CommandDtoTypes.CLEAR
            case _:
                return None

class CommandAdapter:
    @staticmethod
    def to_dict(command: Command) -> dict:
        match command:
            case ClearCommand():
                return {"type": CommandDtoTypes.CLEAR}
    
    @effect.result[Command, str]()
    @staticmethod
    def from_dict(command_dto: dict) -> Generator[Any, Any, Command]:
        command_type = yield from parse_from_dict(command_dto, "type", CommandDtoTypes.parse)
        match command_type:
            case CommandDtoTypes.CLEAR:
                return ClearCommand()
            case _:
                yield from Result.Error(f"command type {command_type} is invalid")
                raise RuntimeError("command type is invalid")

class _python_pickle:
    @staticmethod
    def data_to_message(task_id: TaskIdValue, schedule_id: ScheduleIdValue, command: Command, metadata: dict) -> PythonPickleMessage:
        is_metadata_valid = isinstance(metadata, dict)
        if not is_metadata_valid:
            raise ValueError(f"Invalid 'metadata' value {metadata}")
        task_id_dict = {"task_id": task_id.to_value_with_checksum()}
        schedule_id_dict = {"schedule_id": schedule_id.to_value_with_checksum()}
        command_dto = CommandAdapter.to_dict(command)
        command_data = task_id_dict | schedule_id_dict | {"command": command_dto, "metadata": metadata}
        correlation_id = schedule_id_dict["schedule_id"]
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
    class decoder():
        def __init__(self, input_adapter: Callable[[TaskIdValue, ScheduleIdValue, Command, dict, LoggerAdapter], R]):
            self._input_adapter = input_adapter
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[tuple[str, str, dict, dict], RabbitMessageError]:
            opt_correlation_id = ScheduleIdValue.from_value_with_checksum(msg.correlation_id)
            if opt_correlation_id is None:
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
            
            if "command" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'command' key not found in {decoded}"))
            command_unvalidated = decoded["command"]
            if not isinstance(command_unvalidated, dict):
                return Result.Error(rabbit_msg_err(ParseError, f"'command' should be {dict.__name__} value, got {type(command_unvalidated).__name__}"))
            
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
                        
            if "schedule_id" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'schedule_id' key not found in {decoded}"))
            schedule_id_unvalidated = decoded["schedule_id"]
            if not isinstance(schedule_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'schedule_id' should be string value, got {type(schedule_id_unvalidated).__name__}"))
            if schedule_id_unvalidated != msg.correlation_id:
                return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'schedule_id' value {schedule_id_unvalidated}"))
            
            parsed_data = task_id_unvalidated, schedule_id_unvalidated, command_unvalidated, metadata_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(logger_creator: RabbitMessageLoggerCreator, input_adapter: Callable[[TaskIdValue, ScheduleIdValue, Command, dict, LoggerAdapter], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str, str, dict, dict]) -> Result[R, RabbitMessageError]:
            def validate_id[T](id_parser: Callable[[str], T | None], id_unvalidated: str, id_name: str) -> Result[T, str]:
                opt_id = id_parser(id_unvalidated)
                match opt_id:
                    case None:
                        return Result.Error(f"Invalid '{id_name}' value {id_unvalidated}")
                    case valid_id:
                        return Result.Ok(valid_id)
            
            task_id_unvalidated, schedule_id_unvalidated, command_unvalidated, metadata_unvalidated = parsed_data
            task_id_res = parse_from_str(task_id_unvalidated, "task_id", TaskIdValue.from_value_with_checksum)
            schedule_id_res = parse_from_str(schedule_id_unvalidated, "schedule_id", ScheduleIdValue.from_value_with_checksum)
            command_res = CommandAdapter.from_dict(command_unvalidated).map_error(lambda _: f"Invalid 'command' value {command_unvalidated}")
            match task_id_res, schedule_id_res, command_res:
                case Result(tag=ResultTag.OK, ok=task_id), Result(tag=ResultTag.OK, ok=schedule_id), Result(tag=ResultTag.OK, ok=command):
                    logger = logger_creator.create(task_id, RunIdValue("None"), StepIdValue("None"))
                    res = input_adapter(task_id, schedule_id, command, metadata_unvalidated, logger)
                    return Result.Ok(res)
                case _:
                    errors_with_none = [task_id_res.swap().default_value(None), schedule_id_res.swap().default_value(None), command_res.swap().default_value(None)]
                    errors = [err for err in errors_with_none if err is not None]
                    err = rabbit_msg_err(ValidationError, ", ".join(errors))
                    return Result.Error(err)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            logger_creator = RabbitMessageLoggerCreator(msg.raw_message)
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding {CHANGE_TASK_SCHEDULE_COMMAND}", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, logger_creator, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

@dataclass(frozen=True)
class ChangeTaskScheduleData:
    task_id: TaskIdValue
    schedule_id: ScheduleIdValue
    command: Command
    metadata: dict

def run(rabbit_client: RabbitMQClient, task_id: TaskIdValue, schedule_id: ScheduleIdValue, command: Command, metadata: dict):
    message = _python_pickle.data_to_message(task_id, schedule_id, command, metadata)
    return rabbit_client.send_command(CHANGE_TASK_SCHEDULE_COMMAND, message)

class handler:
    def __init__(self, rabbit_client: RabbitMQClient, input_adapter: Callable[[TaskIdValue, ScheduleIdValue, Command, dict], R]):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return input_adapter
        self._rabbit_client = rabbit_client
        self._input_adapter = validate_input_adapter()
    
    def _consume_input(self, task_id: TaskIdValue, schedule_id: ScheduleIdValue, cmd: Command, metadata: dict, logger: LoggerAdapter):
        self._logger = logger
        return self._input_adapter(task_id, schedule_id, cmd, metadata)
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, Result | None]]):
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs):
            match kwargs.get("input"):
                case Result(tag=ResultTag.OK, ok=input):
                    self._logger.info(f"{CHANGE_TASK_SCHEDULE_COMMAND} RECEIVED {input}")
            change_task_schedule_res = await func(*args, **kwargs)
            match change_task_schedule_res:
                case Result(tag=ResultTag.OK, ok=result):
                    first_100_chars = str(result)[:100]
                    output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                    self._logger.info(f"{CHANGE_TASK_SCHEDULE_COMMAND} successfully completed with output {output}")
                case Result(tag=ResultTag.ERROR, error=error):
                    bits = secrets.randbits(1)
                    requeue = bits == 1
                    self._logger.error(f"{CHANGE_TASK_SCHEDULE_COMMAND} failed with error {error}")
                    self._logger.error(f"requeue={requeue}")
                    raise NackMessage(requeue=requeue)
                case None:
                    self._logger.warning(f"{CHANGE_TASK_SCHEDULE_COMMAND} PROCESSING SKIPPED")
            return change_task_schedule_res
        
        decoder = _python_pickle.decoder(self._consume_input)
        return self._rabbit_client.command_handler(CHANGE_TASK_SCHEDULE_COMMAND, decoder)(wrapper)