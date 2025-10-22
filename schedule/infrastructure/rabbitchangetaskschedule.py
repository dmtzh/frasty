from collections.abc import Callable, Coroutine
import functools
from logging import LoggerAdapter
import pickle
from typing import Any, ParamSpec, TypeVar

from expression import Result
from faststream.rabbit import RabbitMessage

from infrastructure.rabbitmiddlewares import error_result_to_negative_acknowledge_middleware, RequeueChance
from shared.commands import Command, CommandAdapter
from shared.customtypes import ScheduleIdValue, TaskIdValue, Error
from shared.infrastructure.rabbitmq.client import RabbitMQClient
from shared.infrastructure.rabbitmq.error import ParseError, RabbitMessageError, RabbitMessageErrorCreator, ValidationError, rabbit_message_error_creator
from shared.infrastructure.rabbitmq.pythonpickle import DataWithCorrelationId, PythonPickleMessage
from shared.utils.parse import parse_value
from shared.utils.result import ResultTag

R = TypeVar("R")
P = ParamSpec("P")
CHANGE_TASK_SCHEDULE_COMMAND = "change_task_schedule"

class _python_pickle:
    @staticmethod
    def data_to_message(task_id: TaskIdValue, schedule_id: ScheduleIdValue, command: Command) -> PythonPickleMessage:
        task_id_dict = {"task_id": task_id.to_value_with_checksum()}
        schedule_id_dict = {"schedule_id": schedule_id.to_value_with_checksum()}
        command_dto = CommandAdapter.to_dict(command)
        command_data = task_id_dict | schedule_id_dict | {"command": command_dto}
        correlation_id = schedule_id_dict["schedule_id"]
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
    class decoder():
        def __init__(self, input_adapter: Callable[[TaskIdValue, ScheduleIdValue, Command], R]):
            self._input_adapter = input_adapter
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[tuple[str, str, dict], RabbitMessageError]:
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
            
            parsed_data = task_id_unvalidated, schedule_id_unvalidated, command_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(input_adapter: Callable[[TaskIdValue, ScheduleIdValue, Command], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str, str, dict]) -> Result[R, RabbitMessageError]:
            task_id_unvalidated, schedule_id_unvalidated, command_unvalidated = parsed_data
            task_id_res = parse_value(task_id_unvalidated, "task_id", TaskIdValue.from_value_with_checksum)
            schedule_id_res = parse_value(schedule_id_unvalidated, "schedule_id", ScheduleIdValue.from_value_with_checksum)
            command_res = CommandAdapter.from_dict(command_unvalidated).map_error(lambda _: f"Invalid 'command' value {command_unvalidated}")
            match task_id_res, schedule_id_res, command_res:
                case Result(tag=ResultTag.OK, ok=task_id), Result(tag=ResultTag.OK, ok=schedule_id), Result(tag=ResultTag.OK, ok=command):
                    res = input_adapter(task_id, schedule_id, command)
                    return Result.Ok(res)
                case _:
                    errors_with_none = [task_id_res.swap().default_value(None), schedule_id_res.swap().default_value(None), command_res.swap().default_value(None)]
                    errors = [err for err in errors_with_none if err is not None]
                    err = rabbit_msg_err(ValidationError, ", ".join(errors))
                    return Result.Error(err)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding {CHANGE_TASK_SCHEDULE_COMMAND}", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

def run(rabbit_client: RabbitMQClient, task_id: TaskIdValue, schedule_id: ScheduleIdValue, command: Command):
    message = _python_pickle.data_to_message(task_id, schedule_id, command)
    return rabbit_client.send_command(CHANGE_TASK_SCHEDULE_COMMAND, message)

class handler:
    def __init__(self, rabbit_client: RabbitMQClient, input_adapter: Callable[[TaskIdValue, ScheduleIdValue, Command], R]):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return input_adapter
        self._rabbit_client = rabbit_client
        self._input_adapter = validate_input_adapter()
    
    def _consume_input(self, task_id: TaskIdValue, schedule_id: ScheduleIdValue, cmd: Command, logger: LoggerAdapter):
        self._logger = logger
        return self._input_adapter(task_id, schedule_id, cmd)
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, Result | None]]):
        decoder = _python_pickle.decoder(self._input_adapter)
        middlewares = (
            error_result_to_negative_acknowledge_middleware(RequeueChance.FIFTY_FIFTY),
        )
        return self._rabbit_client.command_handler(CHANGE_TASK_SCHEDULE_COMMAND, decoder, middlewares)(func)