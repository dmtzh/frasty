from collections.abc import Callable, Coroutine
from logging import LoggerAdapter
import pickle
from typing import Any

from expression import Result
from faststream.broker.message import StreamMessage
from faststream.rabbit import RabbitMessage

from shared.pipeline.actionhandler import ActionDataDto
from shared.customtypes import Error
from shared.utils.parse import parse_from_dict

from .client import RabbitMQClient
from .error import rabbit_message_error_creator, RabbitMessageErrorCreator, ParseError, ValidationError, RabbitMessageError
from .logging import RabbitMessageLoggerCreator
from .pythonpickle import DataWithCorrelationId, PythonPickleMessage
from .rabbitmiddlewares import error_result_to_negative_acknowledge_middleware, command_handler_logging_middleware, RequeueChance

class _python_pickle:
    @staticmethod
    def data_to_message(data: ActionDataDto) -> PythonPickleMessage:
        ids_dict = {"run_id": data.run_id, "step_id": data.step_id}
        config_dict = {"config": data.config} if data.config is not None else {}
        action_data = ids_dict | config_dict | {"data": data.data, "metadata": data.metadata}
        correlation_id = ids_dict["run_id"]
        data_with_correlation_id = DataWithCorrelationId(action_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
    class decoder():
        def __init__(self, action_name: str):
            self._action_name = action_name
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[ActionDataDto, RabbitMessageError]:
            correlation_id = msg.correlation_id
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
            
            config_unvalidated = decoded.get("config", None)
            if config_unvalidated is not None and not isinstance(config_unvalidated, dict):
                return Result.Error(rabbit_msg_err(ParseError, f"'config' should be {dict.__name__} value, got {type(config_unvalidated).__name__}"))
            
            if "data" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'data' key not found in {decoded}"))
            data_unvalidated = decoded["data"]
            if not isinstance(data_unvalidated, dict) and not isinstance(data_unvalidated, list):
                return Result.Error(rabbit_msg_err(ParseError, f"'data' should be {dict.__name__} or {list.__name__} value, got {type(data_unvalidated).__name__}"))

            if "metadata" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'metadata' key not found in {decoded}"))
            metadata_unvalidated = decoded["metadata"]
            if not isinstance(metadata_unvalidated, dict):
                return Result.Error(rabbit_msg_err(ParseError, f"'metadata' should be {dict.__name__} value, got {type(metadata_unvalidated).__name__}"))

            parsed_data = ActionDataDto(run_id_unvalidated, step_id_unvalidated, config_unvalidated, data_unvalidated, metadata_unvalidated)
            return Result.Ok(parsed_data)
        
        def __call__(self, message):
            msg: RabbitMessage = message
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding RUN_ACTION({self._action_name})", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            return parsed_data_res
        
    @staticmethod
    def parse_task_id_run_id_step_id(msg: StreamMessage[Any]) -> tuple[str, str, str]:
        if not isinstance(msg.body, bytes):
            return "N/A", "N/A", "N/A"
        try:
            decoded = pickle.loads(msg.body)
        except Exception:
            return "N/A", "N/A", "N/A"
        if not isinstance(decoded, dict):
            return "N/A", "N/A", "N/A"
        metadata_res = parse_from_dict(decoded, "metadata", lambda m: m if isinstance(m, dict) else None)
        task_id_res = metadata_res.map(lambda m: str(m.get("task_id", "N/A")))
        task_id = task_id_res.default_value("N/A")
        run_id = str(decoded.get("run_id", "N/A"))
        step_id = str(decoded.get("step_id", "N/A"))
        return task_id, run_id, step_id
    
    @staticmethod
    def create_logger(msg: StreamMessage[Any]) -> LoggerAdapter:
        logger_creator = RabbitMessageLoggerCreator(msg.raw_message)
        ids = _python_pickle.parse_task_id_run_id_step_id(msg)
        return logger_creator.create(*ids)

def run(rabbit_client: RabbitMQClient, action_name: str, data: ActionDataDto):
    command = action_name
    message = _python_pickle.data_to_message(data)
    return rabbit_client.send_command(command, message)

class handler:
    def __init__(self, rabbit_client: RabbitMQClient, action_name: str):
        self._rabbit_client = rabbit_client
        self._action_name = action_name
    
    def __call__(self, func: Callable[[Result[ActionDataDto, Any]], Coroutine]):
        decoder = _python_pickle.decoder(self._action_name)
        middlewares = (
            error_result_to_negative_acknowledge_middleware(RequeueChance.FIFTY_FIFTY),
            command_handler_logging_middleware(self._action_name, _python_pickle.create_logger)
        )
        return self._rabbit_client.command_handler(self._action_name, decoder, middlewares)(func)