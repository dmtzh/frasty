from collections.abc import Awaitable, Callable, Coroutine
import functools
import inspect
from logging import LoggerAdapter
import pickle
from typing import Any, ParamSpec, TypeVar

from expression import Result
from faststream.broker.message import StreamMessage
from faststream.rabbit import RabbitMessage

from shared.customtypes import DefinitionIdValue, Error, RunIdValue, TaskIdValue, StepIdValue
from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag

from .client import ExistingQueueName, NotExistingQueueName, QueueName, RabbitMQClient
from .error import ParseError, RabbitMessageError, RabbitMessageErrorCreator, ValidationError, rabbit_message_error_creator
from .logging import RabbitMessageLoggerCreator
from .rabbitmiddlewares import error_result_to_negative_acknowledge_middleware, RequeueChance

DEFINITION_COMPLETED_EVENT = "definition_completed"
DEFINITION_COMPLETED_EVENT_GROUP = "completed_definitions"

R = TypeVar("R")
P = ParamSpec("P")
T = TypeVar("T")

class _python_pickle:
    class decoder():
        def __init__(self, queue_name: QueueName, input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], R]):
            self._queue_name = queue_name
            self._input_adapter = input_adapter
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[tuple[str, str, dict, dict], RabbitMessageError]:
            opt_correlation_id = RunIdValue.from_value_with_checksum(msg.correlation_id)
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
            
            if "definition_id" not in decoded:
                return Result.Error(rabbit_msg_err(ParseError, f"'definition_id' key not found in {decoded}"))
            definition_id_unvalidated = decoded["definition_id"]
            if not isinstance(definition_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'definition_id' should be string value, got {type(definition_id_unvalidated).__name__}"))
            
            parsed_data = run_id_unvalidated, definition_id_unvalidated, result_unvalidated, metadata_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str, str, dict, dict]) -> Result[R, RabbitMessageError]:
            def validate_id[T](id_parser: Callable[[str], T | None], id_unvalidated: str, id_name: str) -> Result[T, str]:
                opt_id = id_parser(id_unvalidated)
                match opt_id:
                    case None:
                        return Result.Error(f"Invalid '{id_name}' value {id_unvalidated}")
                    case valid_id:
                        return Result.Ok(valid_id)
            
            run_id_unvalidated, definition_id_unvalidated, result_unvalidated, metadata_unvalidated = parsed_data
            run_id_res = validate_id(RunIdValue.from_value_with_checksum, run_id_unvalidated, "run_id")
            definition_id_res = validate_id(DefinitionIdValue.from_value_with_checksum, definition_id_unvalidated, "definition_id")
            result_res = CompletedResultAdapter.from_dict(result_unvalidated).map_error(lambda _: f"Invalid 'result' value {result_unvalidated}")
            match run_id_res, definition_id_res, result_res:
                case Result(tag=ResultTag.OK, ok=run_id), Result(tag=ResultTag.OK, ok=definition_id), Result(tag=ResultTag.OK, ok=result):
                    res = input_adapter(run_id, definition_id, result, metadata_unvalidated)
                    return Result.Ok(res)
                case _:
                    errors_with_none = [run_id_res.swap().default_value(None), definition_id_res.swap().default_value(None), result_res.swap().default_value(None)]
                    errors = [err for err in errors_with_none if err is not None]
                    err = rabbit_msg_err(ValidationError, ", ".join(errors))
                    return Result.Error(err)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding {DEFINITION_COMPLETED_EVENT}", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

        def create_logger(self, msg: StreamMessage[Any]) -> LoggerAdapter:            
            if not isinstance(msg.body, bytes):
                logger_creator = RabbitMessageLoggerCreator(msg.raw_message, self._queue_name, DefinitionIdValue(None))
                return logger_creator.create(TaskIdValue(None), RunIdValue(None), StepIdValue(None))
            try:
                decoded = pickle.loads(msg.body)
            except Exception:
                logger_creator = RabbitMessageLoggerCreator(msg.raw_message, self._queue_name, DefinitionIdValue(None))
                return logger_creator.create(TaskIdValue(None), RunIdValue(None), StepIdValue(None))
            if not isinstance(decoded, dict):
                logger_creator = RabbitMessageLoggerCreator(msg.raw_message, self._queue_name, DefinitionIdValue(None))
                return logger_creator.create(TaskIdValue(None), RunIdValue(None), StepIdValue(None))
            
            definition_id_res = parse_from_dict(decoded, "definition_id", DefinitionIdValue.from_value_with_checksum)
            definition_id = definition_id_res.default_value(DefinitionIdValue(None))
            logger_creator = RabbitMessageLoggerCreator(msg.raw_message, self._queue_name, definition_id)
            metadata_res = parse_from_dict(decoded, "metadata", lambda m: m if isinstance(m, dict) else None)
            task_id_res = metadata_res.bind(lambda m: parse_from_dict(m, "task_id", TaskIdValue.from_value_with_checksum))
            run_id_res = parse_from_dict(decoded, "run_id", RunIdValue.from_value_with_checksum)
            task_id = task_id_res.default_value(TaskIdValue(None))
            run_id = run_id_res.default_value(RunIdValue(None))
            return logger_creator.create(task_id, run_id, StepIdValue(None))

class _logging_middleware:
    def __init__(
        self,
        logger_creator: Callable[[StreamMessage[Any]], LoggerAdapter]
    ):
        self._logger_creator = logger_creator
    
    async def __call__(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        logger = self._logger_creator(msg)
        decoded_data = await msg.decode()
        match decoded_data:
            case Result(tag=ResultTag.OK, ok=data):
                logger.info(f"COMPLETED with {data}")
            case Result(tag=ResultTag.ERROR, error=error):
                logger.error(f"RECEIVED {error}")
            case unsupported_decoded_data:
                logger.warning(f"RECEIVED UNSUPPORTED {unsupported_decoded_data}")
        res = await call_next(msg)
        match res:
            case Result(tag=ResultTag.OK, ok=output):
                logger.info(f"data processed with output {output}")
            case Result(tag=ResultTag.ERROR, error=error):
                logger.error(f"data failed to process with error {error}")
            case None:
                logger.warning("PROCESSING SKIPPED")
        return res

class subscriber:
    @staticmethod
    def _consume_input(input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult], R] | Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], R], run_id: RunIdValue, definition_id: DefinitionIdValue, result: CompletedResult, metadata: dict):
        params = inspect.signature(input_adapter).parameters
        include_metadata_param = len(params) > 3
        if include_metadata_param:
            params_array = [run_id, definition_id, result, metadata]
            return input_adapter(*params_array)
        else:
            params_array = [run_id, definition_id, result]
            return input_adapter(*params_array)
    
    def __init__(self, rabbit_client: RabbitMQClient, input_adapter: Callable[[RunIdValue, DefinitionIdValue, CompletedResult], R] | Callable[[RunIdValue, DefinitionIdValue, CompletedResult, dict], R], queue_name: str | None, requeue_chance: RequeueChance):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return functools.partial(self._consume_input, input_adapter)
        self._rabbit_client = rabbit_client
        self._input_adapter = validate_input_adapter()
        self._queue_name = ExistingQueueName(queue_name) if queue_name is not None else NotExistingQueueName.new_name()
        self._requeue_chance = requeue_chance
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, Any]]):
        decoder = _python_pickle.decoder(self._queue_name, self._input_adapter)
        middlewares = (
            error_result_to_negative_acknowledge_middleware(self._requeue_chance),
            _logging_middleware(decoder.create_logger)
        )
        return self._rabbit_client.event_handler(DEFINITION_COMPLETED_EVENT, DEFINITION_COMPLETED_EVENT_GROUP, self._queue_name, decoder, middlewares)(func)
