from collections.abc import Callable, Coroutine
import functools
import inspect
from logging import LoggerAdapter
import pickle
import secrets
from typing import Any, ParamSpec, TypeVar

from expression import Result
from faststream.exceptions import NackMessage
from faststream.rabbit import RabbitMessage

from infrastructure import rabbitcompletestep as rabbit_complete_step
from shared.customtypes import Error, IdValue
import shared.domaindefinition as shdomaindef
import shared.dtodefinition as shdtodef
from shared.completedresult import CompletedWith, CompletedResult
from shared.infrastructure.rabbitmq.client import RabbitMQClient, Error as RabbitClientError
from shared.infrastructure.rabbitmq.error import rabbit_message_error_creator, RabbitMessageErrorCreator, ParseError, ValidationError, RabbitMessageError
from shared.infrastructure.rabbitmq.logging import RabbitMessageLoggerCreator
from shared.infrastructure.rabbitmq.pythonpickle import DataWithCorrelationId, PythonPickleMessage
from shared.infrastructure.stepdefinitioncreatorsstore import get_step_definition_name
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid

D = TypeVar("D")
TCfg = TypeVar("TCfg")
R = TypeVar("R")
P = ParamSpec("P")

class _python_pickle:
    @staticmethod
    def data_to_message(opt_task_id: IdValue | None, run_id: IdValue, definition_id: IdValue, step_id: IdValue, definition: shdomaindef.StepDefinition, data: Any, metadata: dict) -> PythonPickleMessage:
        step_definition_dict = shdtodef.StepDefinitionAdapter.to_dict(definition)
        is_metadata_valid = isinstance(metadata, dict)
        if not is_metadata_valid:
            raise ValueError(f"Invalid 'metadata' value {metadata}")
        task_id_dict = {"task_id": opt_task_id.to_value_with_checksum()} if opt_task_id is not None else {}
        run_id_with_checksum = run_id.to_value_with_checksum()
        metadata_dict = metadata | task_id_dict |\
            {"run_id": run_id_with_checksum, "definition_id": definition_id.to_value_with_checksum(), "step_id": step_id.to_value_with_checksum()}
        command_data = {"config": step_definition_dict, "definition": step_definition_dict, "data": data, "metadata": metadata_dict}
        correlation_id = run_id_with_checksum
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
    class decoder():
        def __init__(self, command: str, data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[IdValue | None, IdValue, IdValue, IdValue, TCfg, D, dict, LoggerAdapter], R]):
            self._command = command
            self._data_validator = data_validator
            self._input_adapter = input_adapter
        
        @staticmethod
        def _parse_rabbitmq_msg_python_pickle(rabbit_msg_err: RabbitMessageErrorCreator, msg: RabbitMessage) -> Result[tuple[str | None, str, str, str, dict, Any, dict], RabbitMessageError]:
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
            
            if "step_id" not in metadata_unvalidated:
                return Result.Error(rabbit_msg_err(ParseError, f"'step_id' key not found in {metadata_unvalidated}"))
            step_id_unvalidated = metadata_unvalidated["step_id"]
            if not isinstance(step_id_unvalidated, str):
                return Result.Error(rabbit_msg_err(ParseError, f"'step_id' should be string value, got {type(step_id_unvalidated).__name__}"))

            parsed_data = opt_task_id_unvalidated, run_id_unvalidated, definition_id_unvalidated, step_id_unvalidated, step_definition_unvalidated, data_unvalidated, metadata_unvalidated
            return Result.Ok(parsed_data)
        
        @staticmethod
        def _validate_rabbitmq_parsed_data(logger_creator: RabbitMessageLoggerCreator, command: str, data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[IdValue | None, IdValue, IdValue, IdValue, TCfg, D, dict, LoggerAdapter], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str | None, str, str, str, dict, Any, dict]):
            Res = Result[R, RabbitMessageError]
            opt_task_id_unvalidated, run_id_unvalidated, definition_id_unvalidated, step_id_unvalidated, step_definition_unvalidated, data_unvalidated, metadata_unvalidated = parsed_data
            def validate_task_id() -> Result[IdValue | None, RabbitMessageError]:
                match opt_task_id_unvalidated:
                    case None:
                        return Result.Ok(None)
                    case task_id_unvalidated:
                        opt_task_id  = IdValue.from_value_with_checksum(task_id_unvalidated)
                        match opt_task_id:
                            case None:
                                return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'task_id' value {task_id_unvalidated}"))
                            case task_id:
                                return Result.Ok(task_id)
            def validate_run_id() -> Result[IdValue, RabbitMessageError]:
                opt_run_id = IdValue.from_value_with_checksum(run_id_unvalidated)
                match opt_run_id:
                    case None:
                        return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'run_id' value {run_id_unvalidated}"))
                    case run_id:
                        return Result.Ok(run_id)
            def validate_definition_id() -> Result[IdValue, RabbitMessageError]:
                opt_definition_id = IdValue.from_value_with_checksum(definition_id_unvalidated)
                match opt_definition_id:
                    case None:
                        return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'definition_id' value {definition_id_unvalidated}"))
                    case definition_id:
                        return Result.Ok(definition_id)
            def validate_step_id() -> Result[IdValue, RabbitMessageError]:
                opt_step_id = IdValue.from_value_with_checksum(step_id_unvalidated)
                match opt_step_id:
                    case None:
                        return Result.Error(rabbit_msg_err(ValidationError, f"Invalid 'step_id' value {step_id_unvalidated}"))
                    case step_id:
                        return Result.Ok(step_id)
            
            opt_task_id_res = validate_task_id()
            run_id_res = validate_run_id()
            definition_id_res = validate_definition_id()
            step_id_res = validate_step_id()
            cfg_res = shdtodef.StepDefinitionAdapter.from_dict(step_definition_unvalidated)\
                .filter(lambda step_def: get_step_definition_name(type(step_def)) == command, [ValueInvalid("step")])\
                .map(lambda definition: definition.config)\
                .map_error(lambda _: rabbit_msg_err(ValidationError, f"Invalid 'definition' data: {step_definition_unvalidated}"))
            data_res = data_validator(data_unvalidated)\
                .map_error(lambda _: rabbit_msg_err(ValidationError, f"Invalid 'data' value {data_unvalidated}"))
            match opt_task_id_res, run_id_res, definition_id_res, step_id_res, cfg_res, data_res:
                case Result(tag=ResultTag.OK, ok=opt_task_id), Result(tag=ResultTag.OK, ok=run_id), Result(tag=ResultTag.OK, ok=definition_id), Result(tag=ResultTag.OK, ok=step_id), Result(tag=ResultTag.OK, ok=cfg), Result(tag=ResultTag.OK, ok=data):
                    logger = logger_creator.create(opt_task_id, run_id, step_id)
                    res = input_adapter(opt_task_id, run_id, definition_id, step_id, cfg, data, metadata_unvalidated, logger)
                    return Res.Ok(res)
                case _:
                    error = opt_task_id_res.swap().default_value(None) or run_id_res.swap().default_value(None) or definition_id_res.swap().default_value(None) or step_id_res.swap().default_value(None) or cfg_res.swap().default_value(None) or data_res.error
                    return Res.Error(error)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            logger_creator = RabbitMessageLoggerCreator(msg.raw_message)
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding RUN_STEP_COMMAND({self._command})", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, logger_creator, self._command, self._data_validator, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

def run(rabbit_client: RabbitMQClient, opt_task_id: IdValue | None, run_id: IdValue, definition_id: IdValue, step_id: IdValue, definition: shdomaindef.StepDefinition, data: Any, metadata: dict):
    command = get_step_definition_name(type(definition))
    message = _python_pickle.data_to_message(opt_task_id, run_id, definition_id, step_id, definition, data, metadata)
    return rabbit_client.send_command(command, message)

class handler:
    def __init__(self, rabbit_client: RabbitMQClient, step_definition_type: type[shdomaindef.StepDefinition[TCfg]], data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[IdValue | None, IdValue, IdValue, IdValue, TCfg, D], R] | Callable[[IdValue | None, IdValue, IdValue, IdValue, TCfg, D, dict], R]):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return input_adapter
        self._rabbit_client = rabbit_client
        self._step_name = get_step_definition_name(step_definition_type)
        self._data_validator = data_validator
        self._input_adapter = validate_input_adapter()
    
    def _consume_input(self, opt_task_id: IdValue | None, run_id: IdValue, definition_id: IdValue, step_id: IdValue, cfg: TCfg, data: D, metadata: dict, logger: LoggerAdapter):
        logger.info(f"{self._step_name} RECEIVED metadata {metadata}")
        self._opt_task_id = opt_task_id
        self._run_id = run_id
        self._definition_id = definition_id
        self._step_id = step_id
        params = inspect.signature(self._input_adapter).parameters
        include_metadata_param = len(params) > 6
        metadata_keys_to_exclude = ["task_id", "run_id", "definition_id", "step_id"]
        metadata_dict = {k: v for k, v in metadata.items() if k not in metadata_keys_to_exclude}
        self._metadata = metadata_dict
        self._logger = logger
        if include_metadata_param:
            params_array = [opt_task_id, run_id, definition_id, step_id, cfg, data, metadata_dict]
            return self._input_adapter(*params_array)
        else:
            params_array = [opt_task_id, run_id, definition_id, step_id, cfg, data]
            return self._input_adapter(*params_array)
    
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def _send_response(self, result: CompletedResult):
        return rabbit_complete_step.run(self._rabbit_client, self._run_id, self._definition_id, self._step_id, result, self._metadata)
    
    def __call__(self, func: Callable[P, Coroutine[Any, Any, CompletedResult | None]]):
        def result_to_log_output(result: CompletedResult):
            match result:
                case CompletedWith.Data(data=data):
                    first_100_chars = str(data)[:100]
                    return first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                case no_data_or_err:
                    return str(no_data_or_err)
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs):
            res = await func(*args, **kwargs)
            # should_send_reponse = isinstance(res, CompletedResult.__value__)
            if type(res) is not CompletedWith.Data and type(res) is not CompletedWith.NoData and type(res) is not CompletedWith.Error:
                return res
            send_response_res = await self._send_response(res)
            match send_response_res:
                case Result(tag=ResultTag.OK):
                    output = result_to_log_output(res)
                    self._logger.info(f"{self._step_name} successfully completed with output {output}")
                case Result(tag=ResultTag.ERROR, error=error):
                    bits = secrets.randbits(1)
                    requeue = bits == 1
                    self._logger.error(f"{self._step_name} failed with error {error}")
                    self._logger.error(f"requeue={requeue}")
                    raise NackMessage(requeue=requeue)
            return res
        decoder = _python_pickle.decoder(self._step_name, self._data_validator, self._consume_input)
        return self._rabbit_client.command_handler(self._step_name, decoder)(wrapper)
