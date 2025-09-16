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
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid

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
        def __init__(self, command: str, data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D, dict, LoggerAdapter], R]):
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
        def _validate_rabbitmq_parsed_data(logger_creator: RabbitMessageLoggerCreator, command: str, data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D, dict, LoggerAdapter], R], rabbit_msg_err: RabbitMessageErrorCreator, parsed_data: tuple[str, str, dict, Any, dict]) -> Result[R, RabbitMessageError]:
            def validate_id[T](id_parser: Callable[[str], T | None], id_unvalidated: str, id_name: str) -> Result[T, str]:
                opt_id = id_parser(id_unvalidated)
                match opt_id:
                    case None:
                        return Result.Error(f"Invalid '{id_name}' value {id_unvalidated}")
                    case valid_id:
                        return Result.Ok(valid_id)
            
            run_id_unvalidated, step_id_unvalidated, step_definition_unvalidated, data_unvalidated, metadata_unvalidated = parsed_data
            run_id_res = validate_id(RunIdValue.from_value_with_checksum, run_id_unvalidated, "run_id")
            step_id_res = validate_id(StepIdValue.from_value_with_checksum, step_id_unvalidated, "step_id")
            cfg_res = shdtodef.StepDefinitionAdapter.from_dict(step_definition_unvalidated)\
                .filter(lambda step_def: get_step_definition_name(type(step_def)) == command, [ValueInvalid("step")])\
                .map(lambda definition: definition.config)\
                .map_error(lambda _: f"Invalid 'definition' data: {step_definition_unvalidated}")
            data_res = data_validator(data_unvalidated)\
                .map_error(lambda _: f"Invalid 'data' value {data_unvalidated}")
            match run_id_res, step_id_res, cfg_res, data_res:
                case Result(tag=ResultTag.OK, ok=run_id), Result(tag=ResultTag.OK, ok=step_id), Result(tag=ResultTag.OK, ok=cfg), Result(tag=ResultTag.OK, ok=data):
                    opt_task_id = TaskIdValue.from_value_with_checksum(metadata_unvalidated.get("task_id"))
                    logger = logger_creator.create(opt_task_id, run_id, step_id)
                    res = input_adapter(run_id, step_id, cfg, data, metadata_unvalidated, logger)
                    return Result.Ok(res)
                case _:
                    errors_with_none = [run_id_res.swap().default_value(None), step_id_res.swap().default_value(None), cfg_res.swap().default_value(None), data_res.swap().default_value(None)]
                    errors = [err for err in errors_with_none if err is not None]
                    err = rabbit_msg_err(ValidationError, ", ".join(errors))
                    return Result.Error(err)
        
        # @apply_types - uncomment this line to inject context variables like logger: Logger
        def __call__(self, message):
            msg: RabbitMessage = message
            logger_creator = RabbitMessageLoggerCreator(msg.raw_message)
            rabbit_msg_err = rabbit_message_error_creator(f"Decoding RUN_STEP_COMMAND({self._command})", msg.correlation_id)
            parsed_data_res = self._parse_rabbitmq_msg_python_pickle(rabbit_msg_err, msg)
            validate_parsed_data = functools.partial(self._validate_rabbitmq_parsed_data, logger_creator, self._command, self._data_validator, self._input_adapter, rabbit_msg_err)
            validated_data_res = parsed_data_res.bind(validate_parsed_data)
            return validated_data_res

def run(rabbit_client: RabbitMQClient, run_id: RunIdValue, step_id: StepIdValue, definition: shdomaindef.StepDefinition, data: Any, metadata: dict):
    command = get_step_definition_name(type(definition))
    message = _python_pickle.data_to_message(run_id, step_id, definition, data, metadata)
    return rabbit_client.send_command(command, message)

class handler:
    def __init__(self, rabbit_client: RabbitMQClient, step_definition_type: type[shdomaindef.StepDefinition[TCfg]], data_validator: Callable[[Any], Result[D, Any]], input_adapter: Callable[[RunIdValue, StepIdValue, TCfg, D], R] | Callable[[RunIdValue, StepIdValue, TCfg, D, dict], R]):
        def validate_input_adapter():
            if not callable(input_adapter):
               raise TypeError(f"input_adapter should be callable, got {type(input_adapter).__name__}")
            return input_adapter
        self._rabbit_client = rabbit_client
        self._step_name = get_step_definition_name(step_definition_type)
        self._data_validator = data_validator
        self._input_adapter = validate_input_adapter()
    
    def _consume_input(self, run_id: RunIdValue, step_id: StepIdValue, cfg: TCfg, data: D, metadata: dict, logger: LoggerAdapter):
        logger.info(f"{self._step_name} RECEIVED metadata {metadata}")
        self._run_id = run_id
        self._step_id = step_id
        self._metadata = metadata
        self._logger = logger
        params = inspect.signature(self._input_adapter).parameters
        include_metadata_param = len(params) > 4
        if include_metadata_param:
            params_array = [run_id, step_id, cfg, data, metadata]
            return self._input_adapter(*params_array)
        else:
            params_array = [run_id, step_id, cfg, data]
            return self._input_adapter(*params_array)
    
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def _send_response(self, result: CompletedResult):
        return rabbit_complete_step.run(self._rabbit_client, self._run_id, self._step_id, result, self._metadata)
    
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
