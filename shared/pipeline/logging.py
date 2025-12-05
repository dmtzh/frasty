import logging
from typing import Any

from expression import Result

from shared.customtypes import Metadata, RunIdValue, StepIdValue, TaskIdValue
from shared.pipeline.types import CompletedDefinitionData
from shared.utils.parse import parse_from_dict, parse_value
from shared.utils.result import ResultTag

from .handlers import HandlerContinuation, Subscriber, with_middleware

class LoggingFormatter(logging.Formatter):

    GREY = "\x1b[38;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    GREEN = "\x1b[32;20m"
    BLUE = "\x1b[34;20m"
    PURPLE = "\x1b[35;20m"
    RESET = "\x1b[0m"

    COLORS = {
        logging.DEBUG: GREY,
        logging.INFO: PURPLE,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: BOLD_RED
    }

    _LEVELNAME = "%(levelname)"

    def format(self, record):
        def prepend_with_colored_levelname(source: str) -> str:
            levelname_tail, after_levelname = source.split("s", 1)
            colored_levelname = self.COLORS.get(record.levelno, "") + self._LEVELNAME + levelname_tail + "s" + self.RESET
            return colored_levelname + after_levelname
        
        result = (self._fmt or "").split(self._LEVELNAME)
        match result:
            case [] | [_]: 
                return super().format(record)
            case [head, *tail]:
                log_fmt = head + "".join(map(prepend_with_colored_levelname, tail))
                formatter = logging.Formatter(log_fmt)
                return formatter.format(record)

def pipeline_logger[T](message_prefix: str, input_res: Result[T, Any]):
    logger = logging.getLogger("pipeline_logger")
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        log_fmt = '%(asctime)s %(levelname)-8s - %(message_id)-30s: %(message_prefix)s%(message)s'
        formatter = LoggingFormatter(log_fmt)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    match input_res:
        case Result(tag=ResultTag.OK, ok=data):
            data_task_id_res = parse_value(getattr(data, "task_id", None), "task_id", lambda val: val if isinstance(val, TaskIdValue) else None)
            metadata_res = parse_value(getattr(data, "metadata", None), "metadata", lambda val: val if isinstance(val, Metadata) else None)
            metadata_task_id_res = metadata_res.bind(lambda m: parse_from_dict(m, "task_id", TaskIdValue.from_value_with_checksum))
            opt_task_id = data_task_id_res.or_else(metadata_task_id_res).default_value(None)
            opt_run_id = parse_value(getattr(data, "run_id", None), "run_id", lambda val: val if isinstance(val, RunIdValue) else None).default_value(None)
            opt_step_id = parse_value(getattr(data, "step_id", None), "step_id", lambda val: val if isinstance(val, StepIdValue) else None).default_value(None)
            message_id = f"{opt_task_id}-->>{opt_run_id}->{opt_step_id}"
        case _:
            message_id = ""
    message_prefix_str = f"{message_prefix} " if message_prefix is not None else ""
    extra = {"message_id": message_id, "message_prefix": message_prefix_str}
    return logging.LoggerAdapter(logger, extra=extra)

class with_input_output_logging[T]:
    def __init__(self, handler: HandlerContinuation[T], message_prefix: str):
        self._handler = handler
        self._message_prefix = message_prefix
        self.__name__ = handler.__name__
    
    async def __call__(self, input_res: Result[T, Any]):
        logger = pipeline_logger(self._message_prefix, input_res)
        match input_res:
            case Result(tag=ResultTag.OK, ok=data):
                first_100_chars = str(data)[:100]
                output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                logger.info(f"RECEIVED {output}")
            case Result(tag=ResultTag.ERROR, error=error):
                logger.error(f"RECEIVED {error}")
            case unsupported_input:
                logger.warning(f"RECEIVED UNSUPPORTED {unsupported_input}")
        res = await self._handler(input_res)
        match res:
            case Result(tag=ResultTag.OK, ok=result):
                first_100_chars = str(result)[:100]
                output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                logger.info(f"successfully completed with output {output}")
            case Result(tag=ResultTag.ERROR, error=error):
                logger.error(f"failed with error {error}")
            case None:
                logger.warning("PROCESSING SKIPPED")
        return res

def with_input_output_logging_subscriber(subscriber: Subscriber, message_prefix: str) -> Subscriber:
    def decorate_with_logs(decoratee: HandlerContinuation[CompletedDefinitionData]):
        async def logs_middleware(input_res: Result[CompletedDefinitionData, Any]) -> Result | None:
            logger = pipeline_logger(message_prefix, input_res)
            match input_res:
                case Result(tag=ResultTag.OK, ok=data):
                    logger.info(f"RECEIVED {data}")
                case Result(tag=ResultTag.ERROR, error=error):
                    logger.error(f"RECEIVED {error}")
                case unsupported_input:
                    logger.warning(f"RECEIVED UNSUPPORTED {unsupported_input}")
            res = await decoratee(input_res)
            match res:
                case Result(tag=ResultTag.OK, ok=output):
                    logger.info(f"data processed with output {output}")
                case Result(tag=ResultTag.ERROR, error=error):
                    logger.error(f"data failed to process with error {error}")
                case None:
                    logger.warning("PROCESSING SKIPPED")
            return res
        return logs_middleware
    return with_middleware(subscriber, decorate_with_logs)