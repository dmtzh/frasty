from collections.abc import Awaitable, Callable
from enum import StrEnum
from logging import LoggerAdapter
import secrets
from typing import Any

from expression import Result
from faststream.broker.message import StreamMessage
from faststream.exceptions import NackMessage

from shared.utils.result import ResultTag

class RequeueChance(StrEnum):
    LOW = "low"
    FIFTY_FIFTY = "50-50"
    HIGH = "high"

class error_result_to_negative_acknowledge_middleware:
    def __init__(self, requeue_chance: RequeueChance):
        self._requeue_chance = requeue_chance
    
    async def __call__(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        res = await call_next(msg)
        match res:
            case Result(tag=ResultTag.ERROR, error=_):
                match self._requeue_chance:
                    case RequeueChance.LOW:
                        rand_bits = secrets.randbits(3)
                        requeue = rand_bits == 0
                    case RequeueChance.FIFTY_FIFTY:
                        rand_bits = secrets.randbits(1)
                        requeue = rand_bits == 0
                    case RequeueChance.HIGH:
                        rand_bits = secrets.randbits(3)
                        requeue = rand_bits > 0
                raise NackMessage(requeue=requeue)
        return res

class command_handler_logging_middleware:
    def __init__(
        self,
        message_prefix: str,
        logger_creator: Callable[[StreamMessage[Any]], LoggerAdapter]
    ):
        self._message_prefix = message_prefix
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
                first_100_chars = str(data)[:100]
                output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                logger.info(f"{self._message_prefix} RECEIVED {output}")
            case Result(tag=ResultTag.ERROR, error=error):
                logger.error(f"{self._message_prefix} RECEIVED {error}")
            case unsupported_decoded_data:
                logger.warning(f"{self._message_prefix} RECEIVED UNSUPPORTED {unsupported_decoded_data}")
        res = await call_next(msg)
        match res:
            case Result(tag=ResultTag.OK, ok=result):
                first_100_chars = str(result)[:100]
                output = first_100_chars + "..." if len(first_100_chars) == 100 else first_100_chars
                logger.info(f"{self._message_prefix} successfully completed with output {output}")
            case Result(tag=ResultTag.ERROR, error=error):
                logger.error(f"{self._message_prefix} failed with error {error}")
            case None:
                logger.warning(f"{self._message_prefix} PROCESSING SKIPPED")
        return res
