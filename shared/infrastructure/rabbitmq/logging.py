import logging

from aio_pika.abc import AbstractIncomingMessage

from shared.customtypes import IdValue

class RabbitMessageLoggingFormatter(logging.Formatter):

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
            colored_levelname = self.COLORS.get(record.levelno) + self._LEVELNAME + levelname_tail + "s" + self.RESET
            return colored_levelname + after_levelname
        
        result = (self._fmt or []).split(self._LEVELNAME)
        match result:
            case [] | [_]: 
                return super().format(record)
            case [head, *tail]:
                log_fmt = head + "".join(map(prepend_with_colored_levelname, tail))
                formatter = logging.Formatter(log_fmt)
                return formatter.format(record)

class RabbitMessageLoggerCreator():
    def __init__(self, rabbit_msg: AbstractIncomingMessage):
        self._extra = {"exchange": rabbit_msg.exchange, "queue": rabbit_msg.routing_key}
    
    def create(self, opt_task_id: IdValue | None, run_id: IdValue, step_id: IdValue):
        logger = logging.getLogger("rabbit_message_logger")
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            log_fmt = '%(asctime)s %(levelname)-8s - %(exchange)-4s | %(queue)-10s | %(message_id)-30s: %(message)s'
            formatter = RabbitMessageLoggingFormatter(log_fmt)
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        message_id = f"{opt_task_id}-->>{run_id}->{step_id}"
        extra = self._extra | {"message_id": message_id}
        return logging.LoggerAdapter(logger, extra=extra)