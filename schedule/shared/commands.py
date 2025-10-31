from collections.abc import Generator
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result, effect

from shared.utils.parse import parse_from_dict
from shared.utils.string import strip_and_lowercase

from .domainschedule import CronSchedule, CronScheduleAdapter

@dataclass(frozen=True)
class ClearCommand:
    '''Clear task schedule command'''

@dataclass(frozen=True)
class SetCommand:
    '''Set task schedule command'''
    schedule: CronSchedule

type Command = ClearCommand | SetCommand

class CommandDtoTypes(StrEnum):
    CLEAR = ClearCommand.__name__.lower()
    SET = SetCommand.__name__.lower()
    @staticmethod
    def parse(command_type: str) -> Optional["CommandDtoTypes"]:
        if command_type is None:
            return None
        match strip_and_lowercase(command_type):
            case CommandDtoTypes.CLEAR:
                return CommandDtoTypes.CLEAR
            case CommandDtoTypes.SET:
                return CommandDtoTypes.SET
            case _:
                return None

class CommandAdapter:
    @staticmethod
    def to_dict(command: Command) -> dict:
        match command:
            case ClearCommand():
                return {"type": CommandDtoTypes.CLEAR}
            case SetCommand(schedule=schedule):
                return {"type": CommandDtoTypes.SET} | CronScheduleAdapter.to_dict(schedule)
    
    @effect.result[Command, str]()
    @staticmethod
    def from_dict(command_dto: dict) -> Generator[Any, Any, Command]:
        command_type = yield from parse_from_dict(command_dto, "type", CommandDtoTypes.parse)
        match command_type:
            case CommandDtoTypes.CLEAR:
                return ClearCommand()
            case CommandDtoTypes.SET:
                schedule = yield from CronScheduleAdapter.from_dict(command_dto)
                return SetCommand(schedule)
            case _:
                yield from Result.Error(f"command type {command_type} is invalid")
                raise RuntimeError("command type is invalid")