from collections.abc import Callable, Generator
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result, effect

from shared.customtypes import ScheduleIdValue, TaskIdValue
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag
from shared.utils.string import strip_and_lowercase
from shared.validation import ValueError as ValueErr, ValueInvalid, ValueMissing

from .domainschedule import CronSchedule, CronScheduleAdapter

@dataclass(frozen=True)
class ClearCommand:
    '''Clear task schedule command'''
    task_id: TaskIdValue
    schedule_id: ScheduleIdValue

@dataclass(frozen=True)
class SetCommand:
    '''Set task schedule command'''
    task_id: TaskIdValue
    schedule_id: ScheduleIdValue
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
    def to_dict(command: Command) -> dict[str, str]:
        match command:
            case ClearCommand():
                return {
                    "type": CommandDtoTypes.CLEAR.value,
                    "task_id": command.task_id.to_value_with_checksum(),
                    "schedule_id": command.schedule_id.to_value_with_checksum()
                }
            case SetCommand(schedule=schedule):
                return {
                    "type": CommandDtoTypes.SET.value,
                    "task_id": command.task_id.to_value_with_checksum(),
                    "schedule_id": command.schedule_id.to_value_with_checksum()
                } | CronScheduleAdapter.to_dict(schedule)
    
    @staticmethod
    def _parse_clear_command(command_dto: dict) -> Result[ClearCommand, list[ValueErr]]:
        def parse_id[T](id_name: str, id_parser: Callable[[str], T | None]) -> Result[T, list[ValueErr]]:
            if id_name not in command_dto:
                return Result.Error([ValueMissing(id_name)])
            return parse_from_dict(command_dto, id_name, id_parser).map_error(lambda _: [ValueInvalid(id_name)])
        task_id_res = parse_id("task_id", TaskIdValue.from_value_with_checksum)
        schedule_id_res = parse_id("schedule_id", ScheduleIdValue.from_value_with_checksum)
        match task_id_res, schedule_id_res:
            case Result(tag=ResultTag.OK, ok=task_id), Result(tag=ResultTag.OK, ok=schedule_id):
                return Result.Ok(ClearCommand(task_id, schedule_id))
            case _:
                errors = task_id_res.swap().default_value([]) + schedule_id_res.swap().default_value([])
                return Result.Error(errors)
    
    @staticmethod
    def _parse_set_command(command_dto: dict) -> Result[SetCommand, list[ValueErr]]:
        def parse_id[T](id_name: str, id_parser: Callable[[str], T | None]) -> Result[T, list[ValueErr]]:
            if id_name not in command_dto:
                return Result.Error([ValueMissing(id_name)])
            return parse_from_dict(command_dto, id_name, id_parser).map_error(lambda _: [ValueInvalid(id_name)])
        task_id_res = parse_id("task_id", TaskIdValue.from_value_with_checksum)
        schedule_id_res = parse_id("schedule_id", ScheduleIdValue.from_value_with_checksum)
        schedule_res = CronScheduleAdapter.from_dict(command_dto)
        match task_id_res, schedule_id_res, schedule_res:
            case Result(tag=ResultTag.OK, ok=task_id), Result(tag=ResultTag.OK, ok=schedule_id), Result(tag=ResultTag.OK, ok=schedule):
                return Result.Ok(SetCommand(task_id, schedule_id, schedule))
            case _:
                errors = task_id_res.swap().default_value([]) + schedule_id_res.swap().default_value([]) + schedule_res.swap().default_value([])
                return Result.Error(errors)
    
    @effect.result[Command, list[ValueErr]]()
    @staticmethod
    def from_dict(command_dto: dict[str, Any]) -> Generator[Any, Any, Command]:
        def parse_command_type() -> Result[CommandDtoTypes, list[ValueErr]]:
            if "type" not in command_dto:
                return Result.Error([ValueMissing("type")])
            return parse_from_dict(command_dto, "type", CommandDtoTypes.parse).map_error(lambda _: [ValueInvalid("type")])
        command_type = yield from parse_command_type()
        match command_type:
            case CommandDtoTypes.CLEAR:
                clear_cmd = yield from CommandAdapter._parse_clear_command(command_dto)
                return clear_cmd
            case CommandDtoTypes.SET:
                set_cmd = yield from CommandAdapter._parse_set_command(command_dto)
                return set_cmd
            case _:
                yield from Result.Error([ValueInvalid("type")])
                raise RuntimeError("command type is invalid")