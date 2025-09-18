from dataclasses import dataclass
import datetime

from cronsim import CronSim, CronSimError
from expression import Result

from shared.customtypes import ScheduleIdValue
from shared.utils.parse import parse_from_dict
from shared.utils.result import ResultTag

class CronSchedule(str):
    def __new__(cls, value):
        instance = super().__new__(cls, value)
        return instance

    @staticmethod
    def parse(cron: str):
        if cron is None:
            return None
        match cron.strip():
            case "":
                return None
            case cron_stripped:
                try:
                    CronSim(cron_stripped, datetime.datetime.now())
                    return CronSchedule(cron_stripped)
                except CronSimError:
                    return None

class CronScheduleAdapter:
    @staticmethod
    def to_dict(schedule: CronSchedule) -> dict[str, str]:
        return {"cron": schedule}

    @staticmethod
    def from_dict(data: dict[str, str]) -> Result[CronSchedule, str]:
        return parse_from_dict(data, "cron", CronSchedule.parse)

@dataclass(frozen=True)
class TaskSchedule:
    schedule_id: ScheduleIdValue
    cron: CronSchedule

class TaskScheduleAdapter:
    @staticmethod
    def to_dict(schedule: TaskSchedule) -> dict[str, str]:
        return {"schedule_id": schedule.schedule_id} | CronScheduleAdapter.to_dict(schedule.cron)
    
    @staticmethod
    def from_dict(raw_schedule: dict[str, str]) -> Result[TaskSchedule, str]:
        schedule_id_res = parse_from_dict(raw_schedule, "schedule_id", ScheduleIdValue.from_value)
        cron_res = CronScheduleAdapter.from_dict(raw_schedule)
        match schedule_id_res, cron_res:
            case Result(tag=ResultTag.OK, ok=schedule_id), Result(tag=ResultTag.OK, ok=cron_schedule):
                return Result.Ok(TaskSchedule(schedule_id, cron_schedule))
            case _:
                errors_with_none = [schedule_id_res.swap().default_value(None), cron_res.swap().default_value(None)]
                errors = [err for err in errors_with_none if err is not None]
                err = ", ".join(errors)
                return Result.Error(err)