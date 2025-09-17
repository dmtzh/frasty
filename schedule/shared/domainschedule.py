from dataclasses import dataclass
import datetime

from cronsim import CronSim, CronSimError
from expression import Result

@dataclass(frozen=True)
class CronSchedule:
    value: str

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
        return {"cron": schedule.value}

    @staticmethod
    def from_dict(data: dict[str, str]) -> Result[CronSchedule, str]:
        if "cron" not in data:
            return Result.Error("cron is missing")
        raw_cron = data["cron"]
        opt_cron_schedule = CronSchedule.parse(raw_cron)
        match opt_cron_schedule:
            case None:
                return Result.Error(f"invalid cron {raw_cron}")
            case cron_schedule:
                return Result.Ok(cron_schedule)