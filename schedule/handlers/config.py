from collections.abc import Callable
from dataclasses import dataclass
import logging
import os
from typing import Any

import aiocron

from infrastructure.rabbitmq import config
from shared.commands import Command, CommandAdapter
from shared.customtypes import Metadata, RunIdValue, ScheduleIdValue, TaskIdValue
from shared.domainschedule import CronSchedule
from shared.infrastructure.storage.inmemory import InMemory
from shared.pipeline.handlers import HandlerAdapter, map_handler

from scheduler import Scheduler
from shared.pipeline.types import RunTaskData

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def run_task(task_id: TaskIdValue, run_id: RunIdValue, schedule_id: ScheduleIdValue):
    schedule_id_with_checksum = schedule_id.to_value_with_checksum()
    metadata = Metadata()
    metadata.set_from(f"schedule {schedule_id_with_checksum}")
    data = RunTaskData(task_id, run_id, metadata)
    return config.run_task(data)

@dataclass(frozen=True)
class ChangeTaskScheduleData:
    task_id: TaskIdValue
    schedule_id: ScheduleIdValue
    command_dto: dict

def change_task_schedule_handler[T](input_adapter: Callable[[TaskIdValue, ScheduleIdValue, Command], T]):
    handler = config.change_task_schedule_handler(ChangeTaskScheduleData)
    def from_change_task_schedule_data(data: ChangeTaskScheduleData):
        command_res = CommandAdapter.from_dict(data.command_dto)
        return command_res.map(lambda command: input_adapter(data.task_id, data.schedule_id, command))
    change_schedule_handler = map_handler(handler, lambda data_res: data_res.bind(from_change_task_schedule_data))
    return HandlerAdapter(change_schedule_handler)

app = config.create_faststream_app()

_scheduler_states_storage = InMemory[ScheduleIdValue, aiocron.Cron]()
def _add_aiocron_schedule_handler(cron: CronSchedule, action_func: Callable[[], Any]) -> aiocron.Cron:
    state = aiocron.crontab(cron, func=action_func)
    return state
def _remove_aiocron_schedule_handler(state: aiocron.Cron):
    state.stop()
scheduler = Scheduler(_scheduler_states_storage, _add_aiocron_schedule_handler, _remove_aiocron_schedule_handler)

logger = logging.getLogger("schedule_handlers_logger")
logger.setLevel(logging.INFO)
_log_fmt = '%(asctime)s %(levelname)-8s - %(message)s'
formatter = logging.Formatter(fmt=_log_fmt)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)