from collections.abc import Callable, Coroutine
import logging
import os
from typing import Any

import aiocron
from expression import Result

from infrastructure.rabbitmq import config
from shared.changetaskscheduledefinition import ChangeTaskSchedule
from shared.commands import Command
from shared.completedresult import CompletedResult, CompletedWith
from shared.customtypes import Metadata, RunIdValue, ScheduleIdValue, TaskIdValue
from shared.domainschedule import CronSchedule
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.infrastructure.storage.inmemory import InMemory
from shared.pipeline.handlers import StepHandlerAdapterFactory

from scheduler import Scheduler
from shared.pipeline.types import CompleteStepData, RunTaskData, StepData

step_definition_creators_storage.add(ChangeTaskSchedule)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def run_task(task_id: TaskIdValue, run_id: RunIdValue, schedule_id: ScheduleIdValue):
    schedule_id_with_checksum = schedule_id.to_value_with_checksum()
    metadata = Metadata()
    metadata.set_from(f"schedule {schedule_id_with_checksum}")
    data = RunTaskData(task_id, run_id, metadata)
    return config.run_task(data)

def change_task_schedule_handler(func: Callable[[Command], Coroutine[Any, Any, Result | None]]):
    class wrapper:
        def __init__(self):
            self.__name__ = func.__name__
        async def __call__(self, data: StepData[None, Command]) -> CompletedResult | None:
            opt_res = await func(data.data)
            if opt_res is None:
                return None
            result_res = opt_res\
                .map(lambda _: CompletedWith.Data(None))\
                .map_error(lambda error: CompletedWith.Error(str(error)))
            result = result_res.merge()
            return result
    async def complete_step(data: CompleteStepData):
        return Result.Ok(None)
    step_handler_adapter_factory = StepHandlerAdapterFactory(config.step_handler, complete_step)
    step_handler_adapter = step_handler_adapter_factory(ChangeTaskSchedule, ChangeTaskSchedule.validate_input)
    return step_handler_adapter(wrapper())

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