from collections.abc import Callable, Coroutine
from functools import wraps
import logging
import os
from typing import Any

import aiocron
from expression import Result

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.changetaskcheduleaction import CHANGE_TASK_SCHEDULE_ACTION
from shared.commands import Command, CommandAdapter
from shared.customtypes import Metadata, RunIdValue, ScheduleIdValue, StepIdValue, TaskIdValue
from shared.domainschedule import CronSchedule
from shared.infrastructure.storage.inmemory import InMemory
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, ActionInput, run_action_adapter

from scheduler import Scheduler
from shared.pipeline.types import RunTaskData

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

def run_task(task_id: TaskIdValue, schedule_id: ScheduleIdValue):
    execute_task_action = Action(ActionName("execute_task"), ActionType.SERVICE)
    run_id = RunIdValue.new_id()
    step_id = StepIdValue.new_id()
    execute_task_input_dto = {"task_id": task_id.to_value_with_checksum()}
    schedule_id_with_checksum = schedule_id.to_value_with_checksum()
    metadata = Metadata()
    metadata.set_from(f"schedule {schedule_id_with_checksum}")
    run_action_dto = ActionData(run_id, step_id, None, execute_task_input_dto, metadata)
    return run_action_adapter(config.run_action)(execute_task_action, run_action_dto)

def run_legacy_task(task_id: TaskIdValue, run_id: RunIdValue, schedule_id: ScheduleIdValue):
    schedule_id_with_checksum = schedule_id.to_value_with_checksum()
    metadata = Metadata()
    metadata.set_from(f"legacy task schedule {schedule_id_with_checksum}")
    data = RunTaskData(task_id, run_id, metadata)
    return config.run_task(data)

def change_task_schedule_handler(func: Callable[[Command], Coroutine]):
    async def do_nothing_when_run_action(action_name: str, action_input: ActionInput):
        return Result.Ok(None)
    @wraps(func)
    async def func_adapter(data: ActionData[None, Command]):
        await func(data.input)
        return None
    return ActionHandlerFactory(do_nothing_when_run_action, config.action_handler).create_without_config(
        CHANGE_TASK_SCHEDULE_ACTION,
        lambda dto_list: CommandAdapter.from_dict(dto_list[0])
    )(func_adapter)

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