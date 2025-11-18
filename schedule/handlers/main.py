# import asyncio
from dataclasses import dataclass
import functools

from expression import Result

from shared.commands import Command, ClearCommand, SetCommand
from shared.customtypes import ScheduleIdValue, TaskIdValue, RunIdValue
from shared.domainschedule import TaskSchedule, CronSchedule
from shared.tasksschedulesstore import tasks_schedules_storage
from shared.utils.asynchronous import make_async

import cleartaskschedulehandler
import settaskschedulehandler
from config import app, change_task_schedule_handler, logger, run_task, scheduler

def run_task_action(task_id: TaskIdValue, schedule: TaskSchedule):
    logger.info(f"Running {task_id} with schedule {schedule}")
    run_id = RunIdValue.new_id()
    return run_task(task_id, run_id, schedule.schedule_id)
    
@app.after_startup
async def init_scheduled_tasks():
    logger.info("Initializing scheduled tasks...")
    schedules = await tasks_schedules_storage.get_schedules()
    for task_id, schedule in schedules.items():
        schedule_action_func = functools.partial(run_task_action, task_id, schedule)
        scheduler.add(schedule.schedule_id, schedule.cron, schedule_action_func)
        logger.info(f"{task_id} with schedule {schedule} started")
    logger.info("Scheduled tasks initialized")

@make_async
def stop_scheduled_task(cmd: cleartaskschedulehandler.ClearTaskScheduleCommand, cron: CronSchedule):
    try:
        scheduler.remove(cmd.schedule_id)
        schedule = TaskSchedule(cmd.schedule_id, cron)
        logger.warning(f"{cmd.task_id} with schedule {schedule} stopped")
        return Result.Ok(None)
    except:  # noqa: E722
        return Result.Ok(None)

@make_async
def restart_scheduled_task(task_id: TaskIdValue, old_schedule: TaskSchedule | None, new_schedule: TaskSchedule):
    try:
        if old_schedule is not None:
            scheduler.remove(old_schedule.schedule_id)
            logger.warning(f"{task_id} with schedule {old_schedule} stopped")
        schedule_action_func = functools.partial(run_task_action, task_id, new_schedule)
        scheduler.add(new_schedule.schedule_id, new_schedule.cron, schedule_action_func)
        logger.info(f"{task_id} with schedule {new_schedule} started")
        return Result.Ok(None)
    except:  # noqa: E722
        return Result.Ok(None)

@dataclass(frozen=True)
class ChangeTaskScheduleData:
    task_id: TaskIdValue
    schedule_id: ScheduleIdValue
    command: Command

@change_task_schedule_handler(ChangeTaskScheduleData)
async def handle_change_task_schedule_command(data: ChangeTaskScheduleData):
    match data.command:
        case ClearCommand():
            cmd = cleartaskschedulehandler.ClearTaskScheduleCommand(data.task_id, data.schedule_id)
            clear_task_schedule_handler = functools.partial(stop_scheduled_task, cmd)
            res = await cleartaskschedulehandler.handle(clear_task_schedule_handler, cmd)
            return res
        case SetCommand(schedule=schedule):
            schedule = TaskSchedule(data.schedule_id, schedule)
            cmd = settaskschedulehandler.SetTaskScheduleCommand(data.task_id, schedule)
            set_task_schedule_handler = functools.partial(restart_scheduled_task, data.task_id)
            res = await settaskschedulehandler.handle(set_task_schedule_handler, cmd)
            return res

# if __name__ == "__main__":
#     asyncio.run(app.run())

# def main():
#     """Main function to perform setup and start the loop."""
#     # 1. Get the event loop
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)

#     try:
#         # 2. Run the one-time async setup until completion
#         print("Running init step with run_until_complete...")
#         setup = lifespan.__aenter__()
#         loop.run_until_complete(setup)
#         loop.run_until_complete(init_scheduled_tasks())

#         # 3. Start the event loop and run forever
#         print("Starting the event loop with run_forever... To exit, press CTRL+C")
#         loop.run_forever()

#     except KeyboardInterrupt:
#         print("Received shutdown signal. Stopping the loop...")

#     finally:
#         # Graceful shutdown process
#         tasks = asyncio.all_tasks(loop=loop)
#         for task in tasks:
#             task.cancel()
#         teardown = lifespan.__aexit__(None, None, None)
#         loop.run_until_complete(asyncio.gather(teardown, *tasks, return_exceptions=True))
#         loop.close()
#         print("Event loop closed.")

# if __name__ == "__main__":
#     main()