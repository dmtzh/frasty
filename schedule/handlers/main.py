# import asyncio
import datetime
import functools

import aiocron
from expression import Result
from faststream.rabbit.annotations import Logger

from infrastructure import rabbitchangetaskschedule
from infrastructure import rabbitruntask as rabbit_task
from shared.customtypes import TaskIdValue, ScheduleIdValue, RunIdValue
from shared.domainschedule import TaskSchedule
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.inmemory import InMemory
from shared.tasksschedulesstore import tasks_schedules_storage
from shared.utils.asynchronous import make_async
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import ResultTag

import cleartaskschedulehandler
from config import app, rabbit_client
from scheduledtasks import ScheduledTasks
import settaskschedulehandler

@async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
def rabbit_run_task(task_id: TaskIdValue, schedule: TaskSchedule):
    print(f"{datetime.datetime.now()}: running task {task_id} with cron schedule {schedule}")
    run_id = RunIdValue.new_id()
    schedule_id_with_checksum = schedule.schedule_id.to_value_with_checksum()
    return rabbit_task.run(rabbit_client, task_id, run_id, f"schedule {schedule_id_with_checksum}", {})
scheduled_tasks_storage = InMemory[ScheduleIdValue, aiocron.Cron]()
scheduled_tasks = ScheduledTasks(scheduled_tasks_storage)

@app.after_startup
async def init_scheduled_tasks():
    print(f"{datetime.datetime.now()}: initializing schedules...")
    schedules = await tasks_schedules_storage.get_schedules()
    for task_id, schedule in schedules.items():
        schedule_func = functools.partial(rabbit_run_task, task_id)
        scheduled_tasks.add(schedule, schedule_func)
        print(f"{datetime.datetime.now()}: scheduled task {task_id} with cron schedule {schedule} started")
    print(f"{datetime.datetime.now()}: schedules initialized")

@make_async
def stop_scheduled_task(task_id: TaskIdValue, schedule: TaskSchedule):
    try:
        scheduled_tasks.remove(schedule)
        print(f"{datetime.datetime.now()}: scheduled task {task_id} with cron schedule {schedule} stopped")
        return Result.Ok(None)
    except:  # noqa: E722
        return Result.Ok(None)

@make_async
def restart_scheduled_task(task_id: TaskIdValue, old_schedule: TaskSchedule | None, new_schedule: TaskSchedule):
    try:
        if old_schedule is not None:
            scheduled_tasks.remove(old_schedule)
            print(f"{datetime.datetime.now()}: scheduled task {task_id} with cron schedule {old_schedule} stopped")
        schedule_func = functools.partial(rabbit_run_task, task_id)
        scheduled_tasks.add(new_schedule, schedule_func)
        print(f"{datetime.datetime.now()}: scheduled task {task_id} with cron schedule {new_schedule} started")
        return Result.Ok(None)
    except:  # noqa: E722
        return Result.Ok(None)

@rabbitchangetaskschedule.handler(rabbit_client, rabbitchangetaskschedule.ChangeTaskScheduleData)
async def handle_change_task_schedule_command(input, logger: Logger):
    match input:
        case Result(tag=ResultTag.OK, ok=data) if type(data) is rabbitchangetaskschedule.ChangeTaskScheduleData:
            match data.command:
                case rabbitchangetaskschedule.ClearCommand():
                    cmd = cleartaskschedulehandler.ClearTaskScheduleCommand(data.task_id, data.schedule_id)
                    clear_task_schedule_handler = functools.partial(stop_scheduled_task, data.task_id)
                    res = await cleartaskschedulehandler.handle(clear_task_schedule_handler, cmd)
                    return res
                case rabbitchangetaskschedule.SetCommand(schedule=schedule):
                    schedule = TaskSchedule(data.schedule_id, schedule)
                    cmd = settaskschedulehandler.SetTaskScheduleCommand(data.task_id, schedule)
                    set_task_schedule_handler = functools.partial(restart_scheduled_task, data.task_id)
                    res = await settaskschedulehandler.handle(set_task_schedule_handler, cmd)
                    return res
        case Result(tag=ResultTag.ERROR, error=error):
            # TODO: Handle error case
            logger.warning(f">>>> Received invalid change task schedule command data: {error}")

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