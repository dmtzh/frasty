import asyncio
import datetime
import functools

import aiocron

from infrastructure import rabbitruntask as rabbit_task
from shared.customtypes import TaskIdValue, ScheduleIdValue, RunIdValue
from shared.infrastructure.rabbitmq.broker import RabbitMQBroker
from shared.infrastructure.rabbitmq.client import RabbitMQClient
from shared.tasksschedulesstore import tasks_schedules_storage

from config import rabbitmqconfig

def rabbit_run_task(rabbit_client: RabbitMQClient, task_id: TaskIdValue, schedule_id: ScheduleIdValue):
    print(f"{datetime.datetime.now()}: running task {task_id} with cron schedule {schedule_id}")
    run_id = RunIdValue.new_id()
    schedule_id_with_checksum = schedule_id.to_value_with_checksum()
    return rabbit_task.run(rabbit_client, task_id, run_id, f"schedule {schedule_id_with_checksum}", {})

async def init_scheduled_tasks(rabbit_client: RabbitMQClient):
    print(f"{datetime.datetime.now()}: initializing schedules...")
    schedules = await tasks_schedules_storage.get_schedules()
    for task_id, task_schedule in schedules.items():
        schedule_func = functools.partial(rabbit_run_task, rabbit_client, task_id, task_schedule.schedule_id)
        aiocron.crontab(task_schedule.cron, func=schedule_func)
        print(f"{datetime.datetime.now()}: scheduled task {task_id} with cron schedule {task_schedule.cron} initialized")
    print(f"{datetime.datetime.now()}: schedules initialized")

def main():
    """Main function to perform setup and start the loop."""
    # 1. Get the event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # 2. Run the one-time async setup until completion
        print("Running init step with run_until_complete...")
        def subscriber():
            raise ValueError("Not connected")
        rabbit_broker = RabbitMQBroker(subscriber)
        rabbit_client = RabbitMQClient(rabbit_broker)
        loop.run_until_complete(rabbit_broker.connect(rabbitmqconfig))
        loop.run_until_complete(init_scheduled_tasks(rabbit_client))

        # 3. Start the event loop and run forever
        print("Starting the event loop with run_forever... To exit, press CTRL+C")
        loop.run_forever()

    except KeyboardInterrupt:
        print("Received shutdown signal. Stopping the loop...")

    finally:
        # Graceful shutdown process
        tasks = asyncio.all_tasks(loop=loop)
        for task in tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(rabbit_broker.disconnect(), *tasks, return_exceptions=True))
        loop.close()
        print("Event loop closed.")

if __name__ == "__main__":
    main()