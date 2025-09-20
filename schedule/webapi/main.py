from fastapi import FastAPI

from infrastructure import rabbitchangetaskschedule as rabbit_change_task_schedule
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.utils.asyncresult import async_ex_to_error_result

from config import lifespan, rabbit_client
import cleartaskscheduleapihandler
import settaskscheduleapihandler

app = FastAPI(lifespan=lifespan)

@app.post("/schedule/tasks", status_code=201)
async def set_task_schedule(request: settaskscheduleapihandler.SetScheduleRequest):
    return await settaskscheduleapihandler.handle(request)

@app.delete("/schedule/tasks/{id}/{schedule_id}", status_code=202)
async def clear_task_schedule(id: str, schedule_id: str):
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_clear_task_schedule_handler(cmd: cleartaskscheduleapihandler.ClearTaskScheduleCommand):
        clear_cmd = rabbit_change_task_schedule.ClearCommand()
        return rabbit_change_task_schedule.run(rabbit_client, cmd.task_id, cmd.schedule_id, clear_cmd, {})
    return await cleartaskscheduleapihandler.handle(rabbit_clear_task_schedule_handler, id, schedule_id)