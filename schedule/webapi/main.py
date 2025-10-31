from shared.commands import ClearCommand, SetCommand

import cleartaskscheduleapihandler
import settaskscheduleapihandler
from config import app, change_task_schedule

@app.post("/schedule/tasks/{id}", status_code=202)
async def set_task_schedule(id: str, request: settaskscheduleapihandler.SetScheduleRequest):
    def set_task_schedule_handler(cmd: settaskscheduleapihandler.SetTaskScheduleCommand):
        set_cmd = SetCommand(cmd.schedule.cron)
        return change_task_schedule(cmd.task_id, cmd.schedule.schedule_id, set_cmd)
    return await settaskscheduleapihandler.handle(set_task_schedule_handler, id, request)

@app.delete("/schedule/tasks/{id}/{schedule_id}", status_code=202)
async def clear_task_schedule(id: str, schedule_id: str):
    def clear_task_schedule_handler(cmd: cleartaskscheduleapihandler.ClearTaskScheduleCommand):
        clear_cmd = ClearCommand()
        return change_task_schedule(cmd.task_id, cmd.schedule_id, clear_cmd)
    return await cleartaskscheduleapihandler.handle(clear_task_schedule_handler, id, schedule_id)