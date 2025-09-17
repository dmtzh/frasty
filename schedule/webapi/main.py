from fastapi import FastAPI

from config import lifespan
import settaskscheduleapihandler

app = FastAPI(lifespan=lifespan)

@app.post("/schedule/tasks")
async def set_task_schedule(request: settaskscheduleapihandler.SetScheduleRequest):
    return await settaskscheduleapihandler.handle(request)