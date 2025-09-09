from fastapi import FastAPI

from config import lifespan

import addtaskapihandler

app = FastAPI(lifespan=lifespan)

@app.post("/tasks")
async def add_task(request: addtaskapihandler.AddTaskRequest):
    return await addtaskapihandler.handle(request)