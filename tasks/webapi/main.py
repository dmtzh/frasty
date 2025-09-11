from fastapi import FastAPI

from config import lifespan

import addtaskapihandler
import runtaskapihandler

app = FastAPI(lifespan=lifespan)

@app.post("/tasks")
async def add_task(request: addtaskapihandler.AddTaskRequest):
    return await addtaskapihandler.handle(request)

@app.post("/tasks/{id}/run", status_code=201)
async def run_task(id: str):
    return await runtaskapihandler.handle(id)