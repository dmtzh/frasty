from contextlib import asynccontextmanager
import os

from fastapi import FastAPI

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield