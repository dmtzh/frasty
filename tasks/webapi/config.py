from contextlib import asynccontextmanager
import os

from fastapi import FastAPI

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
ADD_DEFINITION_URL = os.environ['ADD_DEFINITION_URL']

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield