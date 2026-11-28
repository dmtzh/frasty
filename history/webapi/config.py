from contextlib import asynccontextmanager
import os

from fastapi import FastAPI

from shared.taskresulthistory import LegacyTaskResultHistoryItemAdapter
from shared.taskresultshistorystore import TaskResultsHistoryStore

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

legacy_taskresultshistory_storage = TaskResultsHistoryStore(
    STORAGE_ROOT_FOLDER,
    "LegacyTaskResults",
    LegacyTaskResultHistoryItemAdapter.to_dict,
    LegacyTaskResultHistoryItemAdapter.from_dict
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(lifespan=lifespan)