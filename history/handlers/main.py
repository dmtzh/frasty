# import asyncio

from shared.taskpendingresultsqueue import CompletedTaskData

import addtaskresulttohistoryhandler
from config import app, task_completed_subscriber

@task_completed_subscriber
async def add_task_result_to_history(data: CompletedTaskData):
    add_to_history_res = await addtaskresulttohistoryhandler.handle(data)
    return add_to_history_res

# if __name__ == "__main__":
#     asyncio.run(app.run())