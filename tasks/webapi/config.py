from contextlib import asynccontextmanager
import os

from fastapi import FastAPI

from infrastructure.rabbitmq import config
from shared.action import Action, ActionName, ActionType
from shared.customtypes import Metadata, RunIdValue, StepIdValue, TaskIdValue
from shared.pipeline.actionhandler import ActionData, run_action_adapter

# async def execute_definition(input: ExecuteDefinitionInput):
#     run_id = RunIdValue.new_id()
#     step_id = StepIdValue.new_id()
#     metadata = Metadata()
#     metadata.set_from("run task webapi")
#     data = ActionData(run_id, step_id, None, input, metadata)
#     execute_definition_res = await run_execute_definition_action(config.run_action, data)
#     return execute_definition_res.map(lambda _: data)

async def execute_task(task_id: TaskIdValue):
    execute_task_action = Action(ActionName("execute_task"), ActionType.SERVICE)
    run_id = RunIdValue.new_id()
    step_id = StepIdValue.new_id()
    execute_task_input_dto = {"task_id": task_id.to_value_with_checksum()}
    metadata = Metadata()
    metadata.set_from("run task webapi")
    data = ActionData(run_id, step_id, None, execute_task_input_dto, metadata)
    run_action_res = await run_action_adapter(config.run_action)(execute_task_action, data)
    return run_action_res.map(lambda _: data)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']
ADD_DEFINITION_URL = os.environ['ADD_DEFINITION_URL']
CHANGE_SCHEDULE_URL = os.environ['CHANGE_SCHEDULE_URL']

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with config.lifespan():
        await config._broker.start()
        yield
        await config._broker.stop()

app = FastAPI(lifespan=lifespan)
