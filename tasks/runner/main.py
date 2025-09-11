# import asyncio

from dataclasses import dataclass

from expression import Result
from faststream.rabbit.annotations import Logger

from infrastructure import rabbitdefinitioncompleted as rabbit_definition_completed
from infrastructure import rabbitrundefinition as rabbit_definition
from infrastructure import rabbitruntask as rabbit_run_task
from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, RunIdValue, TaskIdValue
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.task import Task
from shared.tasksstore import tasks_storage
from shared.utils.asyncresult import async_ex_to_error_result, async_result, coroutine_result
from shared.utils.result import ResultTag

from config import app, rabbit_client

@dataclass(frozen=True)
class RunTaskCommand:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: dict

@dataclass(frozen=True)
class RunDefinitionCommand:
    definition_id: DefinitionIdValue
    run_id: RunIdValue
    metadata: dict

class TasksStorageError(StorageError):
    '''Unexpected tasks storage error'''

@rabbit_run_task.handler(rabbit_client, RunTaskCommand)
async def handle_run_task_command(input, logger: Logger):
    @async_result
    @async_ex_to_error_result(TasksStorageError.from_exception)
    async def get_task(task_id: TaskIdValue) -> Result[Task, NotFoundError]:
        opt_task = await tasks_storage.get(task_id)
        match opt_task:
            case None:
                return Result.Error(NotFoundError(f"Task {task_id} not found"))
            case task:
                return Result.Ok(task)
    @async_result
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_run_definition_handler(cmd: RunDefinitionCommand):
        return rabbit_definition.run(rabbit_client, cmd.definition_id, cmd.run_id, cmd.metadata)
    @coroutine_result()
    async def run_task_workflow(cmd: RunTaskCommand):
        task = await get_task(cmd.task_id)
        run_def_cmd = RunDefinitionCommand(task.definition_id, cmd.run_id, cmd.metadata)
        await rabbit_run_definition_handler(run_def_cmd)
    @async_ex_to_error_result(RabbitClientError.UnexpectedError.from_exception)
    def rabbit_runtask_failure_handler(cmd: RunTaskCommand, error):
        result = CompletedWith.Error(str(error))
        return rabbit_definition_completed.publish(rabbit_client, cmd.task_id, cmd.run_id, cmd.run_id, result, cmd.metadata)
    match input:
        case Result(tag=ResultTag.OK, ok=cmd) if type(cmd) is RunTaskCommand:
            res = await run_task_workflow(cmd)
            match res:
                case Result(tag=ResultTag.ERROR, error=NotFoundError()):
                    return None
                case Result(tag=ResultTag.ERROR, error=error):
                    return await rabbit_runtask_failure_handler(cmd, error)
                case _:
                    return res
        case Result(tag=ResultTag.ERROR, error=error):
            # TODO: Handle error case
            logger.warning(f">>>> Received invalid RunTask command data: {error}")

# if __name__ == "__main__":
#     asyncio.run(app.run())