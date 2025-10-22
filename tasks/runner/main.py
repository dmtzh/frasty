# import asyncio
from dataclasses import dataclass

from expression import Result

from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, RunIdValue, TaskIdValue
from shared.infrastructure.storage.repository import NotFoundError
from shared.utils.result import ResultTag

from config import app, publish_completed_definition, run_definition, run_task_handler
import runtaskdefinitionhandler

@dataclass(frozen=True)
class RunTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: dict

@run_task_handler(RunTaskData)
async def handle_run_task_definition_command(data: RunTaskData):
    def run_definition_handler(definition_id: DefinitionIdValue):
        task_id_dict = {"task_id": data.task_id.to_value_with_checksum()}
        metadata = data.metadata | task_id_dict
        return run_definition(data.run_id, definition_id, metadata)    
    
    cmd = runtaskdefinitionhandler.RunTaskDefinitionCommand(data.task_id, data.run_id)
    run_task_definition_res = await runtaskdefinitionhandler.handle(run_definition_handler, cmd)
    match run_task_definition_res:
        case Result(tag=ResultTag.ERROR, error=NotFoundError()):
            return None
        case Result(tag=ResultTag.ERROR, error=error):
            definition_id = DefinitionIdValue(data.run_id)
            error_result = CompletedWith.Error(str(error))
            publish_completed_definition_res = await publish_completed_definition(data.run_id, definition_id, error_result, data.metadata)
            return publish_completed_definition_res.map(lambda _: error_result)
        case _:
            return run_task_definition_res

# if __name__ == "__main__":
#     asyncio.run(app.run())