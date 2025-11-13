# import asyncio

from expression import Result

from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue
from shared.infrastructure.storage.repository import NotFoundError
from shared.pipeline.types import RunDefinitionData, RunTaskData
from shared.utils.result import ResultTag

from config import app, publish_completed_definition, run_definition, run_task_handler
import runtaskdefinitionhandler

@run_task_handler
async def handle_run_task_definition_command(data: RunTaskData):
    def run_definition_handler(definition_id: DefinitionIdValue):
        metadata = data.metadata.clone()
        metadata.set_task_id(data.task_id)
        run_def_data = RunDefinitionData(data.run_id, definition_id, metadata)
        return run_definition(run_def_data)
    
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