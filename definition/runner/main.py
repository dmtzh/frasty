# import asyncio
from shared.completedresult import CompletedWith
from shared.definition import Definition
from shared.definitionsstore import definitions_storage
from shared.executedefinitionaction import ExecuteDefinitionInput
from shared.infrastructure.storage.repository import StorageError
from shared.pipeline.actionhandler import ActionData
from shared.utils.asyncresult import async_ex_to_error_result

from config import GetDefinitionInput, action_handler, app, get_definition_handler, run_action
import completeactionhandler
import executedefinitionhandler

# ------------------------------------------------------------------------------------------------------------

executedefinitionhandler.register_execute_definition_action_handler(run_action, action_handler)

# ------------------------------------------------------------------------------------------------------------

completeactionhandler.register_complete_action_handler(run_action, action_handler)

# ------------------------------------------------------------------------------------------------------------

@get_definition_handler
async def handle_get_definition_action(data: ActionData[None, GetDefinitionInput]):
    def opt_definition_to_completed_result(opt_definition_with_ver: tuple[Definition, int] | None):
        match opt_definition_with_ver:
            case None:
                return CompletedWith.NoData()
            case (definition, _):
                data_dict = ExecuteDefinitionInput(data.input.definition_id, definition).to_dict()
                return CompletedWith.Data(data_dict)
    get_definition_with_ver = async_ex_to_error_result(StorageError.from_exception)(definitions_storage.get_with_ver)
    opt_definition_with_ver_res = await get_definition_with_ver(data.input.definition_id)
    return opt_definition_with_ver_res.map(opt_definition_to_completed_result).default_with(lambda err: CompletedWith.Error(str(err)))

# ------------------------------------------------------------------------------------------------------------

# if __name__ == "__main__":
#     asyncio.run(app.run())