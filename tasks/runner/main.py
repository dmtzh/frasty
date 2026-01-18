# import asyncio

from expression import Result

from shared.action import ActionName, ActionType
from shared.completedresult import CompletedResultAdapter, CompletedWith
from shared.customtypes import DefinitionIdValue, TaskIdValue
from shared.definition import ActionDefinition, Definition
from shared.executedefinitionaction import EXECUTE_DEFINITION_ACTION, ExecuteDefinitionInput
from shared.infrastructure.storage.repository import NotFoundError, StorageError
from shared.pipeline.actionhandler import ActionData
from shared.task import Task
from shared.tasksstore import tasks_storage
from shared.utils.asyncresult import async_ex_to_error_result
from shared.utils.result import lift_param

from config import app, execute_definition, execute_task_handler

@execute_task_handler
async def handle_execute_task_action(data: ActionData[None, TaskIdValue]):
    @async_ex_to_error_result(StorageError.from_exception)
    async def get_task(task_id: TaskIdValue) -> Result[Task, NotFoundError]:
        opt_task = await tasks_storage.get(task_id)
        return Result.Ok(opt_task) if opt_task is not None else Result.Error(NotFoundError(f"Task {task_id} not found"))
    def to_execute_definition_data(task: Task):
        execution_id = DefinitionIdValue.new_id()
        definition_input_data = {"definition_id": task.definition_id.to_value_with_checksum()}
        add_task_result_to_history_config = {
            "task_id": data.input.to_value_with_checksum(),
            "execution_id": execution_id.to_value_with_checksum()
        }
        definition_steps = (
            ActionDefinition(ActionName("get_definition"), ActionType.SERVICE, None),
            ActionDefinition(EXECUTE_DEFINITION_ACTION.name, EXECUTE_DEFINITION_ACTION.type, None),
            ActionDefinition(ActionName("add_task_result_to_history"), ActionType.SERVICE, add_task_result_to_history_config),
        )
        definition = Definition(definition_input_data, definition_steps)
        input = ExecuteDefinitionInput(execution_id, definition)
        return ActionData(data.run_id, data.step_id, None, input, data.metadata)
    def ok_to_none(_):
        # Task started and will complete eventually. Return None to properly handle ongoing execute task action.
        return None
    def error_to_completed_result(error):
        # Task failed to start. Return CompletedWith.Data result to complete execute task action.
        error_result_dict = CompletedResultAdapter.to_dict(CompletedWith.Error(str(error)))
        return CompletedWith.Data(error_result_dict)

    task_res = await get_task(data.input)
    execute_definition_data_res = task_res.map(to_execute_definition_data)
    execute_definition_res = await lift_param(execute_definition)(execute_definition_data_res)
    return execute_definition_res.map(ok_to_none).default_with(error_to_completed_result)

# if __name__ == "__main__":
#     asyncio.run(app.run())