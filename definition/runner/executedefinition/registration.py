from typing import Any

from expression import Result

from shared.completedresult import CompletedResultAdapter, CompletedWith
from shared.executedefinitionaction import EXECUTE_DEFINITION_ACTION, ExecuteDefinitionInput, ExecuteGroupOfDefinitionsInput
from shared.groupofrunningdefinitions import GroupOfRunningDefinitionsState
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, AsyncActionHandler, DataDto, RunAsyncAction

from config import running_definitions_storage, group_of_running_definitions_storage

from .groupofdefinitionshandler import RunGroupOfDefinitionsStorageError, CompleteFailedDefinitionStorageError, handle as handle_execute_group_of_definitions
from .singledefinitionhandler import handle as handle_execute_single_definition

def register_execute_definition_action_handler(run_action: RunAsyncAction, action_handler: AsyncActionHandler):
    async def handle_execute_definition_action(data: ActionData[None, ExecuteDefinitionInput | ExecuteGroupOfDefinitionsInput]):
        match data.input:
            case ExecuteDefinitionInput():
                action_data = ActionData(data.run_id, data.step_id, data.config, data.input, data.metadata)
                execute_single_definition_res = await handle_execute_single_definition(running_definitions_storage.with_storage, run_action, action_data)
                return _result_to_execute_definition_action_handler_result(execute_single_definition_res)
            case ExecuteGroupOfDefinitionsInput():
                action_data = ActionData(data.run_id, data.step_id, data.config, data.input, data.metadata)
                execute_group_of_definitions_res = await handle_execute_group_of_definitions(group_of_running_definitions_storage.with_storage, run_action, action_data)
                return _group_result_to_execute_definition_action_handler_result(execute_group_of_definitions_res)
    
    return ActionHandlerFactory(run_action, action_handler).create_without_config(
        EXECUTE_DEFINITION_ACTION,
        execute_definition_handler_input_validator
    )(handle_execute_definition_action)

def execute_definition_handler_input_validator(dto_list: list[DataDto]):
    def parse_has_items() -> Result[list[DataDto], str]:
        return Result.Ok(dto_list) if dto_list else Result.Error("definition is missing")
    def parse_execute_definition_input(dto_list: list[DataDto]) -> Result[ExecuteDefinitionInput | ExecuteGroupOfDefinitionsInput, str]:
        match dto_list:
            case [single_item]:
                return ExecuteDefinitionInput.from_dict(single_item)
            case [*multiple_items]:
                return ExecuteGroupOfDefinitionsInput.from_list(multiple_items)
    dto_items_res = parse_has_items()
    execute_definition_input_res = dto_items_res.bind(parse_execute_definition_input)
    return execute_definition_input_res

def _result_to_execute_definition_action_handler_result(result: Result):
    def ok_to_none(_):
        # Definition started and will complete eventually. Return None to properly handle ongoing execute definition action.
        return None
    def err_to_completed_result(err):
        # Definition failed to start. Return CompletedWith.Data result to complete execute definition action.
        error_result_dict = CompletedResultAdapter.to_dict(CompletedWith.Error(str(err)))
        return CompletedWith.Data(error_result_dict)
    return result\
        .map(ok_to_none)\
        .map_error(err_to_completed_result)\
        .merge()

def _group_result_to_execute_definition_action_handler_result(result: Result[Any, RunGroupOfDefinitionsStorageError | list[CompleteFailedDefinitionStorageError]]):
    def ok_to_none_or_completed_result(res):
        match res:
            case GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted():
                # Group of definitions completed.
                all_results = [{"definition_id": def_res.definition_id.to_value_with_checksum()} | CompletedResultAdapter.to_dict(def_res.value) for def_res in res.results]
                return CompletedWith.Data(all_results)
            case _:
                # Group of definitions started and will complete eventually. Return None to properly handle ongoing execute group of definitions action.
                return None
    def err_to_completed_result(err : RunGroupOfDefinitionsStorageError | list[CompleteFailedDefinitionStorageError]):
        # Group of definitions failed to start. Return CompletedWith.Error result wrapped in CompletedWith.Data to complete execute group of definitions action.
        match err:
            case RunGroupOfDefinitionsStorageError():
                err_msg = f"Group of definitions failed to execute due to storage issues: {err.message}" 
                error_result_dict = CompletedWith.Error(err_msg)
                return CompletedWith.Data(error_result_dict)
            case list() as errs:
                storage_err_msg = ", ".join(err.message for err in errs)
                err_msg = f"Group of definitions failed to complete due to storage issues: {storage_err_msg}"
                error_result_dict = CompletedWith.Error(err_msg)
                return CompletedWith.Data(error_result_dict)
    return result\
        .map(ok_to_none_or_completed_result)\
        .map_error(err_to_completed_result)\
        .merge()