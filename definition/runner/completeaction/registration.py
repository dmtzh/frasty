import functools

from expression import Result

from shared.completedresult import CompletedResult, CompletedResultAdapter, CompletedWith
from shared.pipeline.actionhandler import COMPLETE_ACTION, ActionData, ActionInput, ActionHandlerFactory, AsyncActionHandler, DataDtoAdapter, RunAsyncAction
from shared.runningdefinition import RunningDefinitionState

from config import running_definitions_storage
from runningparentaction import RunningParentAction

from .completedefinitionactionhandler import CompleteActionCommand, handle as handle_complete_definition_action

type CompleteInput = CompletedResult

def register_complete_action_handler(run_action: RunAsyncAction, action_handler: AsyncActionHandler):
    async def handle_complete_action(data: ActionData[None, CompleteInput]):
        match data.metadata.get_definition_id():
            case None:
                return None
            case definition_id:
                # definition id is required for complete action
                event_handler = functools.partial(_event_handler, run_action, data)
                cmd = CompleteActionCommand(data.run_id, definition_id, data.step_id, data.input)
                complete_res = await handle_complete_definition_action(running_definitions_storage.with_storage, event_handler, cmd)
                opt_complete_error = complete_res.swap().default_value(None)
                if opt_complete_error is not None:
                    await handle_complete_error(opt_complete_error, data)
                return None
    async def handle_complete_error(complete_error, data: ActionData[None, CompleteInput]):
        opt_parent_action = RunningParentAction.parse(data.metadata)
        if opt_parent_action is not None:
            error_result = CompletedWith.Error(str(complete_error))
            await opt_parent_action.run_complete_definition(run_action, error_result)
    async def do_nothing_when_run_action(action_name: str, action_input: ActionInput):
        return Result.Ok(None)
    
    return ActionHandlerFactory(do_nothing_when_run_action, action_handler).create_without_config(
        COMPLETE_ACTION,
        lambda data: CompletedResultAdapter.from_dict(data[0]) if data else Result.Error("input data is missing")
    )(handle_complete_action)

async def _event_handler(run_action: RunAsyncAction, data: ActionData[None, CompleteInput], evt: RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted):
    match evt:
        case RunningDefinitionState.Events.StepRunning():
            data_dict = DataDtoAdapter.to_input_data(evt.input_data) | (evt.step_definition.config or {})
            action_input = ActionInput(data.run_id.to_value_with_checksum(), evt.step_id.to_value_with_checksum(), data_dict, data.metadata.to_dict())
            return await run_action(evt.step_definition.get_name(), action_input)
        case RunningDefinitionState.Events.DefinitionCompleted():
            opt_parent_action = RunningParentAction.parse(data.metadata)
            match opt_parent_action:
                case None:
                    return Result.Ok(None)
                case parent_action_no_def_id if parent_action_no_def_id.metadata.get_definition_id() is None:
                    return Result.Ok(None)
                case parent_action_with_def_id:
                    return await parent_action_with_def_id.run_complete_definition(run_action, evt.result)