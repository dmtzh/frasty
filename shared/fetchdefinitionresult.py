import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from expression import Result

from shared.action import Action, ActionName, ActionType
from shared.completedresult import CompletedResult, CompletedResultAdapter, CompletedWith
from shared.customtypes import DefinitionIdValue, Error, IdValue, Metadata, RunIdValue, StepIdValue
from shared.definition import ActionDefinition, Definition
from shared.executedefinitionaction import EXECUTE_DEFINITION_ACTION, ExecuteDefinitionInput, run_execute_definition_action
from shared.infrastructure.storage.inmemory import InMemory
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, AsyncActionHandler, DataDto, RunAsyncAction

type FetchDefinitionResultFunc = Callable[[RunIdValue, Definition], Coroutine[Any, Any, Result[CompletedResult, RunDefinitionError | MissingResultError | InvalidResultError]]]

class RunDefinitionError(Error):
    '''Run definition error'''
class MissingResultError(Error):
    '''Missing result error'''
class InvalidResultError(Error):
    '''Invalid result error'''

class CallbackId(IdValue):
    '''Callback id'''

class FetchDefinitionResult:
    def __init__(self, run_action: RunAsyncAction, action_handler: AsyncActionHandler):
        self._run_action = run_action
        self._callbacks_repo = InMemory[DefinitionIdValue, asyncio.Future]()
        callback_id = CallbackId.new_id()
        self._callback_action = Action(ActionName(f"fetch_definition_result_callback_{callback_id}".lower()), ActionType.SERVICE)
        ActionHandlerFactory(run_action, action_handler).create_without_config(
            self._callback_action,
            Result.Ok
        )(self._handle_fetch_definition_result_callback)

    async def _handle_fetch_definition_result_callback(self, data: ActionData[None, list[DataDto]]):
        match data.metadata.get_definition_id():
            case None:
                return CompletedWith.Error("definition id is missing")
            case def_id:
                match self._callbacks_repo.get(def_id):
                    case None:
                        return CompletedWith.Error(f"definition id {def_id.to_value_with_checksum()} not found")
                    case future:
                        future.set_result(data.input)
                        self._callbacks_repo.delete(def_id)
                        return CompletedWith.Data(def_id.to_value_with_checksum())

    async def __call__(self, run_id: RunIdValue, definition: Definition) -> Result[CompletedResult, RunDefinitionError | MissingResultError | InvalidResultError]:
        proxy_definition_input_data = [ExecuteDefinitionInput(DefinitionIdValue.new_id(), definition).to_dict()]
        proxy_definition_steps = (
            ActionDefinition(EXECUTE_DEFINITION_ACTION.name, EXECUTE_DEFINITION_ACTION.type, None),
            ActionDefinition(self._callback_action.name, self._callback_action.type, None)
        )
        proxy_def_id = DefinitionIdValue.new_id()
        proxy_definition = Definition(proxy_definition_input_data, proxy_definition_steps)
        input = ExecuteDefinitionInput(proxy_def_id, proxy_definition)
        step_id = StepIdValue.new_id()
        metadata = Metadata()
        metadata.set_from("fetch definition result action")
        execute_proxy_definition_data = ActionData(run_id, step_id, None, input, metadata)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[list[DataDto]] = loop.create_future()
        run_res = await run_execute_definition_action(self._run_action, execute_proxy_definition_data)
        apply_add_res = run_res.map_error(RunDefinitionError.from_error).map(lambda _: self._callbacks_repo.add(proxy_def_id, future))
        async def get_result(_: None):
            res = await future
            match res:
                case []:
                    return Result.Error(MissingResultError("No result received"))
                case [head, *_]:
                    completed_res = CompletedResultAdapter.from_dict(head)
                    return completed_res.map_error(InvalidResultError)
                case invalid:
                    return Result.Error(InvalidResultError.from_error(invalid))
        async def error_func(err: RunDefinitionError):
            return Result.Error(err)
        compl_res = await apply_add_res.map(get_result).default_with(error_func)
        return compl_res