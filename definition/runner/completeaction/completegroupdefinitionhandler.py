import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Concatenate, NamedTuple
import weakref

from expression import Result

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Error, RunIdValue, StepIdValue
from shared.definitioncustomtypes import GroupIdValue
from shared.groupofrunningdefinitions import GroupOfRunningDefinitionsState
from shared.infrastructure.storage.repository import NotFoundException, StorageError
from shared.utils.asyncresult import AsyncResult
from shared.utils.exceptiondecorators import async_ex_to_error_result

type ToStorageActionConverter[**P] = Callable[[Callable[Concatenate[GroupOfRunningDefinitionsState | None, P], tuple[GroupOfRunningDefinitionsState.Events.Event | None, GroupOfRunningDefinitionsState]]], Callable[Concatenate[RunIdValue, GroupIdValue, P], Coroutine[Any, Any, GroupOfRunningDefinitionsState.Events.Event | None]]]

class CompleteGroupDefinitionCommand(NamedTuple):
    run_id: RunIdValue
    group_id: GroupIdValue
    step_id: StepIdValue
    definition_id: DefinitionIdValue
    result: CompletedResult

async def handle(
    convert_to_storage_action: ToStorageActionConverter,
    event_handler: Callable[[GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted], Coroutine[Any, Any, Result]],
    cmd: CompleteGroupDefinitionCommand
):
    @async_ex_to_error_result(StorageError.from_exception)
    @convert_to_storage_action
    def apply_fail_event_handler(state: GroupOfRunningDefinitionsState | None, error: Any):
        if state is None:
            raise NotFoundException()
        evt = state.apply_command(GroupOfRunningDefinitionsState.Commands.Fail(Error.from_error(error)))
        return (evt, state)
    def map_errors(error: CompleteGroupDefinitionStorageError | _EventHandlerError):
        match error:
            case _EventHandlerError(GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted(), error=evt_error):
                return CompletAllDefinitionsError(evt_error)
            case _:
                return error
    
    res = await _complete_group_definition_workflow(
        convert_to_storage_action,
        event_handler,
        cmd
    )
    opt_error = res.swap().default_value(None)
    match opt_error:
        case _EventHandlerError(event=GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted(), error=evt_error):
            await apply_fail_event_handler(cmd.run_id, cmd.group_id, evt_error)
    return res.map_error(map_errors)

class CompleteGroupDefinitionStorageError(StorageError):
    '''Unexpected complete group definition storage error'''

@dataclass(frozen=True)
class CompletAllDefinitionsError:
    error: Any

@dataclass(frozen=True)
class _EventHandlerError:
    event: GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted
    error: Any

class _KeyedLock:
    """Manages dynamic asyncio locks based on a GroupIdValue key."""
    def __init__(self):
        # WeakValueDictionary automatically removes keys when locks are no longer in use
        self._locks = weakref.WeakValueDictionary()
        self._master_lock = None

    async def get_lock(self, key: GroupIdValue) -> asyncio.Lock:
        if self._master_lock is None:
            self._master_lock = asyncio.Lock()
            
        async with self._master_lock:
            # 1. Try to fetch the lock safely using .get() to avoid throwing a KeyError
            lock = self._locks.get(key)
            
            # 2. If it doesn't exist, create it and store a strong local reference
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
                
            # 3. Returning 'lock' (the strong reference) ensures Python 
            # won't garbage-collect it until the calling function captures it.
            return lock

_lock_manager = _KeyedLock()

def _complete_group_definition_workflow(
    convert_to_storage_action: ToStorageActionConverter,
    event_handler: Callable[[GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted], Coroutine[Any, Any, Result]],
    cmd: CompleteGroupDefinitionCommand
):
    @async_ex_to_error_result(CompleteGroupDefinitionStorageError.from_exception)
    @async_ex_to_error_result(lambda _: CompleteGroupDefinitionStorageError(f"State not found for run_id {cmd.run_id} and group_id {cmd.group_id}"), NotFoundException)
    @convert_to_storage_action
    def apply_complete_definition(state: GroupOfRunningDefinitionsState | None):
        if state is None:
            raise NotFoundException()
        complete_def_cmd = GroupOfRunningDefinitionsState.Commands.CompleteDefinition(cmd.step_id, cmd.definition_id, cmd.result)
        evt = state.apply_command(complete_def_cmd)
        return (evt, state)
    async def handle_all_definitions_completed(opt_evt: GroupOfRunningDefinitionsState.Events.Event | None):
        match opt_evt:
            case GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted() as all_defs_completed:
                event_handler_res = await event_handler(all_defs_completed)
                return event_handler_res\
                    .map(lambda _: opt_evt) \
                    .map_error(lambda err: _EventHandlerError(all_defs_completed, err))
            case _:
                return Result[GroupOfRunningDefinitionsState.Events.Event | None, _EventHandlerError].Ok(opt_evt)

    async def apply_complete_definition_wrapper(run_id: RunIdValue, group_id: GroupIdValue):
        lock = await _lock_manager.get_lock(group_id)
        async with lock:
            res = await apply_complete_definition(run_id, group_id)
        return res
    opt_evt_res = AsyncResult(apply_complete_definition_wrapper(cmd.run_id, cmd.group_id))
    res = opt_evt_res.bind(handle_all_definitions_completed)
    return res.to_coroutine()
