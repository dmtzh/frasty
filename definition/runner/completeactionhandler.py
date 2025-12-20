from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue, StepIdValue
from shared.runningdefinition import RunningDefinitionState

@dataclass(frozen=True)
class CompleteActionCommand:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    step_id: StepIdValue
    result: CompletedResult

ToStorageActionConverter = Callable[[Callable[[RunningDefinitionState | None], tuple[RunningDefinitionState.Events.Event | None, RunningDefinitionState]]], Callable[[RunIdValue, DefinitionIdValue], Coroutine[Any, Any, RunningDefinitionState.Events.Event | None]]]

async def handle(
        convert_to_storage_action: ToStorageActionConverter,
        event_handler: Callable[[RunningDefinitionState.Events.StepRunning | RunningDefinitionState.Events.DefinitionCompleted], Coroutine[Any, Any, Result]],
        cmd: CompleteActionCommand):
    return Result.Ok(None)
