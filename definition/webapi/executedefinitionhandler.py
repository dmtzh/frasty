from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Concatenate, ParamSpec, TypeVar

from expression import Result

from shared.customtypes import DefinitionIdValue, RunIdValue, StepIdValue
from shared.definition import Definition
from shared.runningdefinition import RunningDefinitionState

P = ParamSpec("P")
R = TypeVar("R")
TCfg = TypeVar("TCfg")
D = TypeVar("D")

ToStorageActionConverter = Callable[[Callable[Concatenate[RunningDefinitionState | None, P], tuple[R, RunningDefinitionState]]], Callable[Concatenate[RunIdValue, DefinitionIdValue, P], Coroutine[Any, Any, R]]]

@dataclass(frozen=True)
class ExecuteDefinitionCommand:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    definition: Definition

async def handle(
        convert_to_storage_action: ToStorageActionConverter[P, R],
        run_first_step_handler: Callable[[RunningDefinitionState.Events.StepRunning], Coroutine[Any, Any, Result]],
        cmd: ExecuteDefinitionCommand):
    evt = RunningDefinitionState.Events.StepRunning(StepIdValue.new_id(), cmd.definition.steps[0], None)
    return Result.Ok(evt)