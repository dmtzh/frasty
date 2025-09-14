from collections.abc import Generator
from enum import StrEnum
import functools
from typing import Any, Optional

from expression import Result, effect
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from .domainrunning import RunningDefinitionState
from shared.completedresult import CompletedResultAdapter
from shared.customtypes import IdValue, Error, StepIdValue
import shared.dtodefinition as shdtodef
from shared.utils.string import strip_and_lowercase

class RunningDefinitionStateEventDtoTypes(StrEnum):
    DEFINITION_ADDED = RunningDefinitionState.Events.DefinitionAdded.__name__.lower()
    STEP_RUNNING = RunningDefinitionState.Events.StepRunning.__name__.lower()
    STEP_CANCELED = RunningDefinitionState.Events.StepCanceled.__name__.lower()
    STEP_FAILED = RunningDefinitionState.Events.StepFailed.__name__.lower()
    STEP_COMPLETED = RunningDefinitionState.Events.StepCompleted.__name__.lower()
    DEFINITION_COMPLETED = RunningDefinitionState.Events.DefinitionCompleted.__name__.lower()
    FAILED = RunningDefinitionState.Events.Failed.__name__.lower()

    @staticmethod
    def parse(event_type: str) -> Optional["RunningDefinitionStateEventDtoTypes"]:
        if event_type is None:
            return None
        match strip_and_lowercase(event_type):
            case RunningDefinitionStateEventDtoTypes.DEFINITION_ADDED:
                return RunningDefinitionStateEventDtoTypes.DEFINITION_ADDED
            case RunningDefinitionStateEventDtoTypes.STEP_RUNNING:
                return RunningDefinitionStateEventDtoTypes.STEP_RUNNING
            case RunningDefinitionStateEventDtoTypes.STEP_CANCELED:
                return RunningDefinitionStateEventDtoTypes.STEP_CANCELED
            case RunningDefinitionStateEventDtoTypes.STEP_FAILED:
                return RunningDefinitionStateEventDtoTypes.STEP_FAILED
            case RunningDefinitionStateEventDtoTypes.STEP_COMPLETED:
                return RunningDefinitionStateEventDtoTypes.STEP_COMPLETED
            case RunningDefinitionStateEventDtoTypes.DEFINITION_COMPLETED:
                return RunningDefinitionStateEventDtoTypes.DEFINITION_COMPLETED
            case RunningDefinitionStateEventDtoTypes.FAILED:
                return RunningDefinitionStateEventDtoTypes.FAILED
            case _:
                return None

class RunningDefinitionStateEventAdapter:
    @effect.result[RunningDefinitionState.Events.Event, str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, RunningDefinitionState.Events.Event]:
        raw_event_dict = yield from Result.Ok(data) if isinstance(data, dict) and data else Result.Error("data is invalid")
        raw_event_type = yield from Result.Ok(raw_event_dict["type"]) if "type" in raw_event_dict else Result.Error("event type is missing")
        event_type = RunningDefinitionStateEventDtoTypes.parse(raw_event_type)
        match event_type:
            case RunningDefinitionStateEventDtoTypes.DEFINITION_ADDED:
                raw_definition = yield from Result.Ok(raw_event_dict["definition"]) if "definition" in raw_event_dict else Result.Error("definition is missing")
                definition = yield from shdtodef.DefinitionAdapter.from_list(raw_definition).map_error(str)
                return RunningDefinitionState.Events.DefinitionAdded(
                    definition=definition
                )
            case RunningDefinitionStateEventDtoTypes.STEP_RUNNING:
                raw_step_id = yield from Result.Ok(raw_event_dict["step_id"]) if "step_id" in raw_event_dict else Result.Error("step_id is missing")
                step_id = StepIdValue(raw_step_id)
                raw_step_definition = yield from Result.Ok(raw_event_dict["step_definition"]) if "step_definition" in raw_event_dict else Result.Error("step_definition is missing")
                step_definition = yield from shdtodef.StepDefinitionAdapter.from_dict(raw_step_definition).map_error(str)
                return RunningDefinitionState.Events.StepRunning(
                    step_id=step_id,
                    step_definition=step_definition,
                    input_data=None
                )
            case RunningDefinitionStateEventDtoTypes.STEP_CANCELED:
                raw_step_id = yield from Result.Ok(raw_event_dict["step_id"]) if "step_id" in raw_event_dict else Result.Error("step_id is missing")
                step_id = IdValue(raw_step_id)
                return RunningDefinitionState.Events.StepCanceled(step_id=step_id)
            case RunningDefinitionStateEventDtoTypes.STEP_FAILED:
                raw_step_id = yield from Result.Ok(raw_event_dict["step_id"]) if "step_id" in raw_event_dict else Result.Error("step_id is missing")
                step_id = IdValue(raw_step_id)
                raw_error = yield from Result.Ok(raw_event_dict["error"]) if "error" in raw_event_dict else Result.Error("error is missing")
                error = Error(raw_error)
                return RunningDefinitionState.Events.StepFailed(
                    step_id=step_id,
                    error=error
                )
            case RunningDefinitionStateEventDtoTypes.STEP_COMPLETED:
                raw_step_id = yield from Result.Ok(raw_event_dict["step_id"]) if "step_id" in raw_event_dict else Result.Error("step_id is missing")
                step_id = IdValue(raw_step_id)
                raw_result = yield from Result.Ok(raw_event_dict["result"]) if "result" in raw_event_dict else Result.Error("result is missing")
                result = yield from CompletedResultAdapter.from_dict(raw_result)
                return RunningDefinitionState.Events.StepCompleted(
                    step_id=step_id,
                    result=result
                )
            case RunningDefinitionStateEventDtoTypes.DEFINITION_COMPLETED:
                raw_result = yield from Result.Ok(raw_event_dict["result"]) if "result" in raw_event_dict else Result.Error("result is missing")
                result = yield from CompletedResultAdapter.from_dict(raw_result)
                return RunningDefinitionState.Events.DefinitionCompleted(result=result)
            case RunningDefinitionStateEventDtoTypes.FAILED:
                raw_error = yield from Result.Ok(raw_event_dict["error"]) if "error" in raw_event_dict else Result.Error("error is missing")
                error = Error(raw_error)
                return RunningDefinitionState.Events.Failed(
                    error=error
                )
            case _:
                yield from Result.Error(f"event type {raw_event_type} is invalid")
                raise RuntimeError("event type is invalid")
    
    @staticmethod
    def to_dict(evt: RunningDefinitionState.Events.Event) -> dict[str, Any]:
        match evt:
            case RunningDefinitionState.Events.DefinitionAdded(definition=definition):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.DEFINITION_ADDED.value,
                    "definition": shdtodef.DefinitionAdapter.to_list(definition)
                }
            case RunningDefinitionState.Events.StepRunning(step_id=step_id, step_definition=step_definition):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.STEP_RUNNING.value,
                    "step_id": step_id,
                    "step_definition": shdtodef.StepDefinitionAdapter.to_dict(step_definition)
                }
            case RunningDefinitionState.Events.StepCanceled(step_id=step_id):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.STEP_CANCELED.value,
                    "step_id": step_id
                }
            case RunningDefinitionState.Events.StepFailed(step_id=step_id, error=error):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.STEP_FAILED.value,
                    "step_id": step_id,
                    "error": error.message
                }
            case RunningDefinitionState.Events.StepCompleted(step_id=step_id, result=result):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.STEP_COMPLETED.value,
                    "step_id": step_id,
                    "result": CompletedResultAdapter.to_dict(result)
                }
            case RunningDefinitionState.Events.DefinitionCompleted(result=result):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.DEFINITION_COMPLETED.value,
                    "result": CompletedResultAdapter.to_dict(result)
                }
            case RunningDefinitionState.Events.Failed(error=error):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.FAILED.value,
                    "error": error.message
                }

class RunningDefinitionStateAdapter:
    @effect.result[RunningDefinitionState, str]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, RunningDefinitionState]:
        raw_events = yield from Result.Ok(data) if isinstance(data, list) and data else Result.Error("data is invalid")
        events = list((yield from traverse(RunningDefinitionStateEventAdapter.from_dict, Block(raw_events))))
        res = functools.reduce(RunningDefinitionState.apply, events, RunningDefinitionState())
        return res
    
    @staticmethod
    def to_list(state: RunningDefinitionState):
        return list(RunningDefinitionStateEventAdapter.to_dict(evt) for evt in state.get_events())