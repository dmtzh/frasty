from __future__ import annotations
from collections.abc import Generator
from dataclasses import dataclass
from enum import StrEnum
import functools
from typing import Any

from expression import Result, effect
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from shared.completedresult import CompletedResultAdapter
from shared.customtypes import Error, IdValue, StepIdValue
from shared.definition import AggregateActionDefinition, Definition, DefinitionAdapter, ActionDefinition, ActionDefinitionAdapter
from shared.completedresult import CompletedWith, CompletedResult
from shared.utils.string import strip_and_lowercase

@dataclass(frozen=True)
class _AggregateExecutionState:
    parent_step_id: StepIdValue
    pending_child_ids: frozenset[StepIdValue]
    child_results: dict[StepIdValue, CompletedResult]

    @staticmethod
    def from_events(events: tuple[RunningDefinitionState.Events.Event, ...]) -> _AggregateExecutionState | None:
        parent_id: StepIdValue | None = None
        pending: set[StepIdValue] = set()
        results: dict[StepIdValue, CompletedResult] = {}

        for evt in events:
            match evt:
                case RunningDefinitionState.Events.AggregateStepsRunning(parent_step_id=pid, child_running_events=children):
                    parent_id = pid
                    pending = {c.step_id for c in children}
                    results = {}
                case RunningDefinitionState.Events.ChildStepCompleted(step_id=sid, result=res) if sid in pending:
                    pending.discard(sid)
                    results[sid] = res
                case RunningDefinitionState.Events.StepCompleted(step_id=sid, result=_) if sid == parent_id:
                    parent_id = None
                    pending = set()
                    results = {}
                case RunningDefinitionState.Events.StepCanceled(step_id=sid) | \
                     RunningDefinitionState.Events.StepFailed(step_id=sid, error=_) if sid == parent_id:
                    parent_id = None
                    pending = set()
                    results = {}

        return _AggregateExecutionState(
            parent_step_id=parent_id,
            pending_child_ids=frozenset(pending),
            child_results=results
        ) if parent_id is not None else None

class RunningDefinitionState:
    class Commands:
        class Command:
            pass
        @dataclass(frozen=True)
        class SetDefinition(Command):
            definition: Definition
        class RunFirstStep(Command):
            pass
        @dataclass(frozen=True)
        class FailRunningStep(Command):
            error: Error
        class CancelRunningStep(Command):
            pass
        @dataclass(frozen=True)
        class CompleteRunningStep(Command):
            step_id: StepIdValue
            result: CompletedResult
        class RunNextStep(Command):
            pass
        @dataclass(frozen=True)
        class Fail(Command):
            error: Error
    class Events:
        type Event = DefinitionAdded | StepRunning | StepCanceled | StepFailed | StepCompleted | DefinitionCompleted | Failed | AggregateStepsRunning | ChildStepCompleted
        @dataclass(frozen=True)
        class DefinitionAdded:
            definition: Definition
        @dataclass(frozen=True)
        class StepRunning:
            step_id: StepIdValue
            step_definition: ActionDefinition
            input_data: Any
        @dataclass(frozen=True)
        class StepCanceled:
            step_id: IdValue
        @dataclass(frozen=True)
        class StepFailed:
            step_id: IdValue
            error: Error
        @dataclass(frozen=True)
        class StepCompleted:
            step_id: StepIdValue
            result: CompletedResult
        @dataclass(frozen=True)
        class DefinitionCompleted:
            result: CompletedResult
        @dataclass(frozen=True)
        class Failed:
            error: Error
        @dataclass(frozen=True)
        class AggregateStepsRunning:
            parent_step_id: StepIdValue
            child_running_events: list[RunningDefinitionState.Events.StepRunning]
        @dataclass(frozen=True)
        class ChildStepCompleted:
            step_id: StepIdValue
            result: CompletedResult
    
    @staticmethod
    def apply(state: RunningDefinitionState, evt: RunningDefinitionState.Events.Event) -> RunningDefinitionState:
        state._events += (evt,)
        match evt:
            case RunningDefinitionState.Events.StepRunning(step_id, _):
                state._running_step_id = step_id
            case RunningDefinitionState.Events.AggregateStepsRunning(parent_step_id, _):
                state._running_step_id = parent_step_id
            case RunningDefinitionState.Events.StepCanceled(_):
                state._running_step_id = None
            case RunningDefinitionState.Events.StepFailed(_, _):
                state._running_step_id = None
            case RunningDefinitionState.Events.StepCompleted(step_id, _):
                state._recent_completed_step_id = step_id
                state._running_step_id = None
        return state

    def apply_command(self, cmd: Commands.Command) -> Events.Event | None:
        match cmd:
            case RunningDefinitionState.Commands.SetDefinition(definition=definition):
                has_definition = any(self._events)
                if has_definition:
                    return None
                evt = RunningDefinitionState.Events.DefinitionAdded(definition)
                RunningDefinitionState.apply(self, evt)
                return evt
            case RunningDefinitionState.Commands.RunFirstStep():
                definition = self._events[0].definition if any(self._events) and type(self._events[0]) is RunningDefinitionState.Events.DefinitionAdded else None
                if definition is None:
                    return None
                if self.running_step_id() is not None:
                    return None
                if self.recent_completed_step_id() is not None:
                    return None
                step_id = StepIdValue.new_id()
                match definition.steps[0]:
                    case ActionDefinition() as step_def:
                        apply_evt = RunningDefinitionState.Events.StepRunning(step_id, step_def, None)
                        RunningDefinitionState.apply(self, apply_evt)
                        evt = RunningDefinitionState.Events.StepRunning(step_id, step_def, definition.input_data)
                        return evt
                    case AggregateActionDefinition() as agg_step_def:
                        if not definition.input_data:
                            raise ValueError(f"Aggregate step '{agg_step_def.name}' cannot be executed with an empty input_data")
                        parent_id = step_id
                        input_list = definition.input_data if isinstance(definition.input_data, list) else [definition.input_data]
                        child_step_def = ActionDefinition(agg_step_def.name, agg_step_def.type, agg_step_def.config)
                        child_events = [RunningDefinitionState.Events.StepRunning(StepIdValue.new_id(), child_step_def, None) for _ in input_list]
                        apply_evt = RunningDefinitionState.Events.AggregateStepsRunning(parent_id, child_events)
                        RunningDefinitionState.apply(self, apply_evt)
                        # Return external contract with actual input_data
                        child_running_events = [
                            RunningDefinitionState.Events.StepRunning(evt.step_id, evt.step_definition, input_data)
                            for evt, input_data in zip(child_events, input_list)
                        ]
                        return RunningDefinitionState.Events.AggregateStepsRunning(parent_id, child_running_events)
                    case unsupported_step_def:
                        raise NotImplementedError(f"Unsupported step type: {type(unsupported_step_def)}")
            case RunningDefinitionState.Commands.CancelRunningStep():
                running_step_id = self.running_step_id()
                if running_step_id is None:
                    return None
                evt = RunningDefinitionState.Events.StepCanceled(running_step_id)
                RunningDefinitionState.apply(self, evt)
                return evt
            case RunningDefinitionState.Commands.FailRunningStep(error=error):
                running_step_id = self.running_step_id()
                if running_step_id is None:
                    return None
                evt = RunningDefinitionState.Events.StepFailed(running_step_id, error)
                RunningDefinitionState.apply(self, evt)
                return evt
            case RunningDefinitionState.Commands.CompleteRunningStep(step_id=step_id, result=result):
                running_step_id = self.running_step_id()
                if running_step_id is None:
                    return None
                match self._get_aggregate_execution_state():
                    case None:
                        is_current_step_running = running_step_id == step_id
                        if not is_current_step_running:
                            return None
                        evt = RunningDefinitionState.Events.StepCompleted(running_step_id, result)
                        RunningDefinitionState.apply(self, evt)
                        return evt
                    case agg_state:
                        # Reject direct parent completion
                        if step_id == agg_state.parent_step_id:
                            return None
                        if step_id not in agg_state.pending_child_ids:
                            return None
                        # Complete child
                        child_completed_evt = RunningDefinitionState.Events.ChildStepCompleted(step_id, result)
                        RunningDefinitionState.apply(self, child_completed_evt)
                        updated_agg_state = self._get_aggregate_execution_state()
                        if updated_agg_state is None:
                            return None
                        has_more_running_childs = len(updated_agg_state.pending_child_ids) > 0
                        if has_more_running_childs:
                            return child_completed_evt
                        else:
                            # Auto-Completion
                            all_child_results = [CompletedResultAdapter.to_dict(child_res) for child_res in updated_agg_state.child_results.values()]
                            parent_res = CompletedWith.Data(all_child_results)
                            parent_completed_evt = RunningDefinitionState.Events.StepCompleted(updated_agg_state.parent_step_id, parent_res)
                            RunningDefinitionState.apply(self, parent_completed_evt)
                            return parent_completed_evt
            case RunningDefinitionState.Commands.RunNextStep():
                definition = self._events[0].definition if any(self._events) and type(self._events[0]) is RunningDefinitionState.Events.DefinitionAdded else None
                if definition is None:
                    return None
                if self.running_step_id() is not None:
                    return None
                if self.recent_completed_step_id() is None:
                    return None
                num_of_completed_steps = sum(1 for e in self._events if type(e) is RunningDefinitionState.Events.StepCompleted)
                has_more_steps = len(definition.steps) > num_of_completed_steps
                recent_step_completed_output = next((e.result for e in reversed(self._events) if type(e) is RunningDefinitionState.Events.StepCompleted))
                is_recent_step_completed_with_data = type(recent_step_completed_output) is CompletedWith.Data
                if has_more_steps and is_recent_step_completed_with_data:
                    step_id = StepIdValue.new_id()
                    match definition.steps[num_of_completed_steps]:
                        case ActionDefinition() as step_def:
                            apply_evt = RunningDefinitionState.Events.StepRunning(step_id, step_def, None)
                            RunningDefinitionState.apply(self, apply_evt)
                            evt = RunningDefinitionState.Events.StepRunning(step_id, step_def, recent_step_completed_output.data)
                            return evt
                        case AggregateActionDefinition() as agg_step_def:
                            if not recent_step_completed_output.data:
                                raise ValueError(f"Aggregate step '{agg_step_def.name}' cannot be executed with an empty input_data")
                            parent_id = step_id
                            input_list = recent_step_completed_output.data if isinstance(recent_step_completed_output.data, list) else [recent_step_completed_output.data]
                            child_step_def = ActionDefinition(agg_step_def.name, agg_step_def.type, agg_step_def.config)
                            child_events = [RunningDefinitionState.Events.StepRunning(StepIdValue.new_id(), child_step_def, None) for _ in input_list]
                            apply_evt = RunningDefinitionState.Events.AggregateStepsRunning(parent_id, child_events)
                            RunningDefinitionState.apply(self, apply_evt)
                            # Return external contract with actual input_data
                            child_running_events = [
                                RunningDefinitionState.Events.StepRunning(evt.step_id, evt.step_definition, input_data)
                                for evt, input_data in zip(child_events, input_list)
                            ]
                            return RunningDefinitionState.Events.AggregateStepsRunning(parent_id, child_running_events)
                        case unsupported_step_def:
                            raise NotImplementedError(f"Unsupported step type: {type(unsupported_step_def)}")
                else:
                    evt = RunningDefinitionState.Events.DefinitionCompleted(recent_step_completed_output)
                    RunningDefinitionState.apply(self, evt)
                    return evt
            case RunningDefinitionState.Commands.Fail(error=error):
                evt = RunningDefinitionState.Events.Failed(error)
                RunningDefinitionState.apply(self, evt)
                return evt
        raise ValueError(f"Unknown command {cmd}")
    
    def __init__(self):
        self._events: tuple[RunningDefinitionState.Events.Event, ...] = ()
        self._recent_completed_step_id: IdValue | None = None
        self._running_step_id: StepIdValue | None = None

    def recent_completed_step_id(self) -> IdValue | None:
        return self._recent_completed_step_id
    
    def running_step_id(self) -> StepIdValue | None:
        return self._running_step_id
    
    def get_events(self):
        return self._events
    
    def _get_aggregate_execution_state(self) -> _AggregateExecutionState | None:
        return _AggregateExecutionState.from_events(self._events)

class RunningDefinitionStateEventDtoTypes(StrEnum):
    DEFINITION_ADDED = RunningDefinitionState.Events.DefinitionAdded.__name__.lower()
    STEP_RUNNING = RunningDefinitionState.Events.StepRunning.__name__.lower()
    STEP_CANCELED = RunningDefinitionState.Events.StepCanceled.__name__.lower()
    STEP_FAILED = RunningDefinitionState.Events.StepFailed.__name__.lower()
    STEP_COMPLETED = RunningDefinitionState.Events.StepCompleted.__name__.lower()
    DEFINITION_COMPLETED = RunningDefinitionState.Events.DefinitionCompleted.__name__.lower()
    FAILED = RunningDefinitionState.Events.Failed.__name__.lower()
    AGGREGATE_STEPS_RUNNING = RunningDefinitionState.Events.AggregateStepsRunning.__name__.lower()
    CHILD_STEP_COMPLETED = RunningDefinitionState.Events.ChildStepCompleted.__name__.lower()

    @staticmethod
    def parse(event_type: str) -> RunningDefinitionStateEventDtoTypes | None:
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
            case RunningDefinitionStateEventDtoTypes.AGGREGATE_STEPS_RUNNING:
                return RunningDefinitionStateEventDtoTypes.AGGREGATE_STEPS_RUNNING
            case RunningDefinitionStateEventDtoTypes.CHILD_STEP_COMPLETED:
                return RunningDefinitionStateEventDtoTypes.CHILD_STEP_COMPLETED
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
                definition = yield from DefinitionAdapter.from_list(raw_definition).map_error(str)
                return RunningDefinitionState.Events.DefinitionAdded(
                    definition=definition
                )
            case RunningDefinitionStateEventDtoTypes.STEP_RUNNING:
                raw_step_id = yield from Result.Ok(raw_event_dict["step_id"]) if "step_id" in raw_event_dict else Result.Error("step_id is missing")
                step_id = StepIdValue(raw_step_id)
                raw_step_definition = yield from Result.Ok(raw_event_dict["step_definition"]) if "step_definition" in raw_event_dict else Result.Error("step_definition is missing")
                step_definition = yield from ActionDefinitionAdapter.from_dict(raw_step_definition).map_error(str)
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
                step_id = StepIdValue(raw_step_id)
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
            case RunningDefinitionStateEventDtoTypes.AGGREGATE_STEPS_RUNNING:
                raw_parent_step_id = yield from Result.Ok(raw_event_dict["parent_step_id"]) if "parent_step_id" in raw_event_dict else Result.Error("parent_step_id is missing")
                parent_step_id = StepIdValue(raw_parent_step_id)
                raw_child_running_events = yield from Result.Ok(raw_event_dict["child_running_events"]) if isinstance(raw_event_dict["child_running_events"], list) and raw_event_dict["child_running_events"] else Result.Error("child_running_events is missing")
                def dict_to_step_running(data: dict[str, Any]):
                    return RunningDefinitionStateEventAdapter.from_dict(data).bind(lambda evt: Result.Ok(evt) if isinstance(evt, RunningDefinitionState.Events.StepRunning) else Result.Error(f"expected {RunningDefinitionStateEventDtoTypes.STEP_RUNNING}, but got {raw_event_type}"))
                child_running_events = list((yield from traverse(dict_to_step_running, Block(raw_child_running_events))))
                return RunningDefinitionState.Events.AggregateStepsRunning(
                    parent_step_id=parent_step_id,
                    child_running_events=child_running_events
                )
            case RunningDefinitionStateEventDtoTypes.CHILD_STEP_COMPLETED:
                raw_step_id = yield from Result.Ok(raw_event_dict["step_id"]) if "step_id" in raw_event_dict else Result.Error("step_id is missing")
                step_id = StepIdValue(raw_step_id)
                raw_result = yield from Result.Ok(raw_event_dict["result"]) if "result" in raw_event_dict else Result.Error("result is missing")
                result = yield from CompletedResultAdapter.from_dict(raw_result)
                return RunningDefinitionState.Events.ChildStepCompleted(
                    step_id=step_id,
                    result=result
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
                    "definition": DefinitionAdapter.to_list(definition)
                }
            case RunningDefinitionState.Events.StepRunning(step_id=step_id, step_definition=step_definition):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.STEP_RUNNING.value,
                    "step_id": step_id,
                    "step_definition": ActionDefinitionAdapter.to_dict(step_definition)
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
            case RunningDefinitionState.Events.AggregateStepsRunning(parent_step_id, child_running_events):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.AGGREGATE_STEPS_RUNNING.value,
                    "parent_step_id": parent_step_id,
                    "child_running_events": [RunningDefinitionStateEventAdapter.to_dict(evt) for evt in child_running_events]
                }
            case RunningDefinitionState.Events.ChildStepCompleted(step_id=step_id, result=result):
                return {
                    "type": RunningDefinitionStateEventDtoTypes.CHILD_STEP_COMPLETED.value,
                    "step_id": step_id,
                    "result": CompletedResultAdapter.to_dict(result)
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