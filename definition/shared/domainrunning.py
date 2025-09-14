from __future__ import annotations
from dataclasses import dataclass
from typing import Any

from shared.customtypes import Error, IdValue, StepIdValue
import shared.domaindefinition as shdomaindef
from shared.completedresult import CompletedWith, CompletedResult

class RunningDefinitionState:
    class Commands:
        class Command:
            pass
        @dataclass(frozen=True)
        class SetDefinition(Command):
            definition: shdomaindef.Definition
        class RunFirstStep(Command):
            pass
        @dataclass(frozen=True)
        class FailRunningStep(Command):
            error: Error
        class CancelRunningStep(Command):
            pass
        @dataclass(frozen=True)
        class CompleteRunningStep(Command):
            result: CompletedResult
        class RunNextStep(Command):
            pass
        @dataclass(frozen=True)
        class Fail(Command):
            error: Error
    class Events:
        type Event = DefinitionAdded | StepRunning | StepCanceled | StepFailed | StepCompleted | DefinitionCompleted | Failed
        @dataclass(frozen=True)
        class DefinitionAdded:
            definition: shdomaindef.Definition
        @dataclass(frozen=True)
        class StepRunning:
            step_id: StepIdValue
            step_definition: shdomaindef.StepDefinition
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
            step_id: IdValue
            result: CompletedResult
        @dataclass(frozen=True)
        class DefinitionCompleted:
            result: CompletedResult
        @dataclass(frozen=True)
        class Failed:
            error: Error
    
    @staticmethod
    def apply(state: RunningDefinitionState, evt: RunningDefinitionState.Events.Event) -> RunningDefinitionState:
        state._events += (evt,)
        match evt:
            case RunningDefinitionState.Events.StepRunning(step_id, _):
                state._running_step_id = step_id
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
                apply_evt = RunningDefinitionState.Events.StepRunning(step_id, definition.steps[0], None)
                RunningDefinitionState.apply(self, apply_evt)
                evt = RunningDefinitionState.Events.StepRunning(step_id, definition.steps[0], definition.input_data)
                return evt
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
            case RunningDefinitionState.Commands.CompleteRunningStep(result=result):
                running_step_id = self.running_step_id()
                if running_step_id is None:
                    return None
                evt = RunningDefinitionState.Events.StepCompleted(running_step_id, result)
                RunningDefinitionState.apply(self, evt)
                return evt
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
                    step_def = definition.steps[num_of_completed_steps]
                    apply_evt = RunningDefinitionState.Events.StepRunning(step_id, step_def, None)
                    RunningDefinitionState.apply(self, apply_evt)
                    evt = RunningDefinitionState.Events.StepRunning(step_id, step_def, recent_step_completed_output.data)
                    return evt
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
        self._running_step_id: IdValue | None = None

    def recent_completed_step_id(self) -> IdValue | None:
        return self._recent_completed_step_id
    
    def running_step_id(self) -> IdValue | None:
        return self._running_step_id
    
    def get_events(self):
        return self._events