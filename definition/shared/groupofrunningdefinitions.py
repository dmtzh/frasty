from collections.abc import Generator
from dataclasses import dataclass
from enum import StrEnum
import functools
from typing import Any, NamedTuple

from expression import Result, effect
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from shared.completedresult import CompletedResult, CompletedResultAdapter
from shared.customtypes import DefinitionIdValue, Error, StepIdValue
from shared.definition import Definition, DefinitionAdapter
from shared.utils.parse import parse_from_dict, parse_value
from shared.utils.string import strip_and_lowercase

class DefinitionIdWithValue[T](NamedTuple):
    definition_id: DefinitionIdValue
    value: T

class RunningDefinition(NamedTuple):
    step_id: StepIdValue
    definition_id: DefinitionIdValue
    definition: Definition

class GroupOfRunningDefinitionsState:
    class Commands:
        class Command:
            pass
        @dataclass(frozen=True)
        class SetDefinitions(Command):
            definitions: tuple[DefinitionIdWithValue[Definition], ...]
        class RunDefinitions(Command):
            pass
        @dataclass(frozen=True)
        class CompleteDefinition(Command):
            step_id: StepIdValue
            definition_id: DefinitionIdValue
            result: CompletedResult
        @dataclass(frozen=True)
        class Fail(Command):
            error: Error
    class Events:
        type Event = DefinitionsAdded | DefinitionsRunning | DefinitionCompleted | AllDefinitionsCompleted | Failed
        @dataclass(frozen=True)
        class DefinitionsAdded:
            definitions: tuple[DefinitionIdWithValue[Definition], ...]
        @dataclass(frozen=True)
        class DefinitionsRunning:
            definitions: tuple[RunningDefinition, ...]
        @dataclass(frozen=True)
        class DefinitionCompleted:
            definition_id: DefinitionIdValue
            result: CompletedResult
        @dataclass(frozen=True)
        class AllDefinitionsCompleted:
            results: tuple[DefinitionIdWithValue[CompletedResult], ...]
        @dataclass(frozen=True)
        class Failed:
            error: Error
    
    def __init__(self):
        # Immutable event journal (source of truth)
        self._events: tuple[GroupOfRunningDefinitionsState.Events.Event, ...] = ()
        # Source of truth for available definitions
        self._definitions: tuple[DefinitionIdWithValue[Definition], ...] = ()
        # Projection of actively executing definitions
        self._running_definitions: tuple[DefinitionIdWithValue[StepIdValue], ...] = ()
        # Projection of finished definitions tracking their assigned step IDs
        self._completed_definitions: tuple[DefinitionIdWithValue[StepIdValue], ...] = ()
    
    @staticmethod
    def apply(state: "GroupOfRunningDefinitionsState", evt: Events.Event) -> "GroupOfRunningDefinitionsState":
        state._events += (evt,)
        match evt:
            case GroupOfRunningDefinitionsState.Events.DefinitionsAdded(definitions=defs):
                state._definitions = defs
            case GroupOfRunningDefinitionsState.Events.DefinitionsRunning(definitions=running_defs):
                # Convert domain RunningDefinition to lightweight tracking entries
                state._running_definitions = tuple(
                    DefinitionIdWithValue(rd.definition_id, rd.step_id) for rd in running_defs
                )
            case GroupOfRunningDefinitionsState.Events.DefinitionCompleted(definition_id=def_id, result=_):
                # Move running entry to completed projection to preserve step_id
                entry = next((e for e in state._running_definitions if e.definition_id == def_id), None)
                state._running_definitions = tuple(e for e in state._running_definitions if e.definition_id != def_id)
                if entry:
                    state._completed_definitions += (entry,)
            case GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted():
                # Terminal marker added to journal; no projection updates required
                pass
            case GroupOfRunningDefinitionsState.Events.Failed(_):
                # Terminal failure: clear active tracking
                state._running_definitions = ()
        return state
    
    def apply_command(self, cmd: Commands.Command) -> Events.Event | None:
        match cmd:
            case GroupOfRunningDefinitionsState.Commands.SetDefinitions(definitions=definitions):
                if self._definitions:
                    return None
                evt = GroupOfRunningDefinitionsState.Events.DefinitionsAdded(definitions)
                GroupOfRunningDefinitionsState.apply(self, evt)
                return evt

            case GroupOfRunningDefinitionsState.Commands.RunDefinitions():
                if not self._definitions or self._running_definitions:
                    return None
                running_defs = tuple(
                    RunningDefinition(StepIdValue.new_id(), def_id, def_val)
                    for def_id, def_val in self._definitions
                )
                evt = GroupOfRunningDefinitionsState.Events.DefinitionsRunning(running_defs)
                GroupOfRunningDefinitionsState.apply(self, evt)
                return evt

            case GroupOfRunningDefinitionsState.Commands.CompleteDefinition(step_id=step_id, definition_id=def_id, result=result):
                completion_evt = GroupOfRunningDefinitionsState.Events.DefinitionCompleted(def_id, result)
                
                # Idempotency guard: check if definition is already completed
                completed_entry = next((e for e in self._completed_definitions if e.definition_id == def_id), None)
                if completed_entry is not None:
                    # Strict step_id verification for idempotent calls
                    if completed_entry.value != step_id:
                        return None
                    # If group is fully completed, return terminal marker with filled results
                    if len(self._completed_definitions) == len(self._definitions):
                        # Reconstruct results from the event journal
                        all_results = tuple(
                            DefinitionIdWithValue(e.definition_id, e.result)
                            for e in self._events if type(e) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted
                        )
                        return GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted(all_results)
                    return completion_evt
                
                # Validate step_id against currently running definitions
                current_step = next((entry.value for entry in self._running_definitions if entry.definition_id == def_id), None)
                if current_step != step_id:
                    return None
                
                # Apply completion and update projections
                GroupOfRunningDefinitionsState.apply(self, completion_evt)

                # Check terminal condition: all definitions completed
                if len(self._completed_definitions) == len(self._definitions):
                    # Reconstruct results for the return value based on current state of events
                    all_results = tuple(
                        DefinitionIdWithValue(e.definition_id, e.result)
                        for e in self._events if type(e) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted
                    )
                    
                    # Add terminal marker to journal with empty results to save space
                    all_completed_evt = GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted(())
                    GroupOfRunningDefinitionsState.apply(self, all_completed_evt)
                    
                    # Return the event with filled results to the caller
                    return GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted(all_results)

                return completion_evt

            case GroupOfRunningDefinitionsState.Commands.Fail(error=error):
                if not self._running_definitions and not self._completed_definitions and not self._definitions:
                    return None
                evt = GroupOfRunningDefinitionsState.Events.Failed(error)
                GroupOfRunningDefinitionsState.apply(self, evt)
                return evt

        raise ValueError(f"Unknown command {cmd}")

    def get_events(self) -> tuple[Events.Event, ...]:
        return self._events

class GroupOfRunningDefinitionsStateEventDtoTypes(StrEnum):
    DEFINITIONS_ADDED = GroupOfRunningDefinitionsState.Events.DefinitionsAdded.__name__.lower()
    DEFINITIONS_RUNNING = GroupOfRunningDefinitionsState.Events.DefinitionsRunning.__name__.lower()
    DEFINITION_COMPLETED = GroupOfRunningDefinitionsState.Events.DefinitionCompleted.__name__.lower()
    ALL_DEFINITIONS_COMPLETED = GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted.__name__.lower()
    FAILED = GroupOfRunningDefinitionsState.Events.Failed.__name__.lower()

    @staticmethod
    def parse(event_type: str) -> "GroupOfRunningDefinitionsStateEventDtoTypes | None":
        if event_type is None:
            return None
        match strip_and_lowercase(event_type):
            case GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITIONS_ADDED:
                return GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITIONS_ADDED
            case GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITIONS_RUNNING:
                return GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITIONS_RUNNING
            case GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITION_COMPLETED:
                return GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITION_COMPLETED
            case GroupOfRunningDefinitionsStateEventDtoTypes.ALL_DEFINITIONS_COMPLETED:
                return GroupOfRunningDefinitionsStateEventDtoTypes.ALL_DEFINITIONS_COMPLETED
            case GroupOfRunningDefinitionsStateEventDtoTypes.FAILED:
                return GroupOfRunningDefinitionsStateEventDtoTypes.FAILED
            case _:
                return None

class GroupOfRunningDefinitionsStateEventAdapter:
    @effect.result[GroupOfRunningDefinitionsState.Events.Event, str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, GroupOfRunningDefinitionsState.Events.Event]:
        raw_event_dict = yield from Result.Ok(data) if isinstance(data, dict) and data else Result.Error("data is invalid")
        raw_event_type = yield from Result.Ok(raw_event_dict["type"]) if "type" in raw_event_dict else Result.Error("event type is missing")
        event_type = GroupOfRunningDefinitionsStateEventDtoTypes.parse(raw_event_type)

        match event_type:
            case GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITIONS_ADDED:
                raw_defs = yield from parse_from_dict(raw_event_dict, "definitions", lambda raw_defs: raw_defs if isinstance(raw_defs, list) and raw_defs else None)
                @effect.result[DefinitionIdWithValue[Definition], str]()
                def parse_definition_with_id(raw_data)-> Generator[Any, Any, DefinitionIdWithValue[Definition]]:
                    raw_def_with_id = yield from parse_value(raw_data, "definitions", lambda raw_data: raw_data if isinstance(raw_data, dict) and "definition_id" in raw_data and "definition" in raw_data else None)
                    def_id = yield from parse_from_dict(raw_def_with_id, "definition_id", DefinitionIdValue.from_value)
                    definition = yield from DefinitionAdapter.from_list(raw_def_with_id["definition"]).map_error(str)
                    return DefinitionIdWithValue(def_id, definition)
                defs = yield from traverse(parse_definition_with_id, Block(raw_defs)).map(tuple)
                return GroupOfRunningDefinitionsState.Events.DefinitionsAdded(defs)

            case GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITIONS_RUNNING:
                raw_defs = yield from parse_from_dict(raw_event_dict, "definitions", lambda raw_defs: raw_defs if isinstance(raw_defs, list) and raw_defs else None)
                @effect.result[RunningDefinition, str]()
                def parse_running_definition(raw_data)-> Generator[Any, Any, RunningDefinition]:
                    raw_running_def = yield from parse_value(raw_data, "definitions", lambda raw_data: raw_data if isinstance(raw_data, dict) and "step_id" in raw_data and "definition_id" in raw_data and "definition" in raw_data else None)
                    step_id = yield from parse_from_dict(raw_running_def, "step_id", StepIdValue.from_value)
                    def_id = yield from parse_from_dict(raw_running_def, "definition_id", DefinitionIdValue.from_value)
                    definition = yield from DefinitionAdapter.from_list(raw_running_def["definition"]).map_error(str)
                    return RunningDefinition(step_id, def_id, definition)
                running_defs = yield from traverse(parse_running_definition, Block(raw_defs)).map(tuple)
                return GroupOfRunningDefinitionsState.Events.DefinitionsRunning(running_defs)

            case GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITION_COMPLETED:
                def_id = yield from parse_from_dict(raw_event_dict, "definition_id", DefinitionIdValue.from_value)
                raw_result = yield from parse_from_dict(raw_event_dict, "result", lambda raw_result: raw_result if isinstance(raw_result, dict) else None)
                result = yield from CompletedResultAdapter.from_dict(raw_result)
                return GroupOfRunningDefinitionsState.Events.DefinitionCompleted(def_id, result)

            case GroupOfRunningDefinitionsStateEventDtoTypes.ALL_DEFINITIONS_COMPLETED:
                # Accepts empty tuple as per domain contract
                return GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted(())

            case GroupOfRunningDefinitionsStateEventDtoTypes.FAILED:
                error = yield from parse_from_dict(raw_event_dict, "error", Error.from_error)
                return GroupOfRunningDefinitionsState.Events.Failed(error)

            case _:
                yield from Result.Error(f"event type {raw_event_type} is invalid")
                raise RuntimeError("event type is invalid")

    @staticmethod
    def to_dict(evt: GroupOfRunningDefinitionsState.Events.Event) -> dict[str, Any]:
        match evt:
            case GroupOfRunningDefinitionsState.Events.DefinitionsAdded(definitions=defs):
                return {
                    "type": GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITIONS_ADDED.value,
                    "definitions": [
                        {"definition_id": d.definition_id, "definition": DefinitionAdapter.to_list(d.value)}
                        for d in defs
                    ]
                }
            case GroupOfRunningDefinitionsState.Events.DefinitionsRunning(definitions=running_defs):
                return {
                    "type": GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITIONS_RUNNING.value,
                    "definitions": [
                        {
                            "step_id": rd.step_id,
                            "definition_id": rd.definition_id,
                            "definition": DefinitionAdapter.to_list(rd.definition)
                        } for rd in running_defs
                    ]
                }
            case GroupOfRunningDefinitionsState.Events.DefinitionCompleted(definition_id=def_id, result=result):
                return {
                    "type": GroupOfRunningDefinitionsStateEventDtoTypes.DEFINITION_COMPLETED.value,
                    "definition_id": def_id,
                    "result": CompletedResultAdapter.to_dict(result)
                }
            case GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted():
                return {
                    "type": GroupOfRunningDefinitionsStateEventDtoTypes.ALL_DEFINITIONS_COMPLETED.value
                }
            case GroupOfRunningDefinitionsState.Events.Failed(error=error):
                return {
                    "type": GroupOfRunningDefinitionsStateEventDtoTypes.FAILED.value,
                    "error": error.message
                }

class GroupOfRunningDefinitionsStateAdapter:
    @effect.result[GroupOfRunningDefinitionsState, str]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, GroupOfRunningDefinitionsState]:
        raw_events = yield from parse_value(data, "data", lambda raw_data: raw_data if isinstance(raw_data, list) and raw_data else None)
        events = yield from traverse(GroupOfRunningDefinitionsStateEventAdapter.from_dict, Block(raw_events)).map(tuple)
        return functools.reduce(GroupOfRunningDefinitionsState.apply, events, GroupOfRunningDefinitionsState())
    
    @staticmethod
    def to_list(state: GroupOfRunningDefinitionsState):
        return [GroupOfRunningDefinitionsStateEventAdapter.to_dict(evt) for evt in state.get_events()]