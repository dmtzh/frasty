from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, NamedTuple

from expression import effect

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, Error, StepIdValue
from shared.definition import Definition

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
    def apply_command(self, cmd: Commands.Command) -> Events.Event | None:
        match cmd:
            case GroupOfRunningDefinitionsState.Commands.SetDefinitions(definitions=definitions):
                raise NotImplementedError()
            case GroupOfRunningDefinitionsState.Commands.RunDefinitions():
                raise NotImplementedError()
            case GroupOfRunningDefinitionsState.Commands.CompleteDefinition(definition_id=definition_id, result=result):
                raise NotImplementedError()
            case GroupOfRunningDefinitionsState.Commands.Fail(error=error):
                raise NotImplementedError()
        raise ValueError(f"Unknown command {cmd}")

class GroupOfRunningDefinitionsStateAdapter:
    @effect.result[GroupOfRunningDefinitionsState, str]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, GroupOfRunningDefinitionsState]:
        raise NotImplementedError()
    
    @staticmethod
    def to_list(state: GroupOfRunningDefinitionsState):
        raise NotImplementedError()