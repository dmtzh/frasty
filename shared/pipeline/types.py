from dataclasses import dataclass

from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, TaskIdValue

@dataclass(frozen=True)
class RunTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: Metadata

@dataclass(frozen=True)
class RunDefinitionData:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    metadata: Metadata