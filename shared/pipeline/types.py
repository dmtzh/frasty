from dataclasses import dataclass

from shared.customtypes import DefinitionIdValue, Metadata, RunIdValue, StepIdValue, TaskIdValue

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

@dataclass(frozen=True)
class StepInputData[TCfg, D]:
    run_id: RunIdValue
    step_id: StepIdValue
    config: TCfg
    data: D
    metadata: Metadata