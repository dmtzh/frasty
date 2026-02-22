from dataclasses import dataclass

from shared.completedresult import CompletedResult
from shared.customtypes import Metadata, RunIdValue, StepIdValue, TaskIdValue
from shared.domaindefinition import StepDefinition

@dataclass(frozen=True)
class RunTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: Metadata

@dataclass(frozen=True)
class StepData[TCfg, D]:
    run_id: RunIdValue
    step_id: StepIdValue
    definition: StepDefinition[TCfg]
    data: D
    metadata: Metadata

@dataclass(frozen=True)
class CompleteStepData:
    run_id: RunIdValue
    step_id: StepIdValue
    result: CompletedResult
    metadata: Metadata
