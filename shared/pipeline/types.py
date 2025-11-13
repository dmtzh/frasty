from dataclasses import dataclass

from shared.customtypes import Metadata, RunIdValue, TaskIdValue

@dataclass(frozen=True)
class RunTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: Metadata