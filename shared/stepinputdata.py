from dataclasses import dataclass

from shared.customtypes import Metadata, RunIdValue, StepIdValue

@dataclass(frozen=True)
class StepInputData[TCfg, D]:
    run_id: RunIdValue
    step_id: StepIdValue
    config: TCfg
    data: D
    metadata: Metadata