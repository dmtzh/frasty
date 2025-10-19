from dataclasses import dataclass

from shared.customtypes import RunIdValue, StepIdValue

@dataclass(frozen=True)
class RunStepData[TCfg, D]:
    run_id: RunIdValue
    step_id: StepIdValue
    config: TCfg
    data: D
    metadata: dict