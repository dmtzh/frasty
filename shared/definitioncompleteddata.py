from dataclasses import dataclass

from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue

@dataclass(frozen=True)
class DefinitionCompletedData:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    result: CompletedResult
    metadata: dict