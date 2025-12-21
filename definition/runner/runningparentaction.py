from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from expression import Result

from shared.completedresult import CompletedResult, CompletedResultAdapter, CompletedWith
from shared.customtypes import Metadata, RunIdValue, StepIdValue
from shared.pipeline.actionhandler import ActionDataDto, CompleteActionData
from shared.utils.parse import parse_from_dict

@dataclass(frozen=True)
class RunningParentAction:
    run_id: RunIdValue
    step_id: StepIdValue
    metadata: Metadata

    async def run_complete_definition(self, run_action: Callable[[str, ActionDataDto], Coroutine[Any, Any, Result[None, Any]]], result: CompletedResult):
        if self.metadata.get_definition_id() is None:
            return Result[None, str].Error("metadata does not have definition id")
        result_dict = CompletedResultAdapter.to_dict(result)
        complete_action_data = CompleteActionData(
            self.run_id,
            self.step_id,
            CompletedWith.Data(result_dict),
            self.metadata
        )
        return await complete_action_data.run_complete(run_action)
    
    def add_to_metadata(self, metadata: Metadata):
        metadata.set_id("parent_run_id", self.run_id)
        metadata.set_id("parent_step_id", self.step_id)
        metadata.set("parent_metadata", self.metadata.to_dict())
    
    @staticmethod
    def parse(metadata: Metadata):
        opt_parent_run_id = metadata.get_id("parent_run_id", RunIdValue)
        opt_parent_step_id = metadata.get_id("parent_step_id", StepIdValue)
        opt_parent_metadata = parse_from_dict(metadata, "parent_metadata", lambda pm: pm if isinstance(pm, dict) and pm else None).default_value(None)
        
        if opt_parent_run_id is None or opt_parent_step_id is None or opt_parent_metadata is None:
            return None
        return RunningParentAction(opt_parent_run_id, opt_parent_step_id, Metadata(opt_parent_metadata))