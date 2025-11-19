from types import NoneType
from expression import Result

from shared.domaindefinition import StepDefinition
from shared.validation import ValueError as ValueErr

from .commands import Command, CommandAdapter

class ChangeTaskSchedule(StepDefinition[None]):
    def __init__(self):
        super().__init__(config=None)
    
    @property
    def input_type(self) -> type:
        return Command.__value__
    
    @property
    def output_type(self) -> type:
        return NoneType
    
    @staticmethod
    def create():
        return Result[StepDefinition, list[ValueErr]].Ok(ChangeTaskSchedule())
    
    @staticmethod
    def validate_input(data) -> Result[Command, list[ValueErr]]:
        return CommandAdapter.from_dict(data)