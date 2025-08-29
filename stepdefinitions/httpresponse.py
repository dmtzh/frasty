from expression import Result

from shared.domaindefinition import StepDefinition
from shared.validation import ValueError as ValueErr

from .shared import HttpResponseData

class FilterSuccessResponse(StepDefinition[None]):
    def __init__(self):
        super().__init__(config=None)
    
    @property
    def input_type(self) -> type:
        return HttpResponseData
    
    @property
    def output_type(self) -> type:
        return HttpResponseData
    
    @staticmethod
    def create():
        return Result[StepDefinition, list[ValueErr]].Ok(FilterSuccessResponse())