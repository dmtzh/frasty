from __future__ import annotations
from expression import Result

from shared.domaindefinition import StepDefinition
from shared.validation import ValueError as ValueErr

from .shared import HttpResponseData, ContentData

class FilterHtmlResponse(StepDefinition[None]):
    def __init__(self):
        super().__init__(config=None)
    
    @property
    def input_type(self) -> type:
        return HttpResponseData
    
    @property
    def output_type(self) -> type:
        return HtmlContentData
    
    @staticmethod
    def create():
        return Result[StepDefinition, list[ValueErr]].Ok(FilterHtmlResponse())
    
    @staticmethod
    def validate_input(data):
        return HttpResponseData.from_dict(data).map(HttpResponseData.to_dict)

class HtmlContentData(ContentData):
    def __init__(self, content: str):
        super().__init__({"content": content})
    
    @staticmethod
    def to_dict(data: HtmlContentData):
        return {"content": data.content}
    
    @staticmethod
    def from_dict(data):
        return ContentData.from_dict(data).map(lambda content_data: HtmlContentData(content_data.content))