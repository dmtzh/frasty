from dataclasses import dataclass

from expression import Result

from shared.domaindefinition import StepDefinition
from shared.utils.parse import parse_non_empty_str, parse_value
from shared.utils.result import ResultTag
from shared.validation import ValueInvalid, ValueError as ValueErr
from stepdefinitions.shared import ContentData, ListOfContentData

@dataclass(frozen=True)
class GetContentFromJsonConfig:
    query: str
    output_name: str | None

    @staticmethod
    def create(query: str, output_name: str | None) -> Result["GetContentFromJsonConfig", list[ValueErr]]:
        def validate_query() -> Result[str, list[ValueErr]]:
            return parse_value(query, "query", parse_non_empty_str).map_error(lambda _: [ValueInvalid("query")])
        def validate_output_name() -> Result[str | None, list[ValueErr]]:
            if not isinstance(output_name, str):
                return Result.Ok(None)
            return parse_value(output_name, "output_name", parse_non_empty_str).map_error(lambda _: [ValueInvalid("output_name")])
            
        query_res = validate_query()
        output_name_res = validate_output_name()
        match query_res, output_name_res:
            case Result(tag=ResultTag.OK, ok=valid_query), Result(tag=ResultTag.OK, ok=valid_output_name):
                config = GetContentFromJsonConfig(valid_query, valid_output_name)
                return Result.Ok(config)
            case _:
                errors = query_res.swap().default_value([]) + output_name_res.swap().default_value([])
                return Result.Error(errors)

type GetContentFromJsonInputType = ContentData | ListOfContentData

class GetContentFromJson(StepDefinition[GetContentFromJsonConfig]):
    def __init__(self, config: GetContentFromJsonConfig):
        super().__init__(config=config)
    
    @property
    def input_type(self) -> type:
        return GetContentFromJsonInputType.__value__
    
    @property
    def output_type(self) -> type:
        return ListOfContentData
    
    @staticmethod
    def create(query: str, output_name: str | None = None):
        config_res = GetContentFromJsonConfig.create(query, output_name)
        return config_res.map(GetContentFromJson)
    
    @staticmethod
    def validate_input(data) -> Result[ContentData | ListOfContentData, list[ValueErr]]:
        match data:
            case {**dict_data}:
                return ContentData.from_dict(dict_data)
            case [*list_data]:
                return ListOfContentData.from_list(list_data)
            case _:
                return Result.Error([ValueInvalid("data")])