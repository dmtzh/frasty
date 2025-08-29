from __future__ import annotations
from dataclasses import dataclass
from expression import Result

from shared.domaindefinition import StepDefinition
from shared.validation import ValueMissing, ValueInvalid, ValueError as ValueErr

from .shared import HttpResponseData, ContentData, ListOfContentData, ListOfDictData

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

@dataclass(frozen=True)
class GetContentFromHtmlConfig:
    css_selector: str | None
    regex_selector: str | None
    output_name: str | None

    @staticmethod
    def create(css_selector: str | None, regex_selector: str | None, output_name: str | None) -> Result[GetContentFromHtmlConfig, list[ValueErr]]:
        def validate_has_any_selector() -> Result[None, list[ValueErr]]:
            has_any_selector = isinstance(css_selector, str) or isinstance(regex_selector, str)
            return Result.Ok(None) if has_any_selector else Result.Error([ValueMissing("css_selector")])
        def validate_css_selector() -> Result[str | None, list[ValueErr]]:
            if not isinstance(css_selector, str):
                return Result.Ok(None)
            match css_selector.strip():
                case "":
                    return Result.Error([ValueInvalid("css_selector")])
                case css_selector_stripped:
                    return Result.Ok(css_selector_stripped)
        def validate_regex_selector() -> Result[str | None, list[ValueErr]]:
            if not isinstance(regex_selector, str):
                return Result.Ok(None)
            match regex_selector.strip():
                case "":
                    return Result.Error([ValueInvalid("regex_selector")])
                case regex_selector_stripped:
                    return Result.Ok(regex_selector_stripped)
        def validate_output_name() -> Result[str | None, list[ValueErr]]:
            if not isinstance(output_name, str):
                return Result.Ok(None)
            match output_name.strip():
                case "":
                    return Result.Error([ValueInvalid("output_name")])
                case output_name_stripped:
                    return Result.Ok(output_name_stripped)
        has_any_selector_res = validate_has_any_selector()
        css_selector_res = validate_css_selector()
        regex_selector_res = validate_regex_selector()
        output_name_res = validate_output_name()
        errors = has_any_selector_res.swap().default_value([]) + css_selector_res.swap().default_value([]) + regex_selector_res.swap().default_value([]) + output_name_res.swap().default_value([])
        match errors:
            case []:
                return Result.Ok(GetContentFromHtmlConfig(css_selector_res.ok, regex_selector_res.ok, output_name_res.ok))
            case _:
                return Result.Error(errors)

type GetContentFromHtmlInputType = HtmlContentData | ListOfContentData

class GetContentFromHtml(StepDefinition[GetContentFromHtmlConfig]):
    def __init__(self, config: GetContentFromHtmlConfig):
        super().__init__(config=config)
    
    @property
    def input_type(self) -> type:
        return GetContentFromHtmlInputType.__value__
    
    @property
    def output_type(self) -> type:
        return ListOfContentData
    
    @staticmethod
    def create(css_selector: str | None = None, regex_selector: str | None = None, output_name: str | None = None):
        config_res = GetContentFromHtmlConfig.create(css_selector=css_selector, regex_selector=regex_selector, output_name=output_name)
        return config_res.map(GetContentFromHtml)
    
    @staticmethod
    def validate_input(data):
        match data:
            case {**dict_data}:
                return HtmlContentData.from_dict(dict_data).map(HtmlContentData.to_dict)
            case [*list_data]:
                return ListOfContentData.from_list(list_data).map(ListOfContentData.to_list)
            case _:
                return Result.Error([ValueInvalid("data")])

@dataclass(frozen=True)
class GetLinksFromHtmlConfig:
    text_name: str | None
    link_name: str | None

    @staticmethod
    def create(text_name: str | None, link_name: str | None) -> Result[GetLinksFromHtmlConfig, list[ValueErr]]:
        def validate_text_name() -> Result[str | None, list[ValueErr]]:
            if not isinstance(text_name, str):
                return Result.Ok(None)
            match text_name.strip():
                case "":
                    return Result.Error([ValueInvalid("text_name")])
                case text_name_stripped:
                    return Result.Ok(text_name_stripped)
        def validate_link_name() -> Result[str | None, list[ValueErr]]:
            if not isinstance(link_name, str):
                return Result.Ok(None)
            match link_name.strip():
                case "":
                    return Result.Error([ValueInvalid("link_name")])
                case link_name_stripped:
                    return Result.Ok(link_name_stripped)
        text_name_res = validate_text_name()
        link_name_res = validate_link_name()
        errors = text_name_res.swap().default_value([]) + link_name_res.swap().default_value([])
        match errors:
            case []:
                config = GetLinksFromHtmlConfig(text_name_res.ok, link_name_res.ok)
                return Result.Ok(config)
            case _:
                return Result.Error(errors)

type GetLinksFromHtmlInputType = HtmlContentData | ListOfContentData

class GetLinksFromHtml(StepDefinition[GetLinksFromHtmlConfig]):
    def __init__(self, config: GetLinksFromHtmlConfig):
        super().__init__(config=config)
    
    @property
    def input_type(self) -> type:
        return GetLinksFromHtmlInputType.__value__
    
    @property
    def output_type(self) -> type:
        return ListOfDictData
    
    @staticmethod
    def create(text_name: str | None = None, link_name: str | None = None):
        config_res = GetLinksFromHtmlConfig.create(text_name=text_name, link_name=link_name)
        return config_res.map(GetLinksFromHtml)
    
    @staticmethod
    def validate_input(data):
        return GetContentFromHtml.validate_input(data)

class HtmlContentData(ContentData):
    def __init__(self, content: str):
        super().__init__({"content": content})
    
    @staticmethod
    def to_dict(data: HtmlContentData):
        return {"content": data.content}
    
    @staticmethod
    def from_dict(data):
        return ContentData.from_dict(data).map(lambda content_data: HtmlContentData(content_data.content))