from dataclasses import dataclass
import functools

from expression import Result
from expression.collections.block import Block
from expression.extra.result.traversable import traverse
from parsel import Selector

from shared.completedresult import CompletedWith
from shared.customtypes import Error
from shared.utils.asyncresult import ex_to_error_result
from stepdefinitions.html import GetContentFromHtmlConfig
from stepdefinitions.shared import ListOfContentData

@dataclass(frozen=True)
class GetContentFromHtmlCommand:
    config: GetContentFromHtmlConfig
    html: dict | list

def get_content(config: GetContentFromHtmlConfig, html: dict | list):
    def css_get_all(selector: Selector):
        match config.css_selector:
            case None:
                return None
            case _:
                return Result[list[str], Error].Ok(selector.css(config.css_selector).getall())
    def regex_get_all(selector: Selector):
        match config.regex_selector:
            case None:
                return None
            case _:
                return Result[list[str], Error].Ok(selector.re(config.regex_selector))
    @ex_to_error_result(Error.from_exception)
    def get_from_content(content: dict):
        selector = Selector(text=content["content"])
        matches_res = css_get_all(selector) or regex_get_all(selector) or Result.Error(Error("Selector not specified"))
        output_name = config.output_name or "content"
        html_without_output_name = {k:v for k, v in content.items() if k != output_name}
        res = matches_res.map(lambda matches: [html_without_output_name | {output_name:match} for match in matches])
        return res
    list_of_content = html if isinstance(html, list) else [html]
    contents_res = traverse(get_from_content, Block(list_of_content))
    res = contents_res.map(lambda contents: functools.reduce(lambda acc, curr: acc + curr, contents, []))
    return res

def content_to_result_data(content: list):
    match content:
        case []:
            return Result[list, Error].Ok([])
        case [*items]:
            return ListOfContentData.from_list(items)\
                .map_error(lambda _: Error(f"Ooops... Cannot convert into ListOfContentData: {content}"))\
                .map(ListOfContentData.to_list)

def to_completed_result(result_data: list):
    return CompletedWith.Data(result_data) if result_data else CompletedWith.NoData()

def handle(cmd: GetContentFromHtmlCommand):
    # 1. get data from html
    content_res = get_content(cmd.config, cmd.html)
    # 2. convert to result
    result_data_res = content_res.bind(content_to_result_data)
    result_res = result_data_res\
        .map(to_completed_result)\
        .map_error(lambda error: CompletedWith.Error(str(error)))
    # 3. return result
    result = result_res.merge()
    return result