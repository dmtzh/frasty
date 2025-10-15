from dataclasses import dataclass
import functools
import json

from expression import Result
from expression.collections.block import Block
from expression.extra.result.traversable import traverse
import jsonpath_ng as jp

from shared.completedresult import CompletedWith
from shared.customtypes import Error
from shared.utils.asyncresult import ex_to_error_result
from stepdefinitions.shared import ContentData, ListOfContentData

from .definition import GetContentFromJsonConfig

@dataclass(frozen=True)
class GetContentFromJsonCommand:
    config: GetContentFromJsonConfig
    input_data: ContentData | ListOfContentData

def get_content(config: GetContentFromJsonConfig, input_data: ContentData | ListOfContentData):
    def query_get_all(dict_with_content: dict):
        content_obj = json.loads(dict_with_content["content"])
        jp_query = jp.parse(config.query)
        matches = [json.dumps(match.value) for match in jp_query.find(content_obj)]
        return matches
    @ex_to_error_result(Error.from_exception)
    def get_from_content(dict_with_content: dict):
        matches = query_get_all(dict_with_content)
        output_name = config.output_name or "content"
        dict_without_output_name = {k:v for k, v in dict_with_content.items() if k != output_name}
        output_list = [dict_without_output_name | {output_name:match} for match in matches]
        return output_list
    match input_data:
        case ContentData():
            list_of_content = [ContentData.to_dict(input_data)]
        case ListOfContentData():
            list_of_content = ListOfContentData.to_list(input_data)
    contents_res = traverse(get_from_content, Block(list_of_content))
    res = contents_res.map(lambda contents: functools.reduce(lambda acc, curr: acc + curr, contents, []))
    return res

def content_to_output_data(content_list: list[dict]) -> Result[list, Error]:
    match content_list:
        case []:
            return Result.Ok([])
        case [*items]:
            return ListOfContentData.from_list(items)\
                .map_error(lambda _: Error(f"Ooops... Cannot convert into ListOfContentData: {content_list}"))\
                .map(ListOfContentData.to_list)

def to_completed_result(output_data: list):
    return CompletedWith.Data(output_data) if output_data else CompletedWith.NoData()

def handle(cmd: GetContentFromJsonCommand):
    # 1. get content from input json
    content_res = get_content(cmd.config, cmd.input_data)
    # 2. convert to output data
    output_data_res = content_res.bind(content_to_output_data)
    # 3. convert to completed result
    result_res = output_data_res\
        .map(to_completed_result)\
        .map_error(lambda error: CompletedWith.Error(str(error)))
    result = result_res.merge()
    return result