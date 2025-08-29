from __future__ import annotations
from dataclasses import dataclass

from expression import Result
from expression.collections.block import Block
from expression.extra.result.traversable import traverse

from shared.utils.crockfordid import CrockfordId
from shared.validation import ValueInvalid, ValueMissing, ValueError as ValueErr

def validate_input_id(input_dict: dict, id: str) -> Result[str, list[ValueErr]]:
    if id not in input_dict:
        return Result.Error([ValueMissing(id)])
    raw_id = input_dict.get(id)
    if not isinstance(raw_id, str):
        return Result.Error([ValueInvalid(id)])
    match raw_id.strip():
        case "":
            return Result.Error([ValueInvalid(id)])
        case raw_id_stripped:
            opt_id = CrockfordId.from_value_with_checksum(raw_id_stripped)
            match opt_id:
                case None:
                    return Result.Error([ValueInvalid(id)])
                case _:
                    return Result.Ok(raw_id_stripped)

@dataclass(frozen=True)
class HttpResponseData:
    status_code: int
    content_type: str
    content: str

    @staticmethod
    def from_dict(data) -> Result[HttpResponseData, list[ValueErr]]:
        def validate_status_code() -> Result[int, list[ValueErr]]:
            if "status_code" not in data:
                return Result.Error([ValueMissing("status_code")])
            value = data.get("status_code")
            return Result.Ok(value) if isinstance(value, int) else Result.Error([ValueInvalid("status_code")])
        def validate_content_type() -> Result[str, list[ValueErr]]:
            if "content_type" not in data:
                return Result.Error([ValueMissing("content_type")])
            raw_content_type = data.get("content_type")
            if not isinstance(raw_content_type, str):
                return Result.Error([ValueInvalid("content_type")])
            match raw_content_type.strip():
                case "":
                    return Result.Error([ValueInvalid("content_type")])
                case raw_content_type_stripped:
                    return Result.Ok(raw_content_type_stripped)
        def validate_content() -> Result[str, list[ValueErr]]:
            if "content" not in data:
                return Result.Error([ValueMissing("content")])
            raw_content = data.get("content")
            if not isinstance(raw_content, str):
                return Result.Error([ValueInvalid("content")])
            match raw_content.strip():
                case "":
                    return Result.Error([ValueInvalid("content")])
                case raw_content_stripped:
                    return Result.Ok(raw_content_stripped)
        if not isinstance(data, dict):
            return Result.Error([ValueInvalid("data")])
        status_code_res = validate_status_code()
        content_type_res = validate_content_type()
        content_res = validate_content()
        errors = status_code_res.swap().default_value([]) + content_type_res.swap().default_value([]) + content_res.swap().default_value([])
        match errors:
            case []:
                return Result.Ok(HttpResponseData(status_code_res.ok, content_type_res.ok, content_res.ok))
            case _:
                return Result.Error(errors)
    
    @staticmethod
    def to_dict(data: HttpResponseData):
        return {
            "status_code": data.status_code,
            "content_type": data.content_type,
            "content": data.content
        }

class DictData:
    def __init__(self, dict_data: dict):
        if not isinstance(dict_data, dict) or not dict_data:
            raise ValueError("dict_data must be non empty dict")
        self._dict_data = dict_data
    
    @staticmethod
    def to_dict(data: DictData):
        return data._dict_data
    
    @staticmethod
    def from_dict(data) -> Result[DictData, list[ValueErr]]:
        match data:
            case {**dict_data}:
                return Result.Ok(DictData(dict_data)) if dict_data else Result.Error([ValueInvalid("data")])
            case _:
                return Result.Error([ValueInvalid("data")])

class ContentData(DictData):
    def __init__(self, dict_data: dict):
        super().__init__(dict_data)
        if not isinstance(dict_data.get("content"), str):
            raise ValueError("dict_data['content'] must be str")
        
    @property
    def content(self) -> str:
        return self._dict_data["content"]

    @staticmethod
    def from_dict(data):
        def validate_content(dict_data: DictData) -> Result[ContentData, list[ValueErr]]:
            raw_dict_data = DictData.to_dict(dict_data)
            if "content" not in raw_dict_data:
                return Result.Error([ValueMissing("content")])
            has_content = isinstance(raw_dict_data.get("content"), str)
            return Result.Ok(ContentData(raw_dict_data)) if has_content else Result.Error([ValueInvalid("content")])
        return DictData.from_dict(data).bind(validate_content)

class ListOfDictData:
    def __init__(self, list_data: list):
        is_non_empty_list = isinstance(list_data, list) and list_data
        if not is_non_empty_list:
            raise ValueError("list_data must be non empty list")
        all_items_are_non_empty_dict = all(isinstance(item, dict) and dict for item in list_data)
        if not all_items_are_non_empty_dict:
            raise ValueError("list_data must be list of non empty dictionaries")
        self._list_data = list_data
    
    @staticmethod
    def from_list(data) -> Result[ListOfDictData, list[ValueErr]]:
        match data:
            case []:
                return Result.Error([ValueInvalid("data")])
            case [*list_data]:
                all_items_are_non_empty_dict = all(isinstance(item, dict) and dict for item in list_data)
                return Result.Ok(ListOfDictData(list_data)) if all_items_are_non_empty_dict else Result.Error([ValueInvalid("data")])
            case _:
                return Result.Error([ValueInvalid("data")])
    
    @staticmethod
    def to_list(data: ListOfDictData):
        return data._list_data

class ListOfContentData(ListOfDictData):
    def __init__(self, list_of_content_data: list[ContentData]):
        is_non_empty_list = isinstance(list_of_content_data, list) and list_of_content_data
        if not is_non_empty_list:
            raise ValueError("list_of_content_data must be non empty list")
        all_items_are_content_data = all(isinstance(item, ContentData) for item in list_of_content_data)
        if not all_items_are_content_data:
            raise ValueError("list_of_content_data must be list of content data")
        list_data = [ContentData.to_dict(item) for item in list_of_content_data]
        super().__init__(list_data)
    
    @staticmethod
    def from_list(data):
        def to_list_of_content_data(list_of_dict_data: ListOfDictData):
            list_data = ListOfDictData.to_list(list_of_dict_data)
            items_res = traverse(ContentData.from_dict, Block(list_data)).map(list)
            return items_res
        list_of_dict_data_res = ListOfDictData.from_list(data)
        list_of_content_data_res = list_of_dict_data_res.bind(to_list_of_content_data)
        return list_of_content_data_res.map(ListOfContentData)