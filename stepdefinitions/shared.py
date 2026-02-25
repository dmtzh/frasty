from __future__ import annotations
from dataclasses import dataclass

from expression import Result

from shared.validation import ValueInvalid, ValueMissing, ValueError as ValueErr

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
