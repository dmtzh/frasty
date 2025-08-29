from collections.abc import Generator
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from expression import Result, effect

from shared.utils.string import strip_and_lowercase

class CompletedWith:
    @dataclass(frozen=True)
    class Data:
        data: Any
    @dataclass(frozen=True)
    class NoData:
        pass
    @dataclass(frozen=True)
    class Error:
        message: str

type CompletedResult = CompletedWith.Data | CompletedWith.NoData | CompletedWith.Error

class CompletedResultDtoTypes(StrEnum):
    DATA = "data"
    NO_DATA = "nodata"
    ERROR = "error"

    @staticmethod
    def parse(type: str) -> Optional["CompletedResultDtoTypes"]:
        if type is None:
            return None
        match strip_and_lowercase(type):
            case CompletedResultDtoTypes.DATA:
                return CompletedResultDtoTypes.DATA
            case CompletedResultDtoTypes.NO_DATA:
                return CompletedResultDtoTypes.NO_DATA
            case CompletedResultDtoTypes.ERROR:
                return CompletedResultDtoTypes.ERROR
            case _:
                return None

class CompletedResultAdapter:
    @effect.result[CompletedResult, str]()
    @staticmethod
    def from_dict(data: dict[str, Any]) -> Generator[Any, Any, CompletedResult]:
        data_dict = yield from Result.Ok(data) if isinstance(data, dict) and data else Result.Error("data is invalid")
        raw_type = yield from Result.Ok(data_dict["type"]) if "type" in data_dict else Result.Error("type is missing")
        type = CompletedResultDtoTypes.parse(raw_type)
        match type:
            case CompletedResultDtoTypes.DATA:
                raw_data = yield from Result.Ok(data_dict["data"]) if "data" in data_dict else Result.Error("data is missing")
                return CompletedWith.Data(raw_data)
            case CompletedResultDtoTypes.NO_DATA:
                return CompletedWith.NoData()
            case CompletedResultDtoTypes.ERROR:
                raw_error_message = yield from Result.Ok(data_dict["error_message"]) if "error_message" in data_dict else Result.Error("error_message is missing")
                return CompletedWith.Error(raw_error_message)
            case _:
                yield from Result.Error(f"completed result type {raw_type} is invalid")
                raise RuntimeError("completed result type is invalid")
    
    @staticmethod
    def to_dict(result: CompletedResult) -> dict[str, Any]:
        match result:
            case CompletedWith.Data(data=data):
                return {"type": CompletedResultDtoTypes.DATA.value, "data": data}
            case CompletedWith.NoData():
                return {"type": CompletedResultDtoTypes.NO_DATA.value}
            case CompletedWith.Error(message=error_message):
                return {"type": CompletedResultDtoTypes.ERROR.value, "error_message": error_message}
