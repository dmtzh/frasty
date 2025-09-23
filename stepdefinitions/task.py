from collections.abc import Callable
from dataclasses import dataclass

from expression import Result

from shared.customtypes import TaskIdValue
from shared.domaindefinition import StepDefinition
from shared.utils.result import ResultTag
from shared.validation import ValueError as ValueErr, ValueInvalid, ValueMissing

from .shared import ListOfDictData

def parse_from_dict[T](d: dict, key: str, parser: Callable[[str], T | None]) -> Result[T, ValueErr]:
    if key not in d:
        return Result.Error(ValueMissing(key))
    raw_value = d[key]
    opt_value = parser(raw_value)
    match opt_value:
        case None:
            return Result.Error(ValueInvalid(key))
        case value:
            return Result.Ok(value)

@dataclass(frozen=True)
class FetchNewDataInput:
    task_id: TaskIdValue
    
    @staticmethod
    def from_dict(data) -> Result["FetchNewDataInput", list[ValueErr]]:
        if not isinstance(data, dict):
            return Result.Error([ValueInvalid("data")])
        task_id_res = parse_from_dict(data, "task_id", TaskIdValue.from_value_with_checksum)
        match task_id_res:
            case Result(tag=ResultTag.OK, ok=task_id):
                return Result.Ok(FetchNewDataInput(task_id))
            case _:
                errors_with_none = [task_id_res.swap().default_value(None)]
                errors = [err for err in errors_with_none if err is not None]
                return Result.Error(errors)
    
    @staticmethod
    def to_dict(data: "FetchNewDataInput"):
        return {
            "task_id": data.task_id.to_value_with_checksum()
        }

class FetchNewData(StepDefinition[None]):
    def __init__(self):
        super().__init__(config=None)
    
    @property
    def input_type(self) -> type:
        return FetchNewDataInput
    
    @property
    def output_type(self) -> type:
        return ListOfDictData
    
    @staticmethod
    def create():
        return Result[StepDefinition, list[ValueErr]].Ok(FetchNewData())
    
    @staticmethod
    def validate_input(data):
        validated_input_res = FetchNewDataInput.from_dict(data)
        return validated_input_res.map(FetchNewDataInput.to_dict)
