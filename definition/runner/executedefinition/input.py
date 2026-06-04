from collections.abc import Generator
from dataclasses import dataclass
import functools
from typing import Any

from expression import Result, effect

from shared.executedefinitionaction import ExecuteDefinitionInput
from shared.utils.parse import parse_value
from shared.utils.result import apply

@dataclass(frozen=True)
class ExecuteGroupOfDefinitionsInput:
    items: tuple[ExecuteDefinitionInput, ...]
    
    def to_list(self):
        return [item.to_dict() for item in self.items]
    
    @effect.result['ExecuteGroupOfDefinitionsInput', str]()
    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> Generator[Any, Any, 'ExecuteGroupOfDefinitionsInput']:
        non_empty_list = yield from parse_value(data, "definitions", lambda lst: lst if isinstance(lst, list) and lst else None)
        list_of_dicts = yield from parse_value(non_empty_list, "definitions", lambda lst: lst if all(isinstance(item, dict) and item for item in lst) else None)
        def reduce_func(acc_res: Result[tuple[ExecuteDefinitionInput, ...], tuple[str, ...]], raw_item: dict[str, Any]):
            item_res = ExecuteDefinitionInput.from_dict(raw_item)
            return apply(lambda acc, item: acc + (item,), lambda err: err, acc_res, item_res)
        initial_res = Result[tuple[ExecuteDefinitionInput, ...], tuple[str, ...]].Ok(())
        exec_def_inputs = yield from functools.reduce(reduce_func, list_of_dicts, initial_res).map_error(", ".join)
        # seen_def_ids = set[DefinitionIdValue]()
        return ExecuteGroupOfDefinitionsInput(exec_def_inputs)