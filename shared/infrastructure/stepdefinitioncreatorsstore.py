from collections.abc import Callable

from expression import Result

from .. import domaindefinition as shdomaindef
from .storage.inmemory import InMemory
from .storage.repository import Repository
from ..validation import ValueError

def get_step_definition_name(step_definition_type: type[shdomaindef.StepDefinition]):
    if not issubclass(step_definition_type, shdomaindef.StepDefinition):
        raise TypeError("step_definition_type must be a subclass of StepDefinition")
    return step_definition_type.__name__.lower()

class StepDefinitionCreatorsStore:
    def __init__(self):
        in_memory_repo = InMemory[str, Callable[..., Result[shdomaindef.StepDefinition, list[ValueError]]]]()
        self._storage: Repository[str, Callable[..., Result[shdomaindef.StepDefinition, list[ValueError]]]] = in_memory_repo
    
    def get(self, id: str):
        return self._storage.get(id)
    
    def add(self, step_definition_type: type[shdomaindef.StepDefinition]):
        return self._storage.add(get_step_definition_name(step_definition_type), step_definition_type.create)

step_definition_creators_storage = StepDefinitionCreatorsStore()