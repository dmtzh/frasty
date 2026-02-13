from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

TCfg = TypeVar("TCfg")

class StepDefinition(ABC, Generic[TCfg]):
    def __init__(self, config: TCfg):
        self._config = config
    
    @property
    @abstractmethod
    def input_type(self) -> type:
        '''The type of the input data'''
    
    @property
    @abstractmethod
    def output_type(self) -> type:
        '''The type of the output data'''
    
    @property
    def config(self) -> TCfg:
        return self._config
