from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Generic, ParamSpec, TypeVar

from expression import Result
from faststream.rabbit.subscriber.asyncapi import AsyncAPISubscriber

from shared.customtypes import Error
from shared.infrastructure.rabbitmq.client import Error as RabbitClientError

P = ParamSpec("P")
R = TypeVar("R")

# TODO: should be removed because adds complexity overhead without any benefits
class Command(Generic[P], ABC):
    @abstractmethod
    async def run(self, *args: P.args, **kwargs: P.kwargs) -> Result[None, RabbitClientError.CommandRecipientNotFound | RabbitClientError.SendCommandTimeout]:
        pass

    @abstractmethod
    def handler(input_adapter: Callable[P, R]) -> AsyncAPISubscriber:
        pass

# TODO: should be removed and replaced with client.Error.UnexpectedError
class RunCommandUnexpectedError(Error):
    '''Unexpected error when run command'''
type RunCommandError = RabbitClientError.CommandRecipientNotFound | RabbitClientError.SendCommandTimeout | RunCommandUnexpectedError