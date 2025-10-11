from collections.abc import Generator
from enum import StrEnum
import os
from typing import Any, Optional

from expression import effect
import pytest

import config
from shared.customtypes import IdValue
from shared.infrastructure.serialization.json import JsonSerializer
import shared.infrastructure.storage.file as file
from shared.infrastructure.storage.repository import AlreadyExistsException, NotFoundException
from shared.utils.parse import parse_from_dict
from shared.utils.string import strip_and_lowercase

class SampleFirstName(StrEnum):
    ALICE = "Alice"
    BOB = "Bob"
    JOHN = "John"
    @staticmethod
    def parse(value: str) -> Optional["SampleFirstName"]:
        if value is None:
            return None
        match strip_and_lowercase(value).capitalize():
            case SampleFirstName.ALICE:
                return SampleFirstName.ALICE
            case SampleFirstName.BOB:
                return SampleFirstName.BOB
            case SampleFirstName.JOHN:
                return SampleFirstName.JOHN
            case _:
                return None

class SampleLastName(StrEnum):
    BLACK = "Black"
    JOHNSON = "Johnson"
    SMITH = "Smith"
    @staticmethod
    def parse(value: str) -> Optional["SampleLastName"]:
        if value is None:
            return None
        match strip_and_lowercase(value).capitalize():
            case SampleLastName.BLACK:
                return SampleLastName.BLACK
            case SampleLastName.JOHNSON:
                return SampleLastName.JOHNSON
            case SampleLastName.SMITH:
                return SampleLastName.SMITH
            case _:
                return None

class SampleDomain:
    first_name: SampleFirstName
    last_name: SampleLastName
    def __init__(self, first_name: SampleFirstName, last_name: SampleLastName):
        self.first_name = first_name
        self.last_name = last_name
    
    def __eq__(self, other): 
        if not isinstance(other, SampleDomain):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.first_name == other.first_name and self.last_name == other.last_name

class SampleDomainAdapter:
    @effect.result[SampleDomain, str]()
    @staticmethod
    def from_dict(data: dict) -> Generator[Any, Any, SampleDomain]:
        first_name = yield from parse_from_dict(data, "first_name", SampleFirstName.parse)
        last_name = yield from parse_from_dict(data, "last_name", SampleLastName.parse)
        return SampleDomain(first_name=first_name, last_name=last_name)
    
    @staticmethod
    def to_dict(domain: SampleDomain) -> dict:
        return {"first_name": domain.first_name.value, "last_name": domain.last_name.value}
    
File = file.File[IdValue, SampleDomain, dict]

@pytest.fixture(autouse=True, scope="module")
def file_storage():
    folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "test_file")
    file_store = File(
        SampleDomain.__name__,
        SampleDomainAdapter.to_dict,
        SampleDomainAdapter.from_dict,
        JsonSerializer[dict](),
        "json",
        folder_path
    )
    return file_store

@pytest.fixture(scope="module")
def sample_domain():
    return SampleDomain(SampleFirstName.BOB, SampleLastName.SMITH)



@pytest.mark.asyncio
async def test_add_item(file_storage: File, sample_domain: SampleDomain):
    id = IdValue.new_id()
    
    res = await file_storage.add(id, sample_domain)
    
    assert res is None



async def test_add_raises_exception_for_existing_item(file_storage: File, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_storage.add(id, sample_domain)

    with pytest.raises(AlreadyExistsException):
        await file_storage.add(id, sample_domain)



async def test_get_returns_correct_item(file_storage: File, sample_domain: SampleDomain):
    id = IdValue.new_id()
    expected_item = sample_domain
    await file_storage.add(id, sample_domain)

    item = await file_storage.get(id)

    assert item == expected_item



async def test_get_returns_none_when_id_does_not_exist(file_storage: File):
    id = IdValue.new_id()

    item = await file_storage.get(id)

    assert item is None



async def test_update_item(file_storage: File, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_storage.add(id, sample_domain)
    expected_item = SampleDomain(SampleFirstName.ALICE, SampleLastName.JOHNSON)
    
    await file_storage.update(id, expected_item)
    actual_item = await file_storage.get(id)

    assert actual_item == expected_item



async def test_update_raises_exception_for_not_existing_item(file_storage: File, sample_domain: SampleDomain):
    not_existing_id = IdValue.new_id()
    
    with pytest.raises(NotFoundException):
        await file_storage.update(not_existing_id, sample_domain)



async def test_delete_existing_item(file_storage: File, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_storage.add(id, sample_domain)
    
    await file_storage.delete(id)
    item = await file_storage.get(id)

    assert item is None


async def test_delete_does_not_raise_error_for_not_existing_item(file_storage: File):
    id = IdValue.new_id()
    
    await file_storage.delete(id)
