from collections.abc import Generator
from enum import StrEnum
import os
from typing import Any, Optional

import aiofiles.os as aos
from expression import Result, effect
import pytest

import config
from shared.customtypes import IdValue
from shared.infrastructure.serialization.json import JsonSerializer
import shared.infrastructure.storage.filewithversion as filewithversion
from shared.infrastructure.storage.repository import AlreadyExistsException
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
        raw_first_name = data.get("first_name")
        opt_first_name = SampleFirstName.parse(raw_first_name) if raw_first_name is not None else None
        first_name = yield from Result.Ok(opt_first_name) if opt_first_name is not None else Result.Error("Invalid first_name")
        raw_last_name = data.get("last_name")
        opt_last_name = SampleLastName.parse(raw_last_name) if raw_last_name is not None else None
        last_name = yield from Result.Ok(opt_last_name) if opt_last_name is not None else Result.Error("Invalid last_name")
        return SampleDomain(first_name=first_name, last_name=last_name)
    
    @staticmethod
    def to_dict(domain: SampleDomain) -> dict:
        return {"first_name": domain.first_name.value, "last_name": domain.last_name.value}
    
FileWithVersion = filewithversion.FileWithVersion[IdValue, SampleDomain, dict]

# @pytest_asyncio.fixture(autouse=True, loop_scope="module", scope="module")
@pytest.fixture(autouse=True, scope="module")
def file_with_version_storage():
    folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "test_filewithversion")
    file_with_ver = FileWithVersion(
        SampleDomain.__name__,
        SampleDomainAdapter.to_dict,
        SampleDomainAdapter.from_dict,
        JsonSerializer[dict](),
        "json",
        folder_path
    )
    return file_with_ver

@pytest.fixture
def sample_domain():
    return SampleDomain(SampleFirstName.BOB, SampleLastName.SMITH)

@pytest.mark.asyncio
async def test_add_item(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    res = await file_with_version_storage.add(id, sample_domain)
    assert res is None

async def test_add_version_is_1(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver = await file_with_version_storage.get(id)
    assert item_with_ver is not None
    ver = item_with_ver[0]
    assert ver == 1

async def test_add_raises_error_for_existing_item(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    with pytest.raises(AlreadyExistsException) as ex_info:
        await file_with_version_storage.add(id, sample_domain)
    assert ex_info.value.args[0] == id

async def test_get_returns_correct_item(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver = await file_with_version_storage.get(id)
    assert item_with_ver is not None
    assert item_with_ver[1] == sample_domain

async def test_get_returns_correct_version(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver_1 = await file_with_version_storage.get(id)
    assert item_with_ver_1 is not None
    ver1, _ = item_with_ver_1
    updated_sample_domain = SampleDomain(SampleFirstName.ALICE, SampleLastName.JOHNSON)
    await file_with_version_storage.update(id, ver1, updated_sample_domain)
    item_with_ver_2 = await file_with_version_storage.get(id)
    assert item_with_ver_2 is not None
    ver2, _ = item_with_ver_2
    assert ver2 == (ver1 + 1)

async def test_get_returns_none_when_id_does_not_exist(file_with_version_storage: FileWithVersion):
    id = IdValue.new_id()
    item_with_ver = await file_with_version_storage.get(id)
    assert item_with_ver is None

async def test_update_item(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver_1 = await file_with_version_storage.get(id)
    assert item_with_ver_1 is not None
    ver1, _ = item_with_ver_1
    updated_sample_domain = SampleDomain(SampleFirstName.ALICE, SampleLastName.JOHNSON)
    updated = await file_with_version_storage.update(id, ver1, updated_sample_domain)
    assert updated
    item_with_ver_2 = await file_with_version_storage.get(id)
    assert item_with_ver_2 is not None
    _, item2 = item_with_ver_2
    assert item2 == updated_sample_domain

async def test_update_version_increments_by_1(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver_1 = await file_with_version_storage.get(id)
    assert item_with_ver_1 is not None
    ver1, _ = item_with_ver_1
    updated_sample_domain1 = SampleDomain(SampleFirstName.ALICE, SampleLastName.JOHNSON)
    await file_with_version_storage.update(id, ver1, updated_sample_domain1)
    item_with_ver_2 = await file_with_version_storage.get(id)
    assert item_with_ver_2 is not None
    ver2, _ = item_with_ver_2
    assert ver2 == (ver1 + 1)
    updated_sample_domain2 = SampleDomain(SampleFirstName.JOHN, SampleLastName.BLACK)
    await file_with_version_storage.update(id, ver2, updated_sample_domain2)
    item_with_ver_3 = await file_with_version_storage.get(id)
    assert item_with_ver_3 is not None
    ver3, _ = item_with_ver_3
    assert ver3 == (ver2 + 1)

async def test_update_returns_false_when_not_recent_version(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver_1 = await file_with_version_storage.get(id)
    assert item_with_ver_1 is not None
    ver1, _ = item_with_ver_1
    updated_sample_domain2 = SampleDomain(SampleFirstName.ALICE, SampleLastName.JOHNSON)
    await file_with_version_storage.update(id, ver1, updated_sample_domain2)
    updated_sample_domain3 = SampleDomain(SampleFirstName.JOHN, SampleLastName.BLACK)
    updated = await file_with_version_storage.update(id, ver1, updated_sample_domain3)
    assert not updated

async def test_update_item_not_updated_when_not_recent_version(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver_1 = await file_with_version_storage.get(id)
    assert item_with_ver_1 is not None
    ver1, _ = item_with_ver_1
    updated_sample_domain2 = SampleDomain(SampleFirstName.ALICE, SampleLastName.JOHNSON)
    await file_with_version_storage.update(id, ver1, updated_sample_domain2)
    item_with_ver_2 = await file_with_version_storage.get(id)
    assert item_with_ver_2 is not None
    ver2, _ = item_with_ver_2
    updated_sample_domain3 = SampleDomain(SampleFirstName.JOHN, SampleLastName.BLACK)
    await file_with_version_storage.update(id, ver1, updated_sample_domain3)
    item_with_ver = await file_with_version_storage.get(id)
    assert item_with_ver is not None
    ver, item = item_with_ver
    assert ver == ver2
    assert item == updated_sample_domain2

async def test_update_returns_false_when_not_existing_version(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver_1 = await file_with_version_storage.get(id)
    assert item_with_ver_1 is not None
    ver1, _ = item_with_ver_1
    updated_sample_domain2 = SampleDomain(SampleFirstName.ALICE, SampleLastName.JOHNSON)
    await file_with_version_storage.update(id, ver1, updated_sample_domain2)
    item_with_ver_2 = await file_with_version_storage.get(id)
    assert item_with_ver_2 is not None
    ver2, _ = item_with_ver_2
    updated_sample_domain3 = SampleDomain(SampleFirstName.JOHN, SampleLastName.BLACK)
    updated = await file_with_version_storage.update(id, ver2 + 1, updated_sample_domain3)
    assert not updated

async def test_update_item_not_updated_when_not_existing_version(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    item_with_ver_1 = await file_with_version_storage.get(id)
    assert item_with_ver_1 is not None
    ver1, _ = item_with_ver_1
    updated_sample_domain2 = SampleDomain(SampleFirstName.ALICE, SampleLastName.JOHNSON)
    await file_with_version_storage.update(id, ver1, updated_sample_domain2)
    item_with_ver_2 = await file_with_version_storage.get(id)
    assert item_with_ver_2 is not None
    ver2, _ = item_with_ver_2
    updated_sample_domain3 = SampleDomain(SampleFirstName.JOHN, SampleLastName.BLACK)
    await file_with_version_storage.update(id, ver2 + 1, updated_sample_domain3)
    item_with_ver = await file_with_version_storage.get(id)
    assert item_with_ver is not None
    ver, item = item_with_ver
    assert ver == ver2
    assert item == updated_sample_domain2

async def test_update_returns_false_when_not_existing_id(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    updated = await file_with_version_storage.update(id, 1, sample_domain)
    assert not updated

async def test_delete_existing_item(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    await file_with_version_storage.add(id, sample_domain)
    await file_with_version_storage.delete(id)
    item_with_ver = await file_with_version_storage.get(id)
    assert item_with_ver is None

async def test_delete_does_not_raise_error_for_not_existing_item(file_with_version_storage: FileWithVersion):
    id = IdValue.new_id()
    await file_with_version_storage.delete(id)

async def test_get_returns_none_when_empty_id_folder_exists(file_with_version_storage: FileWithVersion):
    id = IdValue.new_id()
    id_folder_path = os.path.join(file_with_version_storage._folder_path, str(id))
    await aos.makedirs(id_folder_path, exist_ok=True)

    actual_item_with_ver = await file_with_version_storage.get(id)
    
    assert actual_item_with_ver is None

async def test_add_item_when_empty_id_folder_exists(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    id_folder_path = os.path.join(file_with_version_storage._folder_path, str(id))
    await aos.makedirs(id_folder_path, exist_ok=True)

    res = await file_with_version_storage.add(id, sample_domain)
    
    assert res is None


async def test_update_returns_false_when_empty_id_folder_exists(file_with_version_storage: FileWithVersion, sample_domain: SampleDomain):
    id = IdValue.new_id()
    id_folder_path = os.path.join(file_with_version_storage._folder_path, str(id))
    await aos.makedirs(id_folder_path, exist_ok=True)

    actual_updated = await file_with_version_storage.update(id, 1, sample_domain)

    assert actual_updated is False