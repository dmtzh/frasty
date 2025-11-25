import asyncio
import multiprocessing as mp
import os

import pytest

import config
from shared.infrastructure.serialization.serializer import Serializer
from shared.infrastructure.storage.filewithversion import FileWithVersion
from shared.infrastructure.storage.repository import AlreadyExistsException
from shared.utils.crockfordid import CrockfordId

@pytest.fixture(autouse=True, scope="module")
def folder_path():
    folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "test_filewithversion_race")
    return folder_path

def add_item(input: tuple[FileWithVersion[str, str, str], str, int]):
    file_repo, id, num = input
    try:
        item = f"test{num}"
        asyncio.get_event_loop().run_until_complete(file_repo.add(id, item))
        return 1
    except AlreadyExistsException:
        return 0

class StrSerializer(Serializer[str]):
    def serialize(self, obj: str) -> str:
        return obj
    def deserialize(self, data: str) -> str:
        return data

@pytest.mark.repeat(10)
def test_add_item_race_condition(folder_path: str):
    serializer: Serializer[str] = StrSerializer()
    file_repo = FileWithVersion[str, str, str](
        str.__name__,
        str,
        str,
        serializer,
        "html",
        folder_path
    )
    id = "add_" + CrockfordId.new_id()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    num_parallel_instances = 100
    try:
        with mp.Pool(4) as pool:
            async_res = pool.map_async(add_item, [(file_repo, id, num) for num in range(1, num_parallel_instances + 1)][::-1])
            res = sum(async_res.get())
    finally:
        loop.close()
        asyncio.set_event_loop(None)
    assert res == 1

def update_item(input: tuple[FileWithVersion[str, str, str], str, int]):
    file_repo, id, num = input
    while True:
        opt_item = asyncio.get_event_loop().run_until_complete(file_repo.get(id))
        if opt_item is None:
            return 0
        ver, prev_items = opt_item
        item = f"test{num}<br/>{prev_items}"
        updated = asyncio.get_event_loop().run_until_complete(file_repo.update(id, ver, item))
        if updated:
            return 1

@pytest.mark.repeat(10)
def test_update_item_race_condition(folder_path: str):
    serializer: Serializer[str] = StrSerializer()
    file_repo = FileWithVersion[str, str, str](
        str.__name__,
        str,
        str,
        serializer,
        "html",
        folder_path
    )
    id = "update_" + CrockfordId.new_id()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    num_parallel_instances = 20
    try:
        add_item((file_repo, id, 0))
        with mp.Pool(4) as pool:
            async_res = pool.map_async(update_item, [(file_repo, id, num) for num in range(1, num_parallel_instances + 1)][::-1])
            res = sum(async_res.get())
        opt_item = asyncio.get_event_loop().run_until_complete(file_repo.get(id))
    finally:
        loop.close()
        asyncio.set_event_loop(None)
    assert res == num_parallel_instances
    assert opt_item is not None
    ver, item = opt_item
    num_br = item.count("<br/>")
    item_numbers = item.split("<br/>")
    numbers = [f"test{num}" for num in range(num_parallel_instances + 1)]
    assert sorted(item_numbers) == sorted(numbers)
    assert ver == (num_parallel_instances + 1)
    assert f"{id}_{num_br}" == f"{id}_{num_parallel_instances}"