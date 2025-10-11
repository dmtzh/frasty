import asyncio
import multiprocessing as mp
import os

import pytest

import config
from shared.infrastructure.serialization.serializer import Serializer
import shared.infrastructure.storage.file as file
from shared.infrastructure.storage.repository import AlreadyExistsException
from shared.utils.crockfordid import CrockfordId

File = file.File[str, str, str]

@pytest.fixture(autouse=True, scope="module")
def folder_path():
    folder_path = os.path.join(config.STORAGE_ROOT_FOLDER, "test_file_race")
    return folder_path

def add_item(file_storage: File, id: str, num: int):
    try:
        item = str(num)
        asyncio.get_event_loop().run_until_complete(file_storage.add(id, item))
        return num
    except AlreadyExistsException:
        return 0

class StrSerializer(Serializer[str]):
    def serialize(self, obj: str) -> str:
        return obj
    def deserialize(self, data: str) -> str:
        return data

def create_file_storage(folder_path: str, serializer: Serializer[str]):
    return File(
        str.__name__,
        str,
        str,
        serializer,
        "html",
        folder_path
    )    



@pytest.mark.repeat(10)
def test_add_item_race_condition(folder_path: str):
    serializer: Serializer[str] = StrSerializer()
    id = "add_" + CrockfordId.new_id()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    num_parallel_instances = 100

    try:
        items = [(create_file_storage(folder_path, serializer), id, num) for num in range(1, num_parallel_instances + 1)][::-1]
        with mp.Pool(4) as pool:
            async_res = pool.starmap_async(add_item, items)
            actual_num = str(sum(async_res.get()))
        expected_num = loop.run_until_complete(create_file_storage(folder_path, serializer).get(id))
    finally:
        loop.close()
        asyncio.set_event_loop(None)

    assert actual_num == expected_num



def update_item(file_storage: File, id: str, num: int):
    while True:
        prev_items = asyncio.get_event_loop().run_until_complete(file_storage.get(id))
        if prev_items is None:
            return 0
        item = f"{num}<br/>{prev_items}"
        asyncio.get_event_loop().run_until_complete(file_storage.update(id, item))
        return 1



@pytest.mark.repeat(10)
def test_update_item_race_condition(folder_path: str):
    serializer: Serializer[str] = StrSerializer()
    id = "update_" + CrockfordId.new_id()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    num_parallel_instances = 20

    try:
        file_repo = create_file_storage(folder_path, serializer)
        add_item(file_repo, id, 0)
        with mp.Pool(4) as pool:
            async_res = pool.starmap_async(update_item, [(create_file_storage(folder_path, serializer), id, num) for num in range(1, num_parallel_instances + 1)][::-1])
            actual_num_of_updates = sum(async_res.get())
        item = loop.run_until_complete(file_repo.get(id))
    finally:
        loop.close()
        asyncio.set_event_loop(None)
    
    assert actual_num_of_updates == num_parallel_instances
    assert item is not None
    actual_numbers = item.split("<br/>")
    expected_numbers = [str(num) for num in range(num_parallel_instances + 1)]
    assert sorted(actual_numbers) == expected_numbers