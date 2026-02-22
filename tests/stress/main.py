import asyncio
from collections.abc import Callable, Generator
import functools
import time
from typing import Any

from shared.customtypes import RunIdValue, TaskIdValue
from shared.infrastructure.storage.inmemory import InMemory
from shared.infrastructure.storage.repositoryitemaction import ItemActionInRepository

from config import lifespan, run_stress_test_task

def get_tasks_to_run(task_ids: list[TaskIdValue], num_of_tasks: int):
    task_count = 0
    while True:
        for task_id in task_ids:
            yield (task_id, RunIdValue.new_id())
            task_count += 1
            if task_count == num_of_tasks:
                return

class RunningTasksStore:
    def __init__(self, num_of_tasks: int):
        self._in_memory_repo = InMemory[str, dict]()
        self._in_memory_repo.add("RUNNING_TASKS", {"num_of_completed_tasks": 0})
        self._item_action = ItemActionInRepository(self._in_memory_repo)
    
    def add(self, run_id: RunIdValue):
        def add_run_id(item: dict | None):
            item = item or {}
            item[run_id] = True
            return len(item) - 1, item
        return self._item_action(add_run_id)("RUNNING_TASKS")
    
    def get_num_of_running_tasks(self):
        return len(self._in_memory_repo.get("RUNNING_TASKS") or {}) - 1
    
    def remove(self, run_id: RunIdValue):
        def remove_run_id(item: dict | None):
            item = item or {}
            if run_id in item:
                del item[run_id]
                item["num_of_completed_tasks"] += 1
            return item["num_of_completed_tasks"], item
        return self._item_action(remove_run_id)("RUNNING_TASKS")

def get_elapsed_time(start_time, end_time):
    seconds = end_time - start_time
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    match minutes:
        case 0:
            return f"{remaining_seconds} seconds"
        case 1:
            return f"{minutes} minute {remaining_seconds} seconds"
        case _:
            return f"{minutes} minutes {remaining_seconds} seconds"

async def main(state: dict, get_tasks_to_run: Callable[[], Generator[tuple[TaskIdValue, RunIdValue], Any, None]], running_tasks_storage: RunningTasksStore, max_concurrent_tasks: int):
    await lifespan.__aenter__()

    print("------------------------------------------")
    print("Tasks started...")
    print("------------------------------------------")

    state["start_time"] = time.time()

    for task_id, run_id in get_tasks_to_run():
        num_of_running = running_tasks_storage.add(run_id)
        while (num_of_running > max_concurrent_tasks):
            await asyncio.sleep(1)
            num_of_running = running_tasks_storage.get_num_of_running_tasks()
        await run_stress_test_task(task_id, run_id)

    await state["all_tasks_completed"]

    end_time = time.time()
    elapsed_time = get_elapsed_time(state["start_time"], end_time)
    print("------------------------------------------")
    print(f"All tasks completed in {elapsed_time}")
    print("------------------------------------------")

    await lifespan.__aexit__(None, None, None)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run tasks stress test")
    parser.add_argument("num_of_tasks", help="Number of tasks to run")
    parser.add_argument("max_concurrent_tasks", help="Max number of simultaneously running tasks")
    parser.add_argument("-tids", "--task_ids", nargs="*", required=True, help="Task ids to run")

    args = parser.parse_args()
    num_of_tasks = int(args.num_of_tasks)
    max_concurrent_tasks = int(args.max_concurrent_tasks)
    tasks_ids = [task_id for task_id in map(TaskIdValue.from_value_with_checksum, args.task_ids) if task_id is not None]
    if len(tasks_ids) == 0:
        raise ValueError("No valid task ids provided")

    state: dict = {
        "count_of_completed_tasks_for_intermediate_info": 250,
        "num_of_tasks": num_of_tasks
    }

    # @stress_test_definition_completed_subscriber
    async def remove_completed_task(data):
        num_of_completed_tasks = running_tasks_storage.remove(data.run_id)
        print_intermediate_info = num_of_completed_tasks % state["count_of_completed_tasks_for_intermediate_info"] == 0
        if print_intermediate_info:
            end_time = time.time()
            elapsed_time = get_elapsed_time(state["start_time"], end_time)
            print("------------------------------------------")
            print(f"{num_of_completed_tasks} of {state['num_of_tasks']} tasks completed in {elapsed_time}")
            print("------------------------------------------")
        if (num_of_completed_tasks == state["num_of_tasks"]):
            state["all_tasks_completed"].set_result(True)

    running_tasks_storage = RunningTasksStore(num_of_tasks)
    get_tasks = functools.partial(get_tasks_to_run, task_ids=tasks_ids, num_of_tasks=num_of_tasks)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state["all_tasks_completed"] = asyncio.Future(loop=loop)
    loop.run_until_complete(main(state, get_tasks, running_tasks_storage, max_concurrent_tasks))
