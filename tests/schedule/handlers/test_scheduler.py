from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from expression import Result
import pytest

from handlers.scheduler import Scheduler
from shared.customtypes import ScheduleIdValue
from shared.infrastructure.storage.inmemory import InMemory
from shared.infrastructure.storage.repository import Repository

@dataclass(frozen=True)
class TestSchedule:
    __test__ = False  # Instruct pytest to ignore this class for test collection
    value: str
@dataclass(frozen=True)
class TestScheduleState:
    __test__ = False  # Instruct pytest to ignore this class for test collection
    schedule: TestSchedule
    action_func: Callable[[], Any]
    @staticmethod
    def create(schedule: TestSchedule, action_func: Callable[[], Any]):
        return TestScheduleState(schedule, action_func)
    @staticmethod
    def remove(state: "TestScheduleState"):
        return None

@pytest.fixture
def states_storage() -> Repository[ScheduleIdValue, TestScheduleState]:
    return InMemory[ScheduleIdValue, TestScheduleState]()

@pytest.fixture
def schedule() -> TestSchedule:
    return TestSchedule(ScheduleIdValue.new_id())

@pytest.fixture(scope="session")
def async_action_func():
    async def action() -> Result:
        return Result.Ok(None)
    return action



def test_add_puts_schedule_state_to_storage(states_storage: Repository[ScheduleIdValue, TestScheduleState], schedule, async_action_func):
    scheduler = Scheduler(states_storage, TestScheduleState.create, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()

    scheduler.add(schedule_id, schedule, async_action_func)
    
    assert states_storage.get(schedule_id) is not None
    


def test_add_starts_schedule(states_storage, async_action_func):
    state = {}
    def add_schedule_handler(schedule: TestSchedule, action_func: Callable[[], Any]):
        state["actual_schedule"] = schedule
        return TestScheduleState.create(schedule, action_func)
    scheduler = Scheduler(states_storage, add_schedule_handler, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()
    expected_schedule = TestSchedule(ScheduleIdValue.new_id())

    scheduler.add(schedule_id, expected_schedule, async_action_func)
    
    assert state["actual_schedule"] == expected_schedule



def test_add_pass_action_func_to_add_schedule_handler(states_storage, schedule):
    state = {}
    def add_schedule_handler(schedule: TestSchedule, action_func: Callable[[], Any]):
        state["actual_action_func"] = action_func
        return TestScheduleState.create(schedule, action_func)
    scheduler = Scheduler(states_storage, add_schedule_handler, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()
    def expected_action_func():
        return None

    scheduler.add(schedule_id, schedule, expected_action_func)
    
    assert state["actual_action_func"] == expected_action_func



def test_add_returns_added_state(states_storage, schedule, async_action_func):
    state = {}
    def add_schedule_handler(schedule: TestSchedule, action_func: Callable[[], Any]):
        schedule_state = TestScheduleState.create(schedule, action_func)
        state["expected_schedule_state"] = schedule_state
        return schedule_state
    scheduler = Scheduler(states_storage, add_schedule_handler, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()

    actual_schedule_state = scheduler.add(schedule_id, schedule, async_action_func)
    
    assert actual_schedule_state == state["expected_schedule_state"]



def test_remove_excludes_schedule_state_from_storage(states_storage: Repository[ScheduleIdValue, TestScheduleState], schedule, async_action_func):
    scheduler = Scheduler(states_storage, TestScheduleState.create, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()
    scheduler.add(schedule_id, schedule, async_action_func)

    scheduler.remove(schedule_id)
    
    assert states_storage.get(schedule_id) is None



def test_remove_stops_schedule(states_storage, schedule, async_action_func):
    state = {}
    def remove_schedule_handler(schedule_state: TestScheduleState):
        state["actual_schedule_state"] = schedule_state
        return None
    scheduler = Scheduler(states_storage, TestScheduleState.create, remove_schedule_handler)
    schedule_id = ScheduleIdValue.new_id()
    expected_schedule_state = scheduler.add(schedule_id, schedule, async_action_func)

    scheduler.remove(schedule_id)
    
    assert state["actual_schedule_state"] == expected_schedule_state



def test_remove_returns_excluded_schedule_state(states_storage, schedule, async_action_func):
    scheduler = Scheduler(states_storage, TestScheduleState.create, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()
    expected_schedule_state = scheduler.add(schedule_id, schedule, async_action_func)

    actual_schedule_state = scheduler.remove(schedule_id)

    assert actual_schedule_state == expected_schedule_state



def test_add_when_existing_schedule_then_does_not_put_schedule_state_to_storage(states_storage: Repository[ScheduleIdValue, TestScheduleState], schedule, async_action_func):
    scheduler = Scheduler(states_storage, TestScheduleState.create, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()
    expected_schedule_state = scheduler.add(schedule_id, schedule, async_action_func)
    
    actual_schedule_state = scheduler.add(schedule_id, schedule, async_action_func)
    
    assert actual_schedule_state != expected_schedule_state
    assert states_storage.get(schedule_id) == expected_schedule_state



def test_add_when_existing_schedule_then_returns_none(states_storage, schedule, async_action_func):
    scheduler = Scheduler(states_storage, TestScheduleState.create, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()
    scheduler.add(schedule_id, schedule, async_action_func)
    
    actual_schedule_state = scheduler.add(schedule_id, schedule, async_action_func)

    assert actual_schedule_state is None



def test_remove_when_schedule_not_found_then_returns_none(states_storage, schedule, async_action_func):
    scheduler = Scheduler(states_storage, TestScheduleState.create, TestScheduleState.remove)
    schedule_id = ScheduleIdValue.new_id()

    actual_schedule_state = scheduler.remove(schedule_id)

    assert actual_schedule_state is None
