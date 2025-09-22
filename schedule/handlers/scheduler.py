from collections.abc import Callable
from typing import Any
from shared.customtypes import ScheduleIdValue
from shared.infrastructure.storage.repository import AlreadyExistsError, AlreadyExistsException, Repository
from shared.infrastructure.storage.repositoryitemaction import ItemActionInRepository
from shared.utils.asyncresult import ex_to_error_result

class Scheduler[TSchedule, TState]:
    def __init__(self, states_storage: Repository[ScheduleIdValue, TState], add_schedule_handler: Callable[[TSchedule,Callable[[], Any]], TState], remove_schedule_handler: Callable[[TState], None]):
        self._states_storage = states_storage
        self._states_storage_item_action = ItemActionInRepository(states_storage)
        self._add_schedule_handler = add_schedule_handler
        self._remove_schedule_handler = remove_schedule_handler
    
    def add(self, schedule_id: ScheduleIdValue, schedule: TSchedule, action_func: Callable[[], Any]):
        @ex_to_error_result(AlreadyExistsError.from_exception, AlreadyExistsException)
        @self._states_storage_item_action
        def add_new_schedule_state(state: TState | None):
            if state is not None:
                raise AlreadyExistsException()
            new_state = self._add_schedule_handler(schedule, action_func)
            return new_state, new_state
        return add_new_schedule_state(schedule_id).default_value(None)
    
    def remove(self, schedule_id: ScheduleIdValue):
        opt_state = self._states_storage.get(schedule_id)
        if opt_state is None:
            return None
        self._remove_schedule_handler(opt_state)
        self._states_storage.delete(schedule_id)
        return opt_state