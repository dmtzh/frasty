"""E2E regression tests: definition_id attribution in execute_definition results.

Public contracts only: register_execute_definition_action_handler,
register_complete_action_handler, ActionHandlerFactory.create_without_config,
ActionInput wire payloads, storage state reads. No internal handlers are called.

Regression: single-mode execute_definition wrapped child results with the
PARENT's definition_id instead of the completing child's own id, while
group mode already used each child's own id.
"""
from dataclasses import dataclass

import pytest
from collections import deque
from typing import Any
from expression import Result
from shared.action import Action, ActionName, ActionType
from shared.executedefinitionaction import EXECUTE_DEFINITION_ACTION
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, ActionInput, DataDto
from shared.customtypes import RunIdValue, StepIdValue, DefinitionIdValue
from shared.completedresult import CompletedResultAdapter, CompletedResult, CompletedWith
from config import running_definitions_storage
from runner.completeaction.registration import register_complete_action_handler
from runner.executedefinition.registration import register_execute_definition_action_handler
from shared.runningdefinition import RunningDefinitionState


class InMemoryMessageBus:
    """In-memory transport layer simulation (same pattern as test_end_to_end_definition)."""
    def __init__(self):
        self._handlers: dict[str, Any] = {}
        self._captured_run_calls = deque()

    def action_handler(self, action_name: str, handler):
        self._handlers[action_name] = handler

    async def run_action(self, action_name: str, action_input: ActionInput) -> Result:
        self._captured_run_calls.append((action_name, action_input))
        return Result.Ok(None)

    async def dispatch_captured(self):
        while self._captured_run_calls:
            action_name, action_input = self._captured_run_calls.popleft()
            await self.dispatch(action_name, action_input)

    async def dispatch(self, action_name: str, payload: ActionInput):
        if action_name in self._handlers:
            return await self._handlers[action_name](Result.Ok(payload))
        raise ValueError(f"No handler registered for {action_name}")


@pytest.fixture
def message_bus():
    return InMemoryMessageBus()


@pytest.fixture(autouse=True)
def register_handlers(message_bus):
    register_execute_definition_action_handler(message_bus.run_action, message_bus.action_handler)
    register_complete_action_handler(message_bus.run_action, message_bus.action_handler)

    async def wait_before_process_handler(action_data: ActionData[None, list[DataDto]]):
        match action_data.input[0].get("test_value", None):
            case 2000:
                return CompletedWith.NoData()
            case 3000:
                return CompletedWith.Error("test error")
            case _:
                return CompletedWith.Data(action_data.input)

    ActionHandlerFactory(message_bus.run_action, message_bus.action_handler).create_without_config(
        Action(ActionName("test_end_to_end_definition_id"), ActionType.CUSTOM),
        lambda dto_list: Result.Ok(dto_list)
    )(wait_before_process_handler)

@dataclass(frozen=True)
class ActionInputWrapper(ActionInput):
    run_id_val: RunIdValue

def _execute_payload(input_dtos: list[dict]) -> ActionInputWrapper:
    run_id_val = RunIdValue.new_id()
    return ActionInputWrapper(
        run_id=run_id_val.to_value_with_checksum(),
        step_id=StepIdValue.new_id().to_value_with_checksum(),
        data={"input_data": input_dtos},
        metadata={},
        run_id_val=run_id_val
    )


def _child_dto(definition_id: DefinitionIdValue, definition: list[dict]) -> dict:
    return {"definition_id": definition_id.to_value_with_checksum(), "definition": definition}


def _execute_step(input_dtos: list[dict]) -> dict:
    return {"action": EXECUTE_DEFINITION_ACTION.name, "type": ActionType.CORE, "input_data": input_dtos}


async def _completed_result(run_id: RunIdValue, definition_id: DefinitionIdValue) -> CompletedResult:
    state_container = deque()
    def capture_state(state):
        state_container.append(state)
        return (None, state)
    await running_definitions_storage.with_storage(capture_state)(run_id, definition_id)
    assert state_container, "running state not found in storage"
    state = state_container.popleft()
    evt = next((e for e in state.get_events() if type(e) is RunningDefinitionState.Events.DefinitionCompleted), None)
    assert evt is not None, "definition did not reach DefinitionCompleted"
    return evt.result


async def test_single_mode_result_carries_child_own_definition_id(message_bus):
    outer_id = DefinitionIdValue.new_id()
    child_id = DefinitionIdValue.new_id()
    child_definition = [{"action": "test_end_to_end_definition_id", "test_value": 1000}]
    outer_definition = [_execute_step([_child_dto(child_id, child_definition)])]
    payload = _execute_payload([_child_dto(outer_id, outer_definition)])
    run_id = payload.run_id_val

    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), payload)
    await message_bus.dispatch_captured()

    result = await _completed_result(run_id, outer_id)
    assert type(result) is CompletedWith.Data
    assert type(result.data) is list
    assert len(result.data) == 1
    wrapper = result.data[0]
    # completing child's OWN id, not the parent's
    assert wrapper["definition_id"] == child_id.to_value_with_checksum()
    assert wrapper["definition_id"] != outer_id.to_value_with_checksum()
    inner_res = CompletedResultAdapter.from_dict(wrapper)
    assert inner_res.is_ok()
    assert type(inner_res.ok) is CompletedWith.Data


async def test_nested_single_modes_each_level_carries_own_definition_id(message_bus):
    top_id = DefinitionIdValue.new_id()
    middle_id = DefinitionIdValue.new_id()
    grandchild_id = DefinitionIdValue.new_id()
    grandchild_definition = [{"action": "test_end_to_end_definition_id", "test_value": 1000}]
    middle_definition = [_execute_step([_child_dto(grandchild_id, grandchild_definition)])]
    top_definition = [_execute_step([_child_dto(middle_id, middle_definition)])]
    payload = _execute_payload([_child_dto(top_id, top_definition)])
    run_id = payload.run_id_val

    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), payload)
    await message_bus.dispatch_captured()

    result = await _completed_result(run_id, top_id)
    assert type(result) is CompletedWith.Data
    middle_wrapper = result.data[0]
    assert middle_wrapper["definition_id"] == middle_id.to_value_with_checksum()
    grandchild_wrapper = middle_wrapper["data"][0]
    assert grandchild_wrapper["definition_id"] == grandchild_id.to_value_with_checksum()


async def test_single_mode_nodata_result_carries_child_own_definition_id(message_bus):
    outer_id = DefinitionIdValue.new_id()
    child_id = DefinitionIdValue.new_id()
    child_definition = [{"action": "test_end_to_end_definition_id", "test_value": 2000}]
    outer_definition = [_execute_step([_child_dto(child_id, child_definition)])]
    payload = _execute_payload([_child_dto(outer_id, outer_definition)])
    run_id = payload.run_id_val

    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), payload)
    await message_bus.dispatch_captured()

    result = await _completed_result(run_id, outer_id)
    assert type(result) is CompletedWith.Data
    wrapper = result.data[0]
    assert wrapper["definition_id"] == child_id.to_value_with_checksum()
    inner_res = CompletedResultAdapter.from_dict(wrapper)
    assert inner_res.is_ok()
    assert type(inner_res.ok) is CompletedWith.NoData


async def test_single_mode_error_result_carries_child_own_definition_id(message_bus):
    outer_id = DefinitionIdValue.new_id()
    child_id = DefinitionIdValue.new_id()
    child_definition = [{"action": "test_end_to_end_definition_id", "test_value": 3000}]
    outer_definition = [_execute_step([_child_dto(child_id, child_definition)])]
    payload = _execute_payload([_child_dto(outer_id, outer_definition)])
    run_id = payload.run_id_val

    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), payload)
    await message_bus.dispatch_captured()

    result = await _completed_result(run_id, outer_id)
    assert type(result) is CompletedWith.Data
    wrapper = result.data[0]
    assert wrapper["definition_id"] == child_id.to_value_with_checksum()
    inner_res = CompletedResultAdapter.from_dict(wrapper)
    assert inner_res.is_ok()
    assert type(inner_res.ok) is CompletedWith.Error


async def test_group_mode_results_carry_each_child_own_definition_id(message_bus):
    outer_id = DefinitionIdValue.new_id()
    child_a = DefinitionIdValue.new_id()
    child_b = DefinitionIdValue.new_id()
    dtos = [
        _child_dto(child_a, [{"action": "test_end_to_end_definition_id", "test_value": 1000}]),
        _child_dto(child_b, [{"action": "test_end_to_end_definition_id", "test_value": 1000}]),
    ]
    outer_definition = [_execute_step(dtos)]
    payload = _execute_payload([_child_dto(outer_id, outer_definition)])
    run_id = payload.run_id_val

    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), payload)
    await message_bus.dispatch_captured()

    result = await _completed_result(run_id, outer_id)
    assert type(result) is CompletedWith.Data
    assert type(result.data) is list
    assert len(result.data) == 2
    ids = [w["definition_id"] for w in result.data]
    assert set(ids) == {child_a.to_value_with_checksum(), child_b.to_value_with_checksum()}


async def test_single_and_group_wrappers_are_identical_for_same_child(message_bus):
    child_id = DefinitionIdValue.new_id()
    other_id = DefinitionIdValue.new_id()
    child_definition = [{"action": "test_end_to_end_definition_id", "test_value": 1000}]

    # Run A: single mode with child_id
    outer_single_id = DefinitionIdValue.new_id()
    payload_single = _execute_payload([
        _child_dto(outer_single_id, [_execute_step([_child_dto(child_id, child_definition)])])
    ])
    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), payload_single)
    await message_bus.dispatch_captured()
    single_result = await _completed_result(payload_single.run_id_val, outer_single_id)
    assert type(single_result) is CompletedWith.Data
    single_wrapper = single_result.data[0]

    # Run B: group mode with the same child_id plus one more child
    outer_group_id = DefinitionIdValue.new_id()
    payload_group = _execute_payload([
        _child_dto(outer_group_id, [_execute_step([
            _child_dto(child_id, child_definition),
            _child_dto(other_id, child_definition),
        ])])
    ])
    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), payload_group)
    await message_bus.dispatch_captured()
    run_id = payload_group.run_id_val
    group_result = await _completed_result(run_id, outer_group_id)
    assert type(group_result) is CompletedWith.Data
    assert type(group_result.data) is list
    group_wrapper = next(w for w in group_result.data if w["definition_id"] == child_id.to_value_with_checksum())

    # full structural equality: same child -> identical wrapper in both modes
    assert single_wrapper == group_wrapper
