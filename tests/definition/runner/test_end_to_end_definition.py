import pytest
from collections import deque
from typing import Any
from expression import Result
from shared.action import Action, ActionName, ActionType
from shared.definitioncustomtypes import GroupIdValue
from shared.executedefinitionaction import EXECUTE_DEFINITION_ACTION
from shared.groupofrunningdefinitions import GroupOfRunningDefinitionsState
from shared.pipeline.actionhandler import ActionData, ActionHandlerFactory, ActionInput, DataDto
from shared.customtypes import RunIdValue, StepIdValue, DefinitionIdValue
from shared.completedresult import CompletedResultAdapter, CompletedWith
from config import running_definitions_storage, group_of_running_definitions_storage
from runner.completeaction.registration import register_complete_action_handler
from runner.executedefinition.registration import register_execute_definition_action_handler
from shared.runningdefinition import RunningDefinitionState

class InMemoryMessageBus:
    """In-memory transport layer simulation for E2E testing."""
    def __init__(self):
        self._handlers: dict[str, Any] = {}
        self._captured_run_calls = deque[tuple[str, ActionInput]]()

    def action_handler(self, action_name: str, handler):
        # Registers pipeline handler for incoming messages
        self._handlers[action_name] = handler

    async def run_action(self, action_name: str, action_input: ActionInput) -> Result[None, Any]:
        # Captures outbound execution requests; simulates async worker pickup
        self._captured_run_calls.append((action_name,action_input))
        return Result.Ok(None)
    
    async def dispatch_captured(self):
        while self._captured_run_calls:
            action_name, action_input = self._captured_run_calls.popleft()
            await self.dispatch(action_name, action_input)

    async def dispatch(self, action_name: str, payload: ActionInput):
        # Simulates message delivery to registered pipeline handler
        if action_name in self._handlers:
            return await self._handlers[action_name](Result.Ok(payload))
        raise ValueError(f"No handler registered for {action_name}")

@pytest.fixture
def message_bus():
    return InMemoryMessageBus()

@pytest.fixture(autouse=True)
def register_handlers(message_bus):
    # Wire registrations to in-memory transport; return values intentionally ignored
    register_execute_definition_action_handler(message_bus.run_action, message_bus.action_handler)
    register_complete_action_handler(message_bus.run_action, message_bus.action_handler)
    async def wait_before_process_handler(action_data: ActionData[None, list[DataDto]]):
        match action_data.input[0].get("duration_ms", None):
            case 2000:
                return CompletedWith.NoData()
            case 3000:
                return CompletedWith.Error("timeout")
            case _:
                return CompletedWith.Data(action_data.input)
    ActionHandlerFactory(message_bus.run_action, message_bus.action_handler).create_without_config(
        Action(ActionName("waitbeforeprocess"), ActionType.CUSTOM),
        lambda dto_list: Result.Ok(dto_list)
    )(wait_before_process_handler)

@pytest.fixture
def initial_execute_payload_single_definition():
    run_id = RunIdValue.new_id()
    step_id = StepIdValue.new_id()
    # Payload matches ExecuteDefinitionInput contract
    data = {
        "input_data": [
            {"definition_id": "DHJ45Q8EW", "definition": [{"action": "waitbeforeprocess", "duration_ms": 1000}]}
        ]
    }
    return ActionInput(
        run_id=run_id.to_value_with_checksum(),
        step_id=step_id.to_value_with_checksum(),
        data=data,
        metadata={}
    )

@pytest.fixture
def initial_execute_payload():
    run_id = RunIdValue.new_id()
    step_id = StepIdValue.new_id()
    # Payload matches ExecuteGroupOfDefinitionsInput contract
    data = {
        "input_data": [
            {"definition_id": "DHJ45Q8EW", "definition": [{"action": "waitbeforeprocess", "duration_ms": 1000}]},
            {"definition_id": "WBRBKM2BF", "definition": [{"action": "waitbeforeprocess", "duration_ms": 2000}]},
            {"definition_id": "CTZJAQEQ9", "definition": [{"action": "waitbeforeprocess", "duration_ms": 3000}]}
        ]
    }
    return ActionInput(
        run_id=run_id.to_value_with_checksum(),
        step_id=step_id.to_value_with_checksum(),
        data=data,
        metadata={}
    )

async def _read_running_state(run_id: RunIdValue, definition_id: DefinitionIdValue) -> RunningDefinitionState | None:
    # Reads current state directly from storage to verify E2E transitions
    state_container = deque[RunningDefinitionState]()
    def capture_state(state):
        state_container.append(state)
        return (None, state)
    await running_definitions_storage.with_storage(capture_state)(run_id, definition_id)
    return state_container.popleft() if state_container else None

async def _read_group_state(run_id: RunIdValue, group_id: GroupIdValue) -> GroupOfRunningDefinitionsState | None:
    # Reads current state directly from storage to verify E2E transitions
    state_container = deque[GroupOfRunningDefinitionsState]()
    def capture_state(state):
        state_container.append(state)
        return (None, state)
    await group_of_running_definitions_storage.with_storage(capture_state)(run_id, group_id)
    return state_container.popleft() if state_container else None



async def test_e2e_execute_and_complete_single_definition(message_bus, initial_execute_payload_single_definition):
    run_id = RunIdValue.from_value_with_checksum(initial_execute_payload_single_definition.run_id)
    definition_id = DefinitionIdValue.from_value_with_checksum(initial_execute_payload_single_definition.data["input_data"][0]["definition_id"])
    
    # 1. Dispatch execute definition payload
    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), initial_execute_payload_single_definition)
    await message_bus.dispatch_captured()
    
    # 2. Verify definition completed
    assert run_id is not None
    assert definition_id is not None
    running_state = await _read_running_state(run_id, definition_id)
    assert running_state is not None
    definition_completed_evt = next((e for e in running_state.get_events() if type(e) is RunningDefinitionState.Events.DefinitionCompleted), None)
    assert definition_completed_evt is not None



async def test_e2e_execute_and_complete_group_of_definitions(message_bus, initial_execute_payload):
    run_id = RunIdValue.from_value_with_checksum(initial_execute_payload.run_id)
    group_id = GroupIdValue.from_value_with_checksum(initial_execute_payload.step_id) # GroupIdValue inherits StepIdValue
    
    # 1. Dispatch execute group payload
    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), initial_execute_payload)
    await message_bus.dispatch_captured()

    # 2. Verify all definitions completed and group reached terminal state
    assert run_id is not None
    assert group_id is not None
    state_after_all = await _read_group_state(run_id, group_id)
    assert state_after_all is not None
    # Verify journal contains terminal marker
    events = state_after_all.get_events()
    definition_completed_events = [e for e in events if type(e) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted]
    terminal_event = next((e for e in events if type(e) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted), None)
    assert len(definition_completed_events) == 3
    assert terminal_event is not None



async def test_e2e_execute_and_complete_group_of_definitions_inside_another_single_definition(message_bus, initial_execute_payload):
    single_definition_id = DefinitionIdValue.new_id()
    single_definition = [
        {
            "action": EXECUTE_DEFINITION_ACTION.name,
            "type": ActionType.CORE
        } | initial_execute_payload.data,
        {"action": "waitbeforeprocess", "duration_ms": 5000}
    ]
    data = {
        "input_data": [
            {
                "definition_id": single_definition_id.to_value_with_checksum(),
                "definition": single_definition
            }
        ]
    }
    execute_payload = ActionInput(
        run_id=initial_execute_payload.run_id,
        step_id=StepIdValue.new_id().to_value_with_checksum(),
        data=data,
        metadata={}
    )
    run_id = RunIdValue.from_value_with_checksum(execute_payload.run_id)
    
    # 1. Dispatch execute definition payload
    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), execute_payload)
    await message_bus.dispatch_captured()

    # 2. Verify definition completed
    assert run_id is not None
    running_state = await _read_running_state(run_id, single_definition_id)
    assert running_state is not None
    definition_completed_evt = next((e for e in running_state.get_events() if type(e) is RunningDefinitionState.Events.DefinitionCompleted), None)
    assert definition_completed_evt is not None
    assert type(definition_completed_evt.result) is CompletedWith.Data
    assert type(definition_completed_evt.result.data) is list
    assert len(definition_completed_evt.result.data) == 3



async def test_e2e_execute_and_complete_single_definition_with_data_result_inside_another_single_definition(message_bus, initial_execute_payload):
    single_definition_id = DefinitionIdValue.new_id()
    single_definition = [
        {
            "action": EXECUTE_DEFINITION_ACTION.name,
            "type": ActionType.CORE,
            "input_data": [initial_execute_payload.data["input_data"][0]]
        }
    ]
    data = {
        "input_data": [
            {
                "definition_id": single_definition_id.to_value_with_checksum(),
                "definition": single_definition
            }
        ]
    }
    execute_payload = ActionInput(
        run_id=initial_execute_payload.run_id,
        step_id=StepIdValue.new_id().to_value_with_checksum(),
        data=data,
        metadata={}
    )
    run_id = RunIdValue.from_value_with_checksum(execute_payload.run_id)
    
    # 1. Dispatch execute definition payload
    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), execute_payload)
    await message_bus.dispatch_captured()

    # 2. Verify definition completed
    assert run_id is not None
    running_state = await _read_running_state(run_id, single_definition_id)
    assert running_state is not None
    definition_completed_evt = next((e for e in running_state.get_events() if type(e) is RunningDefinitionState.Events.DefinitionCompleted), None)
    assert definition_completed_evt is not None
    assert type(definition_completed_evt.result) is CompletedWith.Data
    assert type(definition_completed_evt.result.data) is dict
    assert "definition_id" in definition_completed_evt.result.data
    data_to_completed_result_res = CompletedResultAdapter.from_dict(definition_completed_evt.result.data)
    assert data_to_completed_result_res.is_ok()
    assert type(data_to_completed_result_res.ok) is CompletedWith.Data



async def test_e2e_execute_and_complete_single_definition_with_no_data_result_inside_another_single_definition(message_bus, initial_execute_payload):
    single_definition_id = DefinitionIdValue.new_id()
    single_definition = [
        {
            "action": EXECUTE_DEFINITION_ACTION.name,
            "type": ActionType.CORE,
            "input_data": [initial_execute_payload.data["input_data"][1]]
        }
    ]
    data = {
        "input_data": [
            {
                "definition_id": single_definition_id.to_value_with_checksum(),
                "definition": single_definition
            }
        ]
    }
    execute_payload = ActionInput(
        run_id=initial_execute_payload.run_id,
        step_id=StepIdValue.new_id().to_value_with_checksum(),
        data=data,
        metadata={}
    )
    run_id = RunIdValue.from_value_with_checksum(execute_payload.run_id)
    
    # 1. Dispatch execute definition payload
    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), execute_payload)
    await message_bus.dispatch_captured()

    # 2. Verify definition completed
    assert run_id is not None
    running_state = await _read_running_state(run_id, single_definition_id)
    assert running_state is not None
    definition_completed_evt = next((e for e in running_state.get_events() if type(e) is RunningDefinitionState.Events.DefinitionCompleted), None)
    assert definition_completed_evt is not None
    assert type(definition_completed_evt.result) is CompletedWith.Data
    assert type(definition_completed_evt.result.data) is dict
    assert "definition_id" in definition_completed_evt.result.data
    data_to_completed_result_res = CompletedResultAdapter.from_dict(definition_completed_evt.result.data)
    assert data_to_completed_result_res.is_ok()
    assert type(data_to_completed_result_res.ok) is CompletedWith.NoData



async def test_e2e_execute_and_complete_single_definition_with_error_result_inside_another_single_definition(message_bus, initial_execute_payload):
    single_definition_id = DefinitionIdValue.new_id()
    single_definition = [
        {
            "action": EXECUTE_DEFINITION_ACTION.name,
            "type": ActionType.CORE,
            "input_data": [initial_execute_payload.data["input_data"][2]]
        }
    ]
    data = {
        "input_data": [
            {
                "definition_id": single_definition_id.to_value_with_checksum(),
                "definition": single_definition
            }
        ]
    }
    execute_payload = ActionInput(
        run_id=initial_execute_payload.run_id,
        step_id=StepIdValue.new_id().to_value_with_checksum(),
        data=data,
        metadata={}
    )
    run_id = RunIdValue.from_value_with_checksum(execute_payload.run_id)
    
    # 1. Dispatch execute definition payload
    await message_bus.run_action(EXECUTE_DEFINITION_ACTION.get_name(), execute_payload)
    await message_bus.dispatch_captured()

    # 2. Verify definition completed
    assert run_id is not None
    running_state = await _read_running_state(run_id, single_definition_id)
    assert running_state is not None
    definition_completed_evt = next((e for e in running_state.get_events() if type(e) is RunningDefinitionState.Events.DefinitionCompleted), None)
    assert definition_completed_evt is not None
    assert type(definition_completed_evt.result) is CompletedWith.Data
    assert type(definition_completed_evt.result.data) is dict
    assert "definition_id" in definition_completed_evt.result.data
    data_to_completed_result_res = CompletedResultAdapter.from_dict(definition_completed_evt.result.data)
    assert data_to_completed_result_res.is_ok()
    assert type(data_to_completed_result_res.ok) is CompletedWith.Error