from collections.abc import Callable
import functools
from typing import Concatenate
from expression import Result
import pytest

from runner.completeaction.completegroupdefinitionhandler import (
    CompleteGroupDefinitionCommand,
    CompletAllDefinitionsError,
    handle as handle_complete_group_definition
)
from shared.action import ActionName, ActionType
from shared.completedresult import CompletedWith
from shared.customtypes import DefinitionIdValue, Error, RunIdValue, StepIdValue
from shared.definition import ActionDefinition, Definition
from shared.definitioncustomtypes import GroupIdValue
from shared.groupofrunningdefinitions import GroupOfRunningDefinitionsState, DefinitionIdWithValue

from config import group_of_running_definitions_storage

async def all_defs_completed_event_handler(evt):
    assert type(evt) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted
    return Result.Ok(None)

async def runtime_error_event_handler(evt):
    raise RuntimeError()

@pytest.fixture
def convert_to_storage_action():
    return group_of_running_definitions_storage.with_storage

@pytest.fixture
def two_definition_group():
    def_id_1 = DefinitionIdValue.new_id()
    def_id_2 = DefinitionIdValue.new_id()
    step_1 = ActionDefinition(ActionName("requesturl"), ActionType.CUSTOM, None)
    step_2 = ActionDefinition(ActionName("parseresponse"), ActionType.CUSTOM, None)
    definition_1 = Definition({"url": "http://a"}, (step_1,))
    definition_2 = Definition({"url": "http://b"}, (step_2,))
    return (def_id_1, definition_1, def_id_2, definition_2)

@pytest.fixture
def create_complete_group_cmd(convert_to_storage_action, two_definition_group):
    async def wrapper(storage_setup_func: Callable[[GroupOfRunningDefinitionsState | None, dict], tuple[GroupOfRunningDefinitionsState.Events.Event | None, GroupOfRunningDefinitionsState]]):
        run_id = RunIdValue.new_id()
        group_id = GroupIdValue(StepIdValue.new_id())
        
        cmd_dict = {
            "run_id": run_id,
            "group_id": group_id,
            "definition_id": two_definition_group[0],
            "step_id": StepIdValue.new_id(),
            "result": CompletedWith.Data({"final": "result"})
        }
        
        evt = await convert_to_storage_action(storage_setup_func)(run_id, group_id, cmd_dict)
        
        cmd = CompleteGroupDefinitionCommand(
            cmd_dict["run_id"], 
            cmd_dict["group_id"], 
            cmd_dict["step_id"], 
            cmd_dict["definition_id"], 
            cmd_dict["result"]
        )
        return (evt, cmd)
    return wrapper

@pytest.fixture
def handle(convert_to_storage_action):
    return functools.partial(handle_complete_group_definition, convert_to_storage_action)



async def test_handle_returns_all_definitions_completed_event_when_last_def_completed(create_complete_group_cmd, handle, two_definition_group):
    def_id_1, def_1, def_id_2, def_2 = two_definition_group
    
    def setup_second_def_running(state: GroupOfRunningDefinitionsState | None, cmd_dict: dict):
        s = GroupOfRunningDefinitionsState()
        defs = (
            DefinitionIdWithValue(def_id_1, def_1), 
            DefinitionIdWithValue(def_id_2, def_2)
        )
        s.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        s.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
        
        # Complete first definition
        first_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_1)
        s.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, def_id_1, CompletedWith.Data("step1_res")))
        
        # Target second definition
        second_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_2)
        cmd_dict["step_id"] = second_step_id
        cmd_dict["definition_id"] = def_id_2
        cmd_dict["result"] = CompletedWith.Data("final_res")
        
        # Do NOT apply completion here; handler will apply it
        return (None, s)
        
    pre_evt, cmd = await create_complete_group_cmd(setup_second_def_running)
    
    handle_res = await handle(all_defs_completed_event_handler, cmd)
    
    assert pre_evt is None
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted
    assert len(evt.results) == 2



async def test_handle_returns_definition_completed_event_when_not_terminal(create_complete_group_cmd, handle, two_definition_group):
    def_id_1, def_1, def_id_2, def_2 = two_definition_group
    
    def setup_both_running(state: GroupOfRunningDefinitionsState | None, cmd_dict: dict):
        s = GroupOfRunningDefinitionsState()
        defs = (
            DefinitionIdWithValue(def_id_1, def_1), 
            DefinitionIdWithValue(def_id_2, def_2)
        )
        s.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        s.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
        
        first_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_1)
        cmd_dict["step_id"] = first_step_id
        cmd_dict["definition_id"] = def_id_1
        cmd_dict["result"] = CompletedWith.Data("step1_res")
        return (None, s)
        
    pre_evt, cmd = await create_complete_group_cmd(setup_both_running)
    
    handle_res = await handle(runtime_error_event_handler, cmd)
    
    assert pre_evt is None
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    evt = handle_res.ok
    assert type(evt) is GroupOfRunningDefinitionsState.Events.DefinitionCompleted



async def test_handle_passes_correct_data_to_all_definitions_completed_event_handler(create_complete_group_cmd, handle, two_definition_group):
    def_id_1, def_1, def_id_2, def_2 = two_definition_group
    passed_result = {}
    
    async def capturing_handler(evt: GroupOfRunningDefinitionsState.Events.AllDefinitionsCompleted):
        passed_result["data"] = evt.results
        return Result.Ok(None)
        
    def setup_second_def_running(state: GroupOfRunningDefinitionsState | None, cmd_dict: dict):
        s = GroupOfRunningDefinitionsState()
        defs = (
            DefinitionIdWithValue(def_id_1, def_1), 
            DefinitionIdWithValue(def_id_2, def_2)
        )
        s.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        s.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
        
        first_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_1)
        s.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, def_id_1, CompletedWith.Data("step1_res")))
        
        second_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_2)
        cmd_dict["step_id"] = second_step_id
        cmd_dict["definition_id"] = def_id_2
        cmd_dict["result"] = CompletedWith.Data("final_res")
        return (None, s)
        
    _, cmd = await create_complete_group_cmd(setup_second_def_running)
    
    await handle(capturing_handler, cmd)
    
    assert type(passed_result["data"]) is tuple
    assert len(passed_result["data"]) == 2



async def test_handle_does_not_invoke_event_handler_when_not_all_completed(create_complete_group_cmd, handle, two_definition_group):
    def_id_1, def_1, def_id_2, def_2 = two_definition_group
    handler_calls = []
    
    async def tracking_handler(evt):
        handler_calls.append(evt)
        return Result.Ok(None)
        
    def setup_both_running(state: GroupOfRunningDefinitionsState | None, cmd_dict: dict):
        s = GroupOfRunningDefinitionsState()
        defs = (
            DefinitionIdWithValue(def_id_1, def_1), 
            DefinitionIdWithValue(def_id_2, def_2)
        )
        s.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        s.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
        
        first_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_1)
        cmd_dict["step_id"] = first_step_id
        cmd_dict["definition_id"] = def_id_1
        cmd_dict["result"] = CompletedWith.Data("step1_res")
        return (None, s)
        
    _, cmd = await create_complete_group_cmd(setup_both_running)
    
    await handle(tracking_handler, cmd)
    
    assert len(handler_calls) == 0



async def test_handle_returns_no_event_when_step_id_different(create_complete_group_cmd, handle, two_definition_group):
    def_id_1, def_1, def_id_2, def_2 = two_definition_group
    
    def setup_second_def_running(state: GroupOfRunningDefinitionsState | None, cmd_dict: dict):
        s = GroupOfRunningDefinitionsState()
        defs = (
            DefinitionIdWithValue(def_id_1, def_1), 
            DefinitionIdWithValue(def_id_2, def_2)
        )
        s.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        s.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
        
        # Inject wrong step_id into command
        cmd_dict["step_id"] = StepIdValue.new_id()
        cmd_dict["definition_id"] = def_id_1
        cmd_dict["result"] = CompletedWith.Data("res")
        return (None, s)
        
    pre_evt, cmd = await create_complete_group_cmd(setup_second_def_running)
    
    handle_res = await handle(runtime_error_event_handler, cmd)
    
    assert pre_evt is None
    assert type(handle_res) is Result
    assert handle_res.is_ok()
    assert handle_res.ok is None



async def test_handle_returns_error_when_storage_exception(create_complete_group_cmd, two_definition_group):
    def setup_running(state: GroupOfRunningDefinitionsState | None, cmd_dict: dict):
        s = GroupOfRunningDefinitionsState()
        return (None, s)
        
    _, cmd = await create_complete_group_cmd(setup_running)
    
    def convert_to_storage_action_err(func: Callable[Concatenate[GroupOfRunningDefinitionsState | None, ...], tuple]):
        async def wrapper(run_id: RunIdValue, group_id: GroupIdValue, *args, **kwargs):
            raise RuntimeError("Storage write failed")
        return wrapper
        
    handle_res = await handle_complete_group_definition(convert_to_storage_action_err, all_defs_completed_event_handler, cmd)
    
    assert type(handle_res) is Result
    assert handle_res.is_error()
    from runner.completeaction.completegroupdefinitionhandler import CompleteGroupDefinitionStorageError
    assert type(handle_res.error) is CompleteGroupDefinitionStorageError



async def test_handle_returns_complete_all_definitions_error_when_event_handler_error(create_complete_group_cmd, handle, two_definition_group):
    def_id_1, def_1, def_id_2, def_2 = two_definition_group
    evt_error = Error("downstream timeout")
    
    async def error_event_handler(evt):
        return Result.Error(evt_error)
        
    def setup_second_def_running(state: GroupOfRunningDefinitionsState | None, cmd_dict: dict):
        s = GroupOfRunningDefinitionsState()
        defs = (
            DefinitionIdWithValue(def_id_1, def_1), 
            DefinitionIdWithValue(def_id_2, def_2)
        )
        s.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        s.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
        
        first_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_1)
        s.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, def_id_1, CompletedWith.Data("step1_res")))
        
        second_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_2)
        cmd_dict["step_id"] = second_step_id
        cmd_dict["definition_id"] = def_id_2
        cmd_dict["result"] = CompletedWith.Data("final_res")
        return (None, s)
        
    _, cmd = await create_complete_group_cmd(setup_second_def_running)
    
    handle_res = await handle(error_event_handler, cmd)
    
    assert type(handle_res) is Result
    assert handle_res.is_error()
    assert type(handle_res.error) is CompletAllDefinitionsError
    assert handle_res.error.error == evt_error



async def test_handle_raises_exception_when_event_handler_exception(create_complete_group_cmd, handle, two_definition_group):
    def_id_1, def_1, def_id_2, def_2 = two_definition_group
    expected_ex = RuntimeError("network collapse")
    
    async def ex_event_handler(evt):
        raise expected_ex
        
    def setup_second_def_running(state: GroupOfRunningDefinitionsState | None, cmd_dict: dict):
        s = GroupOfRunningDefinitionsState()
        defs = (
            DefinitionIdWithValue(def_id_1, def_1), 
            DefinitionIdWithValue(def_id_2, def_2)
        )
        s.apply_command(GroupOfRunningDefinitionsState.Commands.SetDefinitions(defs))
        s.apply_command(GroupOfRunningDefinitionsState.Commands.RunDefinitions())
        
        first_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_1)
        s.apply_command(GroupOfRunningDefinitionsState.Commands.CompleteDefinition(first_step_id, def_id_1, CompletedWith.Data("step1_res")))
        
        second_step_id = next(e.value for e in s._running_definitions if e.definition_id == def_id_2)
        cmd_dict["step_id"] = second_step_id
        cmd_dict["definition_id"] = def_id_2
        cmd_dict["result"] = CompletedWith.Data("final_res")
        return (None, s)
        
    _, cmd = await create_complete_group_cmd(setup_second_def_running)
    
    with pytest.raises(RuntimeError) as exc_info:
        await handle(ex_event_handler, cmd)
    assert exc_info.value == expected_ex