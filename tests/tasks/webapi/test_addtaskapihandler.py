from expression import Result
import pytest

from shared.customtypes import DefinitionIdValue, Error, TaskIdValue
from shared.task import Task
from tasks.webapi.addtaskapihandler import AddDefinitionError, AddTaskResource, AddToStorageError, TaskNameMissing, add_task_workflow

class TestAddDefinitionError(Error):
    '''Add definition error'''
    __test__ = False  # Instruct pytest to ignore this class for test collection

@pytest.fixture
def add_definition_handler():
    async def handler(raw_definiiton):
        definition_id = DefinitionIdValue.new_id()
        return Result.Ok(definition_id)
    return handler

@pytest.fixture
def add_to_storage_handler():
    async def handler(task_id: TaskIdValue, task: Task):
        return Result.Ok(None)
    return handler



async def test_add_task_workflow_success(add_definition_handler, add_to_storage_handler):
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])

    res = await add_task_workflow(add_definition_handler, add_to_storage_handler, resource)

    assert type(res) is Result
    assert res.is_ok()
    assert type(res.ok) is TaskIdValue
    assert TaskIdValue.from_value(res.ok) is not None



async def test_add_task_workflow_error_when_task_name_missing(add_definition_handler, add_to_storage_handler):
    resource = AddTaskResource(name='', definition=[{'key': 'value'}])
    
    res = await add_task_workflow(add_definition_handler, add_to_storage_handler, resource)

    assert type(res) is Result
    assert res.is_error()
    assert type(res.error) is TaskNameMissing



async def test_add_task_workflow_error_when_add_definition_handler_error(add_to_storage_handler):
    expected_error = TestAddDefinitionError("Add definition error message")
    async def handler_with_error(raw_definiiton):
        return Result.Error(expected_error)
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])
    
    res = await add_task_workflow(handler_with_error, add_to_storage_handler, resource)

    assert type(res) is Result
    assert res.is_error()
    assert type(res.error) is AddDefinitionError
    assert res.error.error == expected_error



async def test_add_task_workflow_error_when_add_to_storage_handler_error(add_definition_handler):
    expected_error = Error("Tasks storage error")
    async def handler_with_error(task_id: TaskIdValue, task: Task):
        return Result.Error(expected_error)
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])

    res = await add_task_workflow(add_definition_handler, handler_with_error, resource)

    assert type(res) is Result
    assert res.is_error()
    assert type(res.error) is AddToStorageError
    assert res.error.error == expected_error



async def test_add_task_workflow_when_add_definition_handler_error_then_add_to_storage_handler_not_invoked():
    state = {}
    async def add_definition_handler(raw_definiiton):
        return Result.Error(TestAddDefinitionError("Add definition error message"))
    async def add_to_storage_handler(task_id: TaskIdValue, task: Task):
        state["actual_task_id"] = task_id
        return Result.Ok(None)
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])
    
    await add_task_workflow(add_definition_handler, add_to_storage_handler, resource)

    assert "actual_task_id" not in state