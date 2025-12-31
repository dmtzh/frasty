from expression import Result
import pytest

from shared.customtypes import DefinitionIdValue, Error, TaskIdValue
from shared.task import Task
from tasks.webapi.addtaskapihandler import AddTaskResource, TaskNameMissing, add_task_workflow

class AddDefinitionError(Error):
    '''Add definition error'''

@pytest.fixture
def add_definition_handler():
    async def handler(raw_definiiton):
        definition_id = DefinitionIdValue.new_id()
        return Result.Ok(definition_id)
    return handler

@pytest.fixture
def add_to_storage_handler():
    async def handler(task_id: TaskIdValue, task: Task):
        return None
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
    error_message = "Add definition error message"
    async def handler_with_error(raw_definiiton):
        return Result.Error(AddDefinitionError(error_message))
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])
    
    res = await add_task_workflow(handler_with_error, add_to_storage_handler, resource)

    assert type(res) is Result
    assert res.is_error()
    assert type(res.error) is AddDefinitionError
    assert res.error.message == error_message



async def test_add_task_workflow_error_when_add_to_storage_handler_exception(add_definition_handler):
    tasks_storage_error_message = "Tasks storage error"
    async def handler_with_exception(task_id: TaskIdValue, task: Task):
        raise RuntimeError(tasks_storage_error_message)
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])

    res = await add_task_workflow(add_definition_handler, handler_with_exception, resource)

    assert type(res) is Result
    assert res.is_error()
    assert isinstance(res.error, Error)
    assert res.error.message == tasks_storage_error_message



async def test_add_task_workflow_when_add_definition_handler_error_then_add_to_storage_handler_not_invoked():
    state = {}
    async def add_definition_handler(raw_definiiton):
        return Result.Error(AddDefinitionError("Add definition error message"))
    async def add_to_storage_handler(task_id: TaskIdValue, task: Task):
        state["actual_task_id"] = task_id
        return None
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])
    
    await add_task_workflow(add_definition_handler, add_to_storage_handler, resource)

    assert "actual_task_id" not in state