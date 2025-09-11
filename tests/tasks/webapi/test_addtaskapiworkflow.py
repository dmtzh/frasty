from expression import Result
import pytest

from shared.customtypes import Error, IdValue, TaskIdValue
from tasks.webapi.addtaskapiworkflow import AddDefinitionError, AddTaskResource, DefinitionValidationError, InputValidationError, TaskNameMissing, add_task_workflow

@pytest.fixture
def add_definition_handler():
    async def handler(raw_definiiton):
        definition_id = IdValue.new_id()
        return Result.Ok(definition_id)
    return handler

async def test_add_task_workflow_success(add_definition_handler):
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])

    res = await add_task_workflow(add_definition_handler, resource)

    assert type(res) is Result
    assert res.is_ok()
    assert type(res.ok) is TaskIdValue
    assert TaskIdValue.from_value(res.ok) is not None


async def test_add_task_workflow_error_when_task_name_missing(add_definition_handler):
    resource = AddTaskResource(name='', definition=[{'key': 'value'}])
    
    res = await add_task_workflow(add_definition_handler, resource)

    assert type(res) is Result
    assert res.is_error()
    assert type(res.error) is InputValidationError
    assert type(res.error.error) is TaskNameMissing



async def test_add_task_workflow_error_when_add_definition_handler_validation_error():
    validation_error = [{'key': 'value'}]
    async def handler_with_validation_error(raw_definiiton):
        return Result.Error(DefinitionValidationError(validation_error))
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])
    
    res = await add_task_workflow(handler_with_validation_error, resource)

    assert type(res) is Result
    assert res.is_error()
    assert type(res.error) is DefinitionValidationError
    assert res.error.errors == validation_error



async def test_add_task_workflow_error_when_add_definition_handler_error():
    error_message = "Add definition error message"
    async def handler_with_error(raw_definiiton):
        return Result.Error(AddDefinitionError(error_message))
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])
    
    res = await add_task_workflow(handler_with_error, resource)

    assert type(res) is Result
    assert res.is_error()
    assert type(res.error) is AddDefinitionError
    assert res.error.message == error_message



async def test_add_task_workflow_error_when_tasks_storage_exception(add_definition_handler, set_tasks_storage_error):
    tasks_storage_error_message = "Tasks storage error"
    set_tasks_storage_error(RuntimeError(tasks_storage_error_message))
    resource = AddTaskResource(name='test_task', definition=[{'key': 'value'}])

    res = await add_task_workflow(add_definition_handler, resource)

    assert type(res) is Result
    assert res.is_error()
    assert isinstance(res.error, Error)
    assert res.error.message == tasks_storage_error_message