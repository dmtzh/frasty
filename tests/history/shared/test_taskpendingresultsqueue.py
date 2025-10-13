import pytest

from history.shared.taskpendingresultsqueue import CompletedTaskData, TaskPendingResultsQueue
from history.shared.taskresulthistory import DefinitionVersion
from shared.completedresult import CompletedWith
from shared.customtypes import RunIdValue, TaskIdValue

@pytest.fixture
def task_id():
    return TaskIdValue.new_id()

@pytest.fixture
def first_item(task_id: TaskIdValue):
    return CompletedTaskData(task_id, RunIdValue.new_id(), CompletedWith.Data("first item test data"), DefinitionVersion.parse(1))

@pytest.fixture
def second_item(task_id: TaskIdValue):
    return CompletedTaskData(task_id, RunIdValue.new_id(), CompletedWith.Data("second item test data"), DefinitionVersion.parse(1))



def test_put_when_queue_empty_then_add_to_first_position(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    expected_data = first_item
    
    queue.enqueue(first_item)
    actual_item = queue.peek()

    assert actual_item is not None
    assert actual_item.data == expected_data



def test_put_when_queue_not_empty_then_add_to_last_position(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    expected_data = second_item
    
    queue.enqueue(second_item)
    pending_result = queue.dequeue()
    while pending_result is not None:
        actual_data = pending_result.data
        pending_result = queue.dequeue()

    assert actual_data == expected_data



def test_put_when_queue_empty_then_change_recent_run_id_to_data_run_id(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    expected_recent_run_id = first_item.run_id
    
    queue.enqueue(first_item)
    actual_recent_run_id = queue.recent_run_id

    assert actual_recent_run_id == expected_recent_run_id



def test_put_when_queue_not_empty_then_change_recent_run_id_to_data_run_id(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    expected_recent_run_id = second_item.run_id
    
    queue.enqueue(second_item)
    actual_recent_run_id = queue.recent_run_id

    assert actual_recent_run_id == expected_recent_run_id



def test_put_when_run_id_already_available_then_not_added(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)

    queue.enqueue(first_item)
    queue.dequeue()
    queue.dequeue()
    actual_item_peek = queue.peek()
    actual_item_get = queue.dequeue()

    assert actual_item_peek is None
    assert actual_item_get is None



def test_put_when_run_id_recently_removed_then_not_added(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.dequeue()
    queue.dequeue()

    queue.enqueue(first_item)
    actual_item_peek = queue.peek()
    actual_item_get = queue.dequeue()

    assert actual_item_peek is None
    assert actual_item_get is None



def test_peek_when_empty_then_return_none():
    queue = TaskPendingResultsQueue()

    actual_item = queue.peek()

    assert actual_item is None



def test_peek_when_not_empty_then_return_data(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)

    actual_item = queue.peek()

    assert actual_item is not None



def test_peek_when_single_item_then_data_run_id_same_as_recent_run_id(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    expected_run_id = queue.recent_run_id

    actual_item = queue.peek()

    assert actual_item is not None
    assert actual_item.data.run_id == expected_run_id



def test_peek_when_multiple_items_then_data_run_id_not_same_as_recent_run_id(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    not_expected_run_id = queue.recent_run_id

    actual_item = queue.peek()

    assert actual_item is not None
    assert actual_item.data.run_id != not_expected_run_id



def test_peek_when_put_one_item_then_prev_run_id_none(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)

    actual_item = queue.peek()

    assert actual_item is not None
    assert actual_item.prev_run_id is None



def test_peek_when_has_removed_items_then_prev_run_id_same_as_recent_removed_run_id(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.dequeue()

    actual_item = queue.peek()

    assert actual_item is not None
    assert actual_item.prev_run_id is not None
    assert actual_item.prev_run_id == queue.recent_removed_run_id



def test_get_when_empty_then_return_none():
    queue = TaskPendingResultsQueue()

    actual_item = queue.dequeue()

    assert actual_item is None



def test_get_when_not_empty_then_return_first_item(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    expected_data = first_item

    actual_item = queue.dequeue()

    assert actual_item is not None
    assert actual_item.data == expected_data



def test_get_when_not_empty_then_remove_first_item(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    
    queue.dequeue()
    actual_item = queue.dequeue()

    assert actual_item is None



def test_get_change_recent_removed_run_id_to_first_item_run_id(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    expected_recent_removed_run_id = first_item.run_id

    queue.dequeue()
    actual_recent_removed_run_id = queue.recent_removed_run_id

    assert actual_recent_removed_run_id == expected_recent_removed_run_id



def test_when_not_put_then_recent_removed_run_id_none():
    queue = TaskPendingResultsQueue()

    actual_recent_removed_run_id = queue.recent_removed_run_id

    assert actual_recent_removed_run_id is None



def test_when_not_put_then_recent_run_id_none():
    queue = TaskPendingResultsQueue()

    actual_recent_run_id = queue.recent_run_id

    assert actual_recent_run_id is None



def test_when_put_one_item_and_get_one_item_then_recent_removed_run_id_same_as_recent_run_id(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()

    queue.enqueue(first_item)
    queue.dequeue()

    assert queue.recent_removed_run_id is not None
    assert queue.recent_removed_run_id == queue.recent_run_id



def test_when_put_two_items_and_get_two_items_then_recent_removed_run_id_same_as_recent_run_id(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()

    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.dequeue()
    queue.dequeue()

    assert queue.recent_removed_run_id is not None
    assert queue.recent_removed_run_id == queue.recent_run_id



def test_when_put_one_item_and_get_one_item_then_recent_removed_run_id_same_as_data_run_id(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()

    queue.enqueue(first_item)
    actual_item = queue.dequeue()
    actual_recent_removed_run_id = queue.recent_removed_run_id

    assert actual_item is not None
    expected_recent_removed_run_id = actual_item.data.run_id
    assert actual_recent_removed_run_id == expected_recent_removed_run_id



def test_when_put_one_item_and_get_one_item_then_recent_run_id_same_as_data_run_id(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()

    queue.enqueue(first_item)
    actual_item = queue.dequeue()
    actual_recent_run_id = queue.recent_run_id

    assert actual_item is not None
    expected_recent_run_id = actual_item.data.run_id
    assert actual_recent_run_id == expected_recent_run_id



def test_when_put_one_item_then_recent_removed_run_id_none(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()

    queue.enqueue(first_item)
    actual_recent_removed_run_id = queue.recent_removed_run_id

    assert actual_recent_removed_run_id is None



def test_when_put_one_item_then_recent_run_id_not_none(first_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()

    queue.enqueue(first_item)
    actual_recent_run_id = queue.recent_run_id

    assert actual_recent_run_id is not None



def test_when_put_two_items_and_get_one_item_then_recent_removed_run_id_not_same_as_recent_run_id(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()

    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.dequeue()
    actual_recent_removed_run_id = queue.recent_removed_run_id
    actual_recent_run_id = queue.recent_run_id

    assert actual_recent_removed_run_id is not None
    assert actual_recent_removed_run_id != actual_recent_run_id



def test_when_put_two_items_and_get_one_item_then_recent_removed_run_id_same_as_removed_data_run_id(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()

    queue.enqueue(first_item)
    queue.enqueue(second_item)
    actual_item = queue.dequeue()
    actual_recent_removed_run_id = queue.recent_removed_run_id

    assert actual_item is not None
    expected_recent_removed_run_id = actual_item.data.run_id
    assert actual_recent_removed_run_id == expected_recent_removed_run_id



def test_when_put_two_items_and_get_one_item_then_recent_run_id_same_as_second_item_data_run_id(first_item: CompletedTaskData, second_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    expected_recent_run_id = second_item.run_id

    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.dequeue()
    actual_recent_run_id = queue.recent_run_id

    assert actual_recent_run_id == expected_recent_run_id
