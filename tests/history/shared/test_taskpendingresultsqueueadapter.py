import pytest

from history.shared.taskpendingresultsqueue import CompletedTaskData, TaskPendingResultsQueue, TaskPendingResultsQueueAdapter
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

@pytest.fixture
def third_item(task_id: TaskIdValue):
    return CompletedTaskData(task_id, RunIdValue.new_id(), CompletedWith.NoData(), DefinitionVersion.parse(1))

@pytest.fixture
def forth_item(task_id: TaskIdValue):
    return CompletedTaskData(task_id, RunIdValue.new_id(), CompletedWith.Error("forth item test error"), DefinitionVersion.parse(1))

def perform_queue_assert(actual_queue: TaskPendingResultsQueue, expected_queue: TaskPendingResultsQueue):
    assert actual_queue.recent_run_id == expected_queue.recent_run_id
    assert actual_queue.recent_dequeued_run_id == expected_queue.recent_dequeued_run_id

    expected_item = expected_queue.dequeue()
    actual_item = actual_queue.dequeue()

    while expected_item is not None or actual_item is not None:
        assert actual_item is not None
        assert expected_item is not None
        assert actual_item.data.task_id == expected_item.data.task_id
        assert actual_item.data.run_id == expected_item.data.run_id
        assert actual_item.data.result == expected_item.data.result
        assert actual_item.data.opt_definition_version == expected_item.data.opt_definition_version
        assert actual_item.prev_run_id == expected_item.prev_run_id
        actual_item = actual_queue.dequeue()
        expected_item = expected_queue.dequeue()



def test_to_dict_and_back_enqueue_only(first_item: CompletedTaskData, second_item: CompletedTaskData, third_item: CompletedTaskData, forth_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.enqueue(third_item)
    queue.enqueue(forth_item)
    expected_queue = queue

    queue_dict = TaskPendingResultsQueueAdapter.to_dict(queue)
    actual_queue_res = TaskPendingResultsQueueAdapter.from_dict(queue_dict)

    assert actual_queue_res.is_ok()
    actual_queue = actual_queue_res.ok
    perform_queue_assert(actual_queue, expected_queue)



def test_to_dict_and_back_enqueue_then_dequeue_first(first_item: CompletedTaskData, second_item: CompletedTaskData, third_item: CompletedTaskData, forth_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.enqueue(third_item)
    queue.enqueue(forth_item)
    queue.dequeue()
    expected_queue = queue

    queue_dict = TaskPendingResultsQueueAdapter.to_dict(queue)
    actual_queue_res = TaskPendingResultsQueueAdapter.from_dict(queue_dict)

    assert actual_queue_res.is_ok()
    actual_queue = actual_queue_res.ok
    perform_queue_assert(actual_queue, expected_queue)



def test_to_dict_and_back_enqueue_then_dequeue_first_and_second(first_item: CompletedTaskData, second_item: CompletedTaskData, third_item: CompletedTaskData, forth_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.enqueue(third_item)
    queue.enqueue(forth_item)
    queue.dequeue()
    queue.dequeue()
    expected_queue = queue

    queue_dict = TaskPendingResultsQueueAdapter.to_dict(queue)
    actual_queue_res = TaskPendingResultsQueueAdapter.from_dict(queue_dict)

    assert actual_queue_res.is_ok()
    actual_queue = actual_queue_res.ok
    perform_queue_assert(actual_queue, expected_queue)



def test_to_dict_and_back_enqueue_then_dequeue_first_second_and_third(first_item: CompletedTaskData, second_item: CompletedTaskData, third_item: CompletedTaskData, forth_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.enqueue(third_item)
    queue.enqueue(forth_item)
    queue.dequeue()
    queue.dequeue()
    queue.dequeue()
    expected_queue = queue

    queue_dict = TaskPendingResultsQueueAdapter.to_dict(queue)
    actual_queue_res = TaskPendingResultsQueueAdapter.from_dict(queue_dict)

    assert actual_queue_res.is_ok()
    actual_queue = actual_queue_res.ok
    perform_queue_assert(actual_queue, expected_queue)



def test_to_dict_and_back_enqueue_then_dequeue_all(first_item: CompletedTaskData, second_item: CompletedTaskData, third_item: CompletedTaskData, forth_item: CompletedTaskData):
    queue = TaskPendingResultsQueue()
    queue.enqueue(first_item)
    queue.enqueue(second_item)
    queue.enqueue(third_item)
    queue.enqueue(forth_item)
    queue.dequeue()
    queue.dequeue()
    queue.dequeue()
    queue.dequeue()
    expected_queue = queue

    queue_dict = TaskPendingResultsQueueAdapter.to_dict(queue)
    actual_queue_res = TaskPendingResultsQueueAdapter.from_dict(queue_dict)

    assert actual_queue_res.is_ok()
    actual_queue = actual_queue_res.ok
    perform_queue_assert(actual_queue, expected_queue)