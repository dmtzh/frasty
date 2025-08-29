import pytest

from shared.utils.crockfordid import CrockfordId

@pytest.fixture(scope="session", autouse=True)
def all_generated_ids():
    all_ids = set()
    yield all_ids
    print(f"Total number of generated ids: {len(all_ids)}")

_NUM_OF_IDS_TO_GENERATE = 1000000
_MAX_NUM_OF_ALLOWED_DUPLICATES = 3
_ARG_VALUES = [(_NUM_OF_IDS_TO_GENERATE, _MAX_NUM_OF_ALLOWED_DUPLICATES) for _ in range(1)]

@pytest.mark.parametrize(("num_ids", "max_duplicates"), _ARG_VALUES)
def test_uniqueness(all_generated_ids, num_ids, max_duplicates):
    generated_ids = set()

    num_of_generated = 0
    for _ in range(num_ids):
        new_id = CrockfordId.new_id()
        generated_ids.add(new_id.get_value())
        num_of_generated += 1
        num_of_duplicates = num_of_generated - len(generated_ids)
        assert (num_of_generated - len(generated_ids)) <= max_duplicates
    
    all_generated_ids.update(generated_ids)
    print(f"Generated {num_of_generated} ids with {num_of_duplicates} duplicates")

        