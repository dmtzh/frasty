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

@pytest.mark.skip(reason="too slow")
@pytest.mark.parametrize(("num_ids", "max_duplicates"), _ARG_VALUES)
def test_uniqueness(all_generated_ids, num_ids, max_duplicates):
    generated_ids = set()

    num_of_generated = 0
    for _ in range(num_ids):
        new_id = CrockfordId.new_id()
        generated_ids.add(new_id)
        num_of_generated += 1
        num_of_duplicates = num_of_generated - len(generated_ids)
        assert (num_of_generated - len(generated_ids)) <= max_duplicates
    
    all_generated_ids.update(generated_ids)
    print(f"Generated {num_of_generated} ids with {num_of_duplicates} duplicates")

def test_new_id_for_default_length():
    default_length = 8

    id = CrockfordId.new_id()

    assert len(id) == default_length

def test_new_id_for_custom_length():
    custom_length = 4

    id = CrockfordId.new_id(custom_length)

    assert len(id) == custom_length

def test_from_value_with_checksum():
    id = CrockfordId.new_id()
    value_with_checksum = id.get_value_with_checksum()

    opt_crockford_id = CrockfordId.from_value_with_checksum(value_with_checksum)

    assert opt_crockford_id is not None

def test_from_value_with_invalid_checksum():
    id = CrockfordId.new_id()
    value_with_checksum = id.get_value_with_checksum()
    value_without_checksum = value_with_checksum[:-1]
    checksum = value_with_checksum[-1]
    checksum_next_letter = chr(ord(checksum) + 1)
    value_with_invalid_checksum = value_without_checksum + checksum_next_letter

    opt_crockford_id = CrockfordId.from_value_with_checksum(value_with_invalid_checksum)

    assert opt_crockford_id is None