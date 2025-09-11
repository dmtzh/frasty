from shared.customtypes import TaskIdValue

def test_new_id_length():
    id_value_len = 3

    id = TaskIdValue.new_id()

    assert len(id) == id_value_len

def test_from_value_with_checksum():
    id = TaskIdValue.new_id()
    value_with_checksum = id.to_value_with_checksum()

    opt_id = TaskIdValue.from_value_with_checksum(value_with_checksum)

    assert opt_id is not None

def test_from_value_with_invalid_checksum():
    id = TaskIdValue.new_id()
    value_with_checksum = id.to_value_with_checksum()
    value_without_checksum = value_with_checksum[:-1]
    checksum = value_with_checksum[-1]
    checksum_next_letter = chr(ord(checksum) + 1)
    value_with_invalid_checksum = value_without_checksum + checksum_next_letter

    opt_id = TaskIdValue.from_value_with_checksum(value_with_invalid_checksum)

    assert opt_id is None

def test_to_value_with_checksum():
    expected_value_with_checksum = TaskIdValue.new_id().to_value_with_checksum()
    id = TaskIdValue.from_value_with_checksum(expected_value_with_checksum)
    assert id is not None

    value_with_checksum = id.to_value_with_checksum()

    assert value_with_checksum == expected_value_with_checksum