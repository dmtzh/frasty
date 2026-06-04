# Unit tests for execute_definition_handler_input_validator

import pytest

from shared.customtypes import DefinitionIdValue
from shared.executedefinitionaction import ExecuteDefinitionInput
from shared.pipeline.actionhandler import DataDto

from runner.executedefinition.input import ExecuteGroupOfDefinitionsInput
from runner.executedefinition.registration import execute_definition_handler_input_validator

# =============================================================================
# FIXTURES: Canonical valid input structures
# =============================================================================

@pytest.fixture
def canonical_single_definition_dto() -> DataDto:
    """
    Canonical fixture for ExecuteDefinitionInput.
    Represents a valid single definition with three steps.
    """
    expected_definition_id = DefinitionIdValue.new_id()
    input_data_dict = {"url": "http://localhost/1", "http_method": "Get"}
    definition_dto = [
        {"action": "requesturl", "type": "core", **input_data_dict},
        {"action": "filtersuccessresponse"},
        {"action": "filterhtmlresponse"}
    ]
    return {
        "definition_id": expected_definition_id.to_value_with_checksum(),
        "definition": definition_dto
    }


@pytest.fixture
def canonical_two_definitions_dto() -> list[DataDto]:
    """
    Canonical fixture for ExecuteGroupOfDefinitionsInput.
    Represents a valid group with two definitions, each containing three steps.
    """
    expected_definition_id1 = DefinitionIdValue.new_id()
    expected_definition_id2 = DefinitionIdValue.new_id()
    
    definition_dto1 = [
        {"action": "requesturl", "type": "core", "url": "http://localhost/1", "http_method": "Get"},
        {"action": "filtersuccessresponse"},
        {"action": "filterhtmlresponse"}
    ]
    definition_dto2 = [
        {"action": "requesturl", "type": "core", "url": "http://localhost/2", "http_method": "Post"},
        {"action": "filtersuccessresponse"},
        {"action": "filterhtmlresponse"}
    ]
    
    return [
        {
            "definition_id": expected_definition_id1.to_value_with_checksum(),
            "definition": definition_dto1
        },
        {
            "definition_id": expected_definition_id2.to_value_with_checksum(),
            "definition": definition_dto2
        }
    ]


@pytest.fixture
def dto_list_single(canonical_single_definition_dto) -> list[DataDto]:
    """Wraps single definition DTO into DataDto list as expected by validator."""
    return [canonical_single_definition_dto]


@pytest.fixture
def dto_list_group(canonical_two_definitions_dto) -> list[DataDto]:
    """Wraps group definitions DTO into DataDto list as expected by validator."""
    return canonical_two_definitions_dto


# =============================================================================
# POSITIVE TESTS: ExecuteDefinitionInput (single definition)
# =============================================================================

def test_validate_single_definition_success(dto_list_single):
    """
    GIVEN a valid single definition DTO list
    WHEN validator is called
    THEN returns Result.Ok with ExecuteDefinitionInput
    """
    result = execute_definition_handler_input_validator(dto_list_single)
    
    assert result.is_ok()
    assert isinstance(result.ok, ExecuteDefinitionInput)


def test_validate_single_definition_with_extra_root_fields(dto_list_single):
    """
    GIVEN a single definition DTO with unknown root-level fields
    WHEN validator is called
    THEN unknown fields are ignored and parsing succeeds
    """
    dto_list_single[0]["extra_field"] = "should_be_ignored"
    dto_list_single[0]["another_unknown"] = 123
    
    result = execute_definition_handler_input_validator(dto_list_single)
    
    assert result.is_ok()


# =============================================================================
# POSITIVE TESTS: ExecuteGroupOfDefinitionsInput (group of definitions)
# =============================================================================

def test_validate_group_definitions_success(dto_list_group):
    """
    GIVEN a valid group definitions DTO list
    WHEN validator is called
    THEN returns Result.Ok with ExecuteGroupOfDefinitionsInput
    """
    result = execute_definition_handler_input_validator(dto_list_group)
    
    assert result.is_ok()
    assert isinstance(result.ok, ExecuteGroupOfDefinitionsInput)


def test_validate_mixed_input_data_in_group(dto_list_group):
    """
    GIVEN a group where first DTO has 'input_data', second does not
    WHEN validator is called
    THEN traverse processes each item independently via lambda
    AND first item gets replaced steps, second retains original steps
    """
    # Add custom input_data for First item
    input_data1 = [
        {"param": "value1"},
        {"param": "value2"}
    ]
    dto_list_group[0]["input_data"] = input_data1
    # dto_list_group[1] input_data remains unchanged
    
    result = execute_definition_handler_input_validator(dto_list_group)
    
    assert result.is_ok()
    assert isinstance(result.ok, ExecuteGroupOfDefinitionsInput)
    # First item: with replaced input_data
    assert result.ok.items[0].definition.input_data == input_data1
    # Second item: original input_data preserved
    assert result.ok.items[1].definition.input_data is not None
    assert result.ok.items[1].definition.input_data != input_data1


# =============================================================================
# POSITIVE TESTS: input_data injection path
# =============================================================================

def test_validate_single_definition_with_input_data_injection(dto_list_single):
    """
    GIVEN a single definition DTO with valid 'input_data' key
    WHEN validator is called
    THEN input_data is injected and steps are replaced via replace_input_data
    """
    # Add valid input_data: list of step-like DTOs
    input_data = [
        {"param": "value1"},
        {"param": "value2"}
    ]
    dto_list_single[0]["input_data"] = input_data
    
    result = execute_definition_handler_input_validator(dto_list_single)
    
    assert result.is_ok()
    assert isinstance(result.ok, ExecuteDefinitionInput)
    assert result.ok.definition.input_data == input_data


def test_validate_group_definitions_with_input_data_injection(dto_list_group):
    """
    GIVEN a group definitions DTO with valid 'input_data' key
    WHEN validator is called
    THEN input_data replaces steps in ALL definitions within the group
    """
    input_data1 = [
        {"param": "value1"}
    ]
    input_data2 = [
        {"param": "value1"},
        {"param": "value2"}
    ]
    dto_list_group[0]["input_data"] = input_data1
    dto_list_group[1]["input_data"] = input_data2
    
    result = execute_definition_handler_input_validator(dto_list_group)
    
    assert result.is_ok()
    assert isinstance(result.ok, ExecuteGroupOfDefinitionsInput)
    assert result.ok.items[0].definition.input_data == input_data1
    assert result.ok.items[1].definition.input_data == input_data2


# =============================================================================
# NEGATIVE TESTS: Error handling and edge cases
# =============================================================================


def test_validate_when_input_data_is_invalid_returns_error(dto_list_single):
    # Add INVALID input_data: None instead of list
    dto_list_single[0]["input_data"] = None
    
    result = execute_definition_handler_input_validator(dto_list_single)
    
    # EXPECTED AFTER FIX:
    assert result.is_error()


def test_validate_empty_dto_list_returns_error():
    """
    GIVEN an empty dto_list
    WHEN validator is called
    THEN returns Result.Error with descriptive message
    """
    result = execute_definition_handler_input_validator([])
    
    assert result.is_error()
    assert "definition is missing" in str(result.error)


def test_validate_missing_definition_key_returns_error():
    """
    GIVEN a DTO without 'definition' key
    WHEN validator is called
    THEN returns Result.Error
    """
    invalid_dto = [{"unknown_key": "value"}]
    
    result = execute_definition_handler_input_validator(invalid_dto)
    
    assert result.is_error()


def test_validate_invalid_definition_id_format_returns_error(dto_list_single):
    """
    GIVEN a DTO with malformed definition_id (no checksum)
    WHEN validator is called
    THEN returns Result.Error from DefinitionIdValue parser
    """
    dto_list_single[0]["definition_id"] = "invalid-uuid-without-checksum"
    
    result = execute_definition_handler_input_validator(dto_list_single)
    
    assert result.is_error()


def test_validate_step_without_action_returns_error(dto_list_single):
    """
    GIVEN a definition with a step missing required 'action' field
    WHEN validator is called
    THEN returns Result.Error from DefinitionAdapter
    """
    dto_list_single[0]["definition"][0] = {"type": "core"}  # Missing 'action'
    
    result = execute_definition_handler_input_validator(dto_list_single)
    
    assert result.is_error()


def test_validate_group_with_invalid_nested_structure_returns_error(dto_list_group):
    """
    GIVEN a group DTO with wrong nesting level
    WHEN validator is called
    THEN returns Result.Error
    """
    # Wrong structure: definitions should be list of lists
    dto_list_group[0] = [{"action": "wrong_level"}]
    
    result = execute_definition_handler_input_validator(dto_list_group)
    
    assert result.is_error()


def test_validate_double_failure_aggregates_or_chooses_single_error(dto_list_single):
    """
    GIVEN a DTO that fails to parse as both single and group definition
    WHEN validator is called
    THEN returns Result.Error
    NOTE: Current implementation returns only single-definition error.
    Consider aggregating both errors for better diagnostics.
    """
    # Remove all recognizable keys
    dto_list_single[0] = {"garbage": "data"}
    
    result = execute_definition_handler_input_validator(dto_list_single)
    
    assert result.is_error()
    # Verify error message contains hint about what was expected
    error_msg = str(result.error).lower()
    assert all(keyword in error_msg for keyword in ["definition", "definition_id"])


def test_validate_multiple_errors_captured(dto_list_group):
    """
    GIVEN a list of 4 DTOs where the second one has malformed definition and fourth has malformed definition_id
    WHEN validator is called
    THEN all items are processed
    AND Result.Error contains definition parsing failure and definition_id parsing failure
    """
    dto_list = [
        dto_list_group[0],
        {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": [{"param": "value"}]},
        dto_list_group[1],
        {"definition_id": "invalid-checksum-format", "definition": [{"action": "step", "param": "value"}]}
    ]
    
    result = execute_definition_handler_input_validator(dto_list)
    
    assert result.is_error()
    error_msg = str(result.error).lower()
    assert "action" in error_msg
    assert "definition_id" in error_msg
    assert "checksum" in error_msg


def test_validate_empty_definition_list_rejected(dto_list_single):
    """
    GIVEN a DTO with empty 'definition' list
    WHEN validator is called
    THEN parse_from_dict lambda returns None due to 'and lst' check
    AND apply returns Result.Error
    """
    dto_list_single[0]["definition"] = []
    
    result = execute_definition_handler_input_validator(dto_list_single)
    
    assert result.is_error()