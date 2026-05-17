import pytest

from shared.customtypes import DefinitionIdValue
from shared.definition import Definition
from shared.executedefinitionaction import ExecuteDefinitionInput, ExecuteGroupOfDefinitionsInput

# ==========================================
# Negative cases
# ==========================================

class TestNegativeCases:
    @pytest.mark.parametrize("case_name, payload, expected_error_fragment", [
        ("missing_definition_id", {"definitions": [[{"action": "filtersuccessresponse"}]]}, "definition_id"),
        ("missing_definition", {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum()}, "definition"),
        ("missing_both1", {}, "definition_id"),
        ("missing_both2", {}, "definition")
    ])
    def test_single_definition_missing_required_fields_returns_error(
        self, case_name, payload, expected_error_fragment
    ):
        result = ExecuteDefinitionInput.from_dict(payload)

        assert result.is_error()
        assert expected_error_fragment in result.error
    
    @pytest.mark.parametrize("case_name, payload, expected_error_fragment", [
        ("missing_definitions", [{"definition_id": DefinitionIdValue.new_id().to_value_with_checksum()}], "definition"),
        ("missing", [], "definitions")
    ])
    def test_multiple_defintions_missing_required_fields_returns_error(
        self, case_name, payload, expected_error_fragment
    ):
        result = ExecuteGroupOfDefinitionsInput.from_list(payload)

        assert result.is_error()
        assert expected_error_fragment in result.error

    @pytest.mark.parametrize("case_name, payload, expected_error_fragment", [
        ("wrong_definition_id", {"definition_id": "invalid", "definition": [{"action": "filtersuccessresponse"}]}, "definition_id"),
        ("none_definition_id", {"definition_id": None, "definition": [{"action": "filtersuccessresponse"}]}, "definition_id"),
        ("wrong_definitions1", {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": "invalid"}, "definition"),
        ("wrong_definitions2", {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": {}}, "definition"),
        ("wrong_definitions3", {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": []}, "definition"),
        ("wrong_definitions4", {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": ["invalid"]}, "definition"),
        ("wrong_definitions5", {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": [{"action": "filtersuccessresponse"}]}, "input_data"),
        ("wrong_definitions6", {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": [{"name": "filtersuccessresponse"}]}, "action"),
        ("none_definitions", {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": None}, "definition"),
        ("wrong_both1", {"definition_id": "invalid", "definition": "invalid"}, "definition_id"),
        ("wrong_both2", {"definition_id": "invalid", "definition": "invalid"}, "definition")
    ])
    def test_single_defintion_wrong_data_returns_error(
        self, case_name, payload, expected_error_fragment
    ):
        result = ExecuteDefinitionInput.from_dict(payload)
        
        assert result.is_error()
        assert expected_error_fragment in result.error
    
    @pytest.mark.parametrize("case_name, payload, expected_error_fragment", [
        ("wrong_definitions1", "invalid", "definitions"),
        ("wrong_definitions2", {}, "definitions"),
        ("wrong_definitions3", [], "definitions"),
        ("wrong_definitions4", ["invalid"], "definitions"),
        ("wrong_definitions5", [{"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": [{"action": "filtersuccessresponse"}]}], "input_data"),
        ("wrong_definitions6", [{"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": [{"name": "filtersuccessresponse"}]}], "action"),
        ("none_definitions", None, "definitions"),
    ])
    def test_multiple_defintions_wrong_data_returns_error(
        self, case_name, payload, expected_error_fragment
    ):
        result = ExecuteGroupOfDefinitionsInput.from_list(payload)
        
        assert result.is_error()
        assert expected_error_fragment in result.error

# ==========================================
# Happy path
# ==========================================

class TestHappyPath:
    def test_single_definition(
        self
    ):
        expected_definition_id = DefinitionIdValue.new_id()
        input_data_dict = {"url": "http://localhost", "http_method": "Get"}
        definition_dto = [
            {"action": "requesturl", "type": "core", **input_data_dict},
            {"action": "filtersuccessresponse"},
            {"action": "filterhtmlresponse"}
        ]
        data = {
            "definition_id": expected_definition_id.to_value_with_checksum(),
            "definition": definition_dto
        }

        result = ExecuteDefinitionInput.from_dict(data)

        assert result.is_ok()
        assert type(result.ok) is ExecuteDefinitionInput
        assert result.ok.definition_id == expected_definition_id
        assert type(result.ok.definition) is Definition

    def test_multiple_definitions(
        self
    ):
        input_data1_dict = {"url": "http://localhost/1", "http_method": "Get"}
        definition1_dto = [
            {"action": "requesturl", "type": "core", **input_data1_dict},
            {"action": "filtersuccessresponse"},
            {"action": "filterhtmlresponse"}
        ]
        input_data2_dict = {"url": "http://localhost/2", "http_method": "Get"}
        definition2_dto = [
            {"action": "requesturl", "type": "core", **input_data2_dict},
            {"action": "filtersuccessresponse"},
            {"action": "filterjsonresponse"}
        ]
        data = [
            {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": definition1_dto},
            {"definition_id": DefinitionIdValue.new_id().to_value_with_checksum(), "definition": definition2_dto}
        ]

        result = ExecuteGroupOfDefinitionsInput.from_list(data)

        assert result.is_ok()
        assert type(result.ok) is ExecuteGroupOfDefinitionsInput
        assert type(result.ok.items) is list
        assert result.ok.items
        for item in result.ok.items:
            assert type(item) is ExecuteDefinitionInput