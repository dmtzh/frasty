from shared.action import ActionType
from shared.definition import Definition, DefinitionAdapter, StepsMissing
from shared.validation import ValueInvalid, ValueMissing



def test_from_valid_definition_list_with_input_data_as_part_of_first_step_definition():
    expected_input_data_dict = {"url": "http://localhost", "http_method": "Get"}
    expected_num_of_steps = 3
    list_data = [
        {"action": "requesturl", "type": "core", **expected_input_data_dict},
        {"action": "filtersuccessresponse"},
        {"action": "filterhtmlresponse"}
    ]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_ok()
    assert type(res.ok) is Definition
    assert res.ok.input_data == expected_input_data_dict
    assert len(res.ok.steps) == expected_num_of_steps
    actual_first_step = res.ok.steps[0]
    assert actual_first_step.name == list_data[0]["action"]
    assert actual_first_step.type == ActionType.CORE
    assert actual_first_step.config == expected_input_data_dict
    actual_second_step = res.ok.steps[1]
    assert actual_second_step.name == list_data[1]["action"]
    assert actual_second_step.type == ActionType.CUSTOM
    assert actual_second_step.config is None
    actual_third_step = res.ok.steps[2]
    assert actual_third_step.name == list_data[2]["action"]
    assert actual_third_step.type == ActionType.CUSTOM
    assert actual_third_step.config is None



def test_from_valid_definition_list_with_single_item_list_input_data():
    expected_first_step_config = {"cfg1": "test1", "cfg2": "test2"}
    expected_input_data_dict = {"url": "http://localhost", "http_method": "Get"}
    expected_third_step_config = {"cfg3": "test3", "cfg4": "test4"}
    expected_num_of_steps = 3
    list_data = [
        {"action": "requesturl", "type": "core", **expected_first_step_config, "input_data": [expected_input_data_dict]},
        {"action": "filtersuccessresponse", "type": "core"},
        {"action": "filterhtmlresponse", **expected_third_step_config}
    ]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_ok()
    assert type(res.ok) is Definition
    assert res.ok.input_data == expected_input_data_dict
    assert len(res.ok.steps) == expected_num_of_steps
    actual_first_step = res.ok.steps[0]
    assert actual_first_step.name == list_data[0]["action"]
    assert actual_first_step.type == ActionType.CORE
    assert actual_first_step.config == expected_first_step_config
    actual_second_step = res.ok.steps[1]
    assert actual_second_step.name == list_data[1]["action"]
    assert actual_second_step.type == ActionType.CORE
    assert actual_second_step.config is None
    actual_third_step = res.ok.steps[2]
    assert actual_third_step.name == list_data[2]["action"]
    assert actual_third_step.type == ActionType.CUSTOM
    assert actual_third_step.config == expected_third_step_config



def test_from_valid_definition_list_with_multi_items_list_input_data():
    expected_first_step_config = {"cfg1": "test1", "cfg2": "test2"}
    expected_input_data_list = [
        {"url": "http://localhost", "http_method": "Get"},
        {"url": "http://localhost", "http_method": "POST"}
    ]
    expected_third_step_config = {"cfg3": "test3", "cfg4": "test4"}
    expected_num_of_steps = 3
    list_data = [
        {"action": "requesturl", "type": "core", **expected_first_step_config, "input_data": expected_input_data_list},
        {"action": "filtersuccessresponse", "type": "core"},
        {"action": "filterhtmlresponse", **expected_third_step_config}
    ]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_ok()
    assert type(res.ok) is Definition
    assert res.ok.input_data == expected_input_data_list
    assert len(res.ok.steps) == expected_num_of_steps
    actual_first_step = res.ok.steps[0]
    assert actual_first_step.name == list_data[0]["action"]
    assert actual_first_step.type == ActionType.CORE
    assert actual_first_step.config == expected_first_step_config
    actual_second_step = res.ok.steps[1]
    assert actual_second_step.name == list_data[1]["action"]
    assert actual_second_step.type == ActionType.CORE
    assert actual_second_step.config is None
    actual_third_step = res.ok.steps[2]
    assert actual_third_step.name == list_data[2]["action"]
    assert actual_third_step.type == ActionType.CUSTOM
    assert actual_third_step.config == expected_third_step_config



def test_from_invalid_list_without_input_data():
    list_data = [{"action": "requesturl", "type": "core"}]
    expected_errors = [ValueMissing("input_data")]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_invalid_list_with_empty_input_data():
    list_data = [{"action": "requesturl", "type": "core", "input_data": []}]
    expected_errors = [ValueMissing("input_data")]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_invalid_list_with_non_list_input_data():
    list_data = [{"action": "requesturl", "type": "core", "input_data": "invalid input data"}]
    expected_errors = [ValueInvalid("input_data")]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_invalid_list_without_steps():
    list_data = []

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_error()
    assert type(res.error) is StepsMissing



def test_from_invalid_list_with_missing_step_action():
    list_data = [{"url": "http://localhost", "http_method": "Get"}]
    expected_errors = [ValueMissing("action")]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_invalid_list_with_empty_step_action():
    list_data = [{"action": "", "url": "http://localhost", "http_method": "Get"}]
    expected_errors = [ValueInvalid("action")]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_invalid_list_with_wrong_step_action_type():
    list_data = [{"action": "requesturl", "type": "wrong_type", "url": "http://localhost", "http_method": "Get"}]
    expected_errors = [ValueInvalid("type")]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_invalid_list_with_empty_step_action_type():
    list_data = [{"action": "requesturl", "type": "", "url": "http://localhost", "http_method": "Get"}]
    expected_errors = [ValueInvalid("type")]

    res = DefinitionAdapter.from_list(list_data)

    assert res.is_error()
    assert res.error == expected_errors



def test_to_list_with_dict_input_data():
    input_data_dict = {"url": "http://localhost"}
    source_list_data = [
        {"action": "requesturl", "type": "core", **input_data_dict},
        {"action": "filtersuccessresponse"},
        {"action": "filterhtmlresponse"}
    ]
    expected_list_data = [
        {"action": "requesturl", "type": "core", **input_data_dict, "input_data": [input_data_dict]},
        {"action": "filtersuccessresponse"},
        {"action": "filterhtmlresponse"}
    ]
    definition = DefinitionAdapter.from_list(source_list_data).ok

    actual_list_data = DefinitionAdapter.to_list(definition)

    assert type(definition.input_data) is dict
    assert actual_list_data == expected_list_data



def test_to_list_with_list_input_data():
    expected_list_data = [
        {
            "action": "requesturl", "type": "core",
            "input_data": [
                {"url": "http://localhost", "http_method": "Get"},
                {"url": "http://localhost", "http_method": "POST"}
            ]
        },
        {"action": "filtersuccessresponse", "type": "core"},
        {"action": "filterhtmlresponse", "cfg3": "test3", "cfg4": "test4"}
    ]
    definition = DefinitionAdapter.from_list(expected_list_data).ok

    actual_list_data = DefinitionAdapter.to_list(definition)

    assert type(definition.input_data) is list
    assert actual_list_data == expected_list_data
