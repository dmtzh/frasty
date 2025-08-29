from shared.domaindefinition import Definition, StepNotAllowed, StepsMissing
from shared.dtodefinition import DefinitionAdapter, UnsupportedStep
from shared.validation import ValueInvalid, ValueMissing
from stepdefinitions.requesturl import RequestUrl

def test_from_list_with_valid_requesturl():
    input_data = {"url": "http://localhost", "http_method": "Get"}
    data_requesturl = [
        {
            "step": "requesturl",
            **input_data
        }
    ]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_ok()
    assert type(res.ok) is Definition
    assert res.ok.input_data == input_data
    assert len(res.ok.steps) == 1
    assert type(res.ok.steps[0]) is RequestUrl
    assert res.ok.steps[0].config is None



def test_from_list_without_steps():
    data_requesturl = []

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert type(res.error) is StepsMissing



def test_from_list_with_missing_step():
    data_requesturl = [{}]
    expected_errors = [ValueMissing("step")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_empty_step():
    data_requesturl = [{"step": ""}]
    expected_errors = [ValueInvalid("step")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_unsupported_step():
    data_requesturl = [{"step": "unsupported step"}]
    expected_errors = [UnsupportedStep("unsupported step")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_two_request_url():
    input_data = {"url": "http://localhost", "http_method": "Get"}
    requesturl_step = {"step": "requesturl", **input_data}
    data_requesturl = [
        requesturl_step,
        requesturl_step
    ]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert type(res.error) is StepNotAllowed
    assert type(res.error.value) is RequestUrl



def test_from_list_with_missing_url():
    input_data = {"http_method": "Get"}
    data_requesturl = [
        {
            "step": "requesturl",
            **input_data
        }
    ]
    expected_errors = [ValueMissing("url")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_missing_http_method():
    input_data = {"url": "http://localhost"}
    data_requesturl = [
        {
            "step": "requesturl",
            **input_data
        }
    ]
    expected_errors = [ValueMissing("http_method")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_missing_url_and_http_method():
    input_data = {}
    data_requesturl = [
        {
            "step": "requesturl",
            **input_data
        }
    ]
    expected_errors = [ValueMissing("http_method"), ValueMissing("url")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    for expected_error in expected_errors:
        assert expected_error in res.error # type: ignore



def test_from_list_with_empty_url():
    input_data = {"url": "", "http_method": "Get"}
    data_requesturl = [{"step": "requesturl", **input_data}]
    expected_errors = [ValueInvalid("url")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_empty_http_method():
    input_data = {"url": "http://localhost", "http_method": ""}
    data_requesturl = [{"step": "requesturl", **input_data}]
    expected_errors = [ValueInvalid("http_method")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_empty_url_and_http_method():
    input_data = {"url": "", "http_method": ""}
    data_requesturl = [{"step": "requesturl", **input_data}]
    expected_errors = [ValueInvalid("http_method"), ValueInvalid("url")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    for expected_error in expected_errors:
        assert expected_error in res.error # type: ignore



def test_from_list_with_invalid_url():
    input_data = {"url": "invalid url", "http_method": "Get"}
    data_requesturl = [{"step": "requesturl", **input_data}]
    expected_errors = [ValueInvalid("url")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_invalid_http_method():
    input_data = {"url": "http://localhost", "http_method": "invalid http method"}
    data_requesturl = [{"step": "requesturl", **input_data}]
    expected_errors = [ValueInvalid("http_method")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_invalid_url_and_http_method():
    input_data = {"url": "invalid url", "http_method": "invalid http method"}
    data_requesturl = [{"step": "requesturl", **input_data}]
    expected_errors = [ValueInvalid("http_method"), ValueInvalid("url")]

    res = DefinitionAdapter.from_list(data_requesturl)

    assert res.is_error()
    for expected_error in expected_errors:
        assert expected_error in res.error # type: ignore



def test_to_list_with_requesturl_step():
    input_data_dict = {"url": "http://localhost", "http_method": "Get"}
    requesturl_step = RequestUrl.create().ok
    definition = Definition.from_steps(input_data_dict, [requesturl_step]).ok
    expected_definition_dto = [
        {
            **input_data_dict,
            "step": "requesturl"
        }
    ]
    
    definition_dto = DefinitionAdapter.to_list(definition)

    assert definition_dto == expected_definition_dto



def test_from_list_getcontentfromhtml_with_missing_config():
    data_getcontetnfromhtml = [{"step": "getcontentfromhtml"}]
    expected_errors = [ValueMissing("css_selector")]

    res = DefinitionAdapter.from_list(data_getcontetnfromhtml)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_getcontentfromhtml_with_empty_css_selector():
    data_getcontetnfromhtml = [{"step": "getcontentfromhtml", "css_selector": ""}]
    expected_errors = [ValueInvalid("css_selector")]

    res = DefinitionAdapter.from_list(data_getcontetnfromhtml)

    assert res.is_error()
    assert res.error == expected_errors



def test_from_list_with_step_accepting_different_types_of_input_data():
    raw_input_data = {"url": "http://localhost", "http_method": "Get"}
    raw_first_step = {"step": "requesturl", **raw_input_data}
    raw_second_step = {"step": "filterhtmlresponse"}
    raw_differentinputtypes_supported_step = {"step": "getcontentfromhtml"}
    raw_third_step = {**raw_differentinputtypes_supported_step, "css_selector": "ul"}
    raw_forth_step = {**raw_differentinputtypes_supported_step, "css_selector": "li"}
    raw_definition = [
        raw_first_step,
        raw_second_step,
        raw_third_step,
        raw_forth_step
    ]

    res = DefinitionAdapter.from_list(raw_definition)

    assert res.is_ok()