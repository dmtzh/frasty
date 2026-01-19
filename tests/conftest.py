import pytest

from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl

@pytest.fixture(scope="session", autouse=True)
def init_step_definition_creators_storage():
    step_definitions: list[type[StepDefinition]] = [
        RequestUrl, FilterSuccessResponse,
        FilterHtmlResponse, GetContentFromHtml
    ]
    for step_definition in step_definitions:
        step_definition_creators_storage.add(step_definition)
    yield