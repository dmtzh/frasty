from dataclasses import dataclass
import os

from infrastructure.rabbitmq import config
from shared.completedresult import CompletedResult
from shared.customtypes import DefinitionIdValue, RunIdValue, StepIdValue
from shared.domaindefinition import StepDefinition
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from shared.pipeline.handlers import HandlerAdapter
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData
from stepdefinitions.viber import SendToViberChannel
from stephandlers.getcontentfromjson.definition import GetContentFromJson

step_definitions: list[type[StepDefinition]] = [
    RequestUrl, FilterSuccessResponse,
    FilterHtmlResponse, GetContentFromHtml, GetLinksFromHtml,
    FetchNewData, SendToViberChannel,
    GetContentFromJson
]
for step_definition in step_definitions:
    step_definition_creators_storage.add(step_definition)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

@dataclass(frozen=True)
class RunDefinitionData:
    run_id: RunIdValue
    definition_id: DefinitionIdValue
    metadata: dict

run_definition_handler = HandlerAdapter(config.run_definition_handler(RunDefinitionData))

run_step = config.run_step

@dataclass(frozen=True)
class CompleteStepData:
    run_id: RunIdValue
    step_id: StepIdValue
    result: CompletedResult
    metadata: dict
    
complete_step_handler = HandlerAdapter(config.complete_step_handler(CompleteStepData))

publish_completed_definition = config.publish_completed_definition

app = config.create_faststream_app()