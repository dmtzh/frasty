from contextlib import asynccontextmanager
import os

from faststream import FastStream
from faststream.rabbit import RabbitBroker

from shared.domaindefinition import StepDefinition
from shared.infrastructure.rabbitmq.broker import RabbitMQBroker
from shared.infrastructure.rabbitmq.client import RabbitMQClient
from shared.infrastructure.rabbitmq.config import RabbitMQConfig
from shared.infrastructure.stepdefinitioncreatorsstore import step_definition_creators_storage
from stepdefinitions.html import FilterHtmlResponse, GetContentFromHtml
from stepdefinitions.httpresponse import FilterSuccessResponse
from stepdefinitions.requesturl import RequestUrl
from stepdefinitions.task import FetchNewData

step_definitions: list[type[StepDefinition]] = [
    FetchNewData, RequestUrl, FilterSuccessResponse,
    FilterHtmlResponse, GetContentFromHtml
]
for step_definition in step_definitions:
    step_definition_creators_storage.add(step_definition)

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

_raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
_raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
_rabbitmqconfig = RabbitMQConfig.parse(_raw_rabbitmq_url, _raw_rabbitmq_publisher_confirms)
if _rabbitmqconfig is None:
    raise ValueError("Invalid RabbitMQ configuration")
_log_fmt = '%(asctime)s %(levelname)-8s - %(exchange)-4s | %(queue)-10s | %(message_id)-10s - %(message)s'
_broker = RabbitBroker(url=_rabbitmqconfig.url.value, publisher_confirms=_rabbitmqconfig.publisher_confirms, log_fmt=_log_fmt)
_rabbit_broker = RabbitMQBroker(_broker.subscriber)
rabbit_client = RabbitMQClient(_rabbit_broker)

@asynccontextmanager
async def lifespan():
    raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
    raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
    rabbitmqconfig = RabbitMQConfig.parse(raw_rabbitmq_url, raw_rabbitmq_publisher_confirms)
    if rabbitmqconfig is None:
        raise ValueError("Invalid RabbitMQ configuration")
    await _rabbit_broker.connect(rabbitmqconfig)
    yield
    await _rabbit_broker.disconnect()

app = FastStream(broker=_broker, lifespan=lifespan)