import os

from shared.infrastructure.rabbitmq.config import RabbitMQConfig

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

_raw_rabbitmq_url = os.environ["RABBITMQ_URL"]
_raw_rabbitmq_publisher_confirms = os.environ["RABBITMQ_PUBLISHER_CONFIRMS"]
_opt_rabbitmqconfig = RabbitMQConfig.parse(_raw_rabbitmq_url, _raw_rabbitmq_publisher_confirms)
if _opt_rabbitmqconfig is None:
    raise ValueError("Invalid RabbitMQ configuration")
rabbitmqconfig = _opt_rabbitmqconfig
