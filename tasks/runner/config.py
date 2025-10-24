import os

from infrastructure.rabbitmq import config

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

run_task_handler = config.run_task_handler

run_definition = config.run_definition

publish_completed_definition = config.publish_completed_definition

app = config.create_faststream_app()