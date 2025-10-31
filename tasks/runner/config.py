from dataclasses import dataclass
import os

from infrastructure.rabbitmq import config
from shared.customtypes import RunIdValue, TaskIdValue
from shared.pipeline.handlers import HandlerAdapter

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

@dataclass(frozen=True)
class RunTaskData:
    task_id: TaskIdValue
    run_id: RunIdValue
    metadata: dict
    
run_task_handler = HandlerAdapter(config.run_task_handler(RunTaskData))

run_definition = config.run_definition

publish_completed_definition = config.publish_completed_definition

app = config.create_faststream_app()