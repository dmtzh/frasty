from shared.customtypes import RunIdValue, TaskIdValue

from .client import RabbitMQClient
from .pythonpickle import DataWithCorrelationId, PythonPickleMessage

RUN_TASK_COMMAND = "run_task"

class _python_pickle:
    @staticmethod
    def data_to_message(task_id: TaskIdValue, run_id: RunIdValue, metadata: dict) -> PythonPickleMessage:
        is_metadata_valid = isinstance(metadata, dict)
        if not is_metadata_valid:
            raise ValueError(f"Invalid 'metadata' value {metadata}")
        task_id_with_checksum = task_id.to_value_with_checksum()
        run_id_with_checksum = run_id.to_value_with_checksum()
        command_data = {"task_id": task_id_with_checksum, "run_id": run_id_with_checksum, "metadata": metadata}
        correlation_id = run_id_with_checksum
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)
    
def run(rabbit_client: RabbitMQClient, task_id: TaskIdValue, run_id: RunIdValue, metadata: dict):
    command = RUN_TASK_COMMAND
    message = _python_pickle.data_to_message(task_id, run_id, metadata)
    return rabbit_client.send_command(command, message)
