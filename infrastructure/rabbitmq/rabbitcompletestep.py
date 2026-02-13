from shared.customtypes import RunIdValue, StepIdValue
from shared.completedresult import CompletedResult, CompletedResultAdapter

from .client import RabbitMQClient
from .pythonpickle import DataWithCorrelationId, PythonPickleMessage

COMPLETE_STEP_COMMAND = "complete_step"

class _python_pickle:
    @staticmethod
    def data_to_message(run_id: RunIdValue, step_id: StepIdValue, result: CompletedResult, metadata: dict) -> PythonPickleMessage:
        result_dto = CompletedResultAdapter.to_dict(result)
        is_metadata_valid = isinstance(metadata, dict)
        if not is_metadata_valid:
            raise ValueError(f"Invalid 'metadata' value {metadata}")
        run_id_dict = {"run_id": run_id.to_value_with_checksum()}
        step_id_dict = {"step_id": step_id.to_value_with_checksum()}
        command_data = run_id_dict | step_id_dict | {"result": result_dto, "metadata": metadata}
        correlation_id = run_id_dict["run_id"]
        data_with_correlation_id = DataWithCorrelationId(command_data, correlation_id)
        return PythonPickleMessage(data_with_correlation_id)

def run(rabbit_client: RabbitMQClient, run_id: RunIdValue, step_id: StepIdValue, result: CompletedResult, metadata: dict):
    message = _python_pickle.data_to_message(run_id, step_id, result, metadata)
    return rabbit_client.send_command(COMPLETE_STEP_COMMAND, message)
