import sys
from shared.runningdefinitionsstore import RunningDefinitionsStore
import config

sys.path.append('definition/runner')
config.running_definitions_storage = RunningDefinitionsStore(config.STORAGE_ROOT_FOLDER)
