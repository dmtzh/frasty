import sys
from shared.runningdefinitionsstore import GroupOfRunningDefinitionsStore, RunningDefinitionsStore
import config

sys.path.append('definition/runner')
config.running_definitions_storage = RunningDefinitionsStore(config.STORAGE_ROOT_FOLDER)
config.group_of_running_definitions_storage = GroupOfRunningDefinitionsStore(config.STORAGE_ROOT_FOLDER)
