import os
import tempfile

from shared.customtypes import IdValue

STORAGE_ROOT_FOLDER = os.path.join(tempfile.gettempdir(), "tests", IdValue.new_id())