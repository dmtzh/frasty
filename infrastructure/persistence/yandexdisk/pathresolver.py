import urllib.parse
from typing import Any

from shared.utils.parse import NonEmptyStr

class YandexDiskPathResolver:
    """
    Converts domain IDs into fully qualified, URL-encoded Yandex Disk paths.
    
    Responsibilities:
       - Concatenate base_folder with the URL-encoded ID.
       - Append the file extension (.json).
       - Ensure special characters in IDs do not break the path structure.
    """
    def __init__(self, base_folder: NonEmptyStr) -> None:
        # Store the base folder for path concatenation
        self._base_folder = base_folder

    def resolve(self, id: Any) -> str:
        """
        Resolve a domain ID to a Yandex Disk resource path.
        
        Args:
            id: The domain identifier (will be cast to string).
            
        Returns:
            A fully qualified path (e.g., "/app_data/my%20doc%2F1.json").
        """
        # URL-encode the base folder and ID to handle spaces, Cyrillic, and special characters safely.
        # safe='' ensures that even slashes in the ID are encoded, treating the ID 
        # strictly as a single file name within the base_folder.
        # encoded_base_folder = urllib.parse.quote(self._base_folder, safe='')
        encoded_id = urllib.parse.quote(str(id), safe='')
        return f"{self._base_folder}{encoded_id}.json"
